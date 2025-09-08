#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <ctype.h>

static int g_tid = 1;

void fatal(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

/* write all bytes (handles short writes) */
ssize_t write_all(int fd, const void *buf, size_t count)
{
    const char *p = buf;
    size_t left = count;
    while (left > 0)
    {
        ssize_t w = write(fd, p, left);
        if (w < 0)
        {
            if (errno == EINTR) continue;
            return -1;
        }
        left -= (size_t)w;
        p += w;
    }
    return (ssize_t)count;
}

/* append and fsync */
void append_and_sync(int fd, const char *text)
{
    size_t len = strlen(text);
    if (write_all(fd, text, len) != (ssize_t)len)
        fatal("write failed");
    if (fsync(fd) != 0)
        fatal("fsync failed");
}

/* append only (no fsync) */
void append_only(int fd, const char *text)
{
    size_t len = strlen(text);
    if (write_all(fd, text, len) != (ssize_t)len)
        fatal("write failed");
}

/* write with sync: wal entries are synced and DB write is synced too */
void write_with_sync(int wal_fd, int db_fd, int key, int value)
{
    char buf[256];
    int tid = g_tid++;

    snprintf(buf, sizeof(buf), "TRANSACTION %d BEGIN\n", tid);
    append_and_sync(wal_fd, buf);

    snprintf(buf, sizeof(buf), "SET %d %d\n", key, value);
    append_and_sync(wal_fd, buf);

    snprintf(buf, sizeof(buf), "TRANSACTION %d COMMIT\n", tid);
    append_and_sync(wal_fd, buf);

    /* Apply to DB and sync */
    snprintf(buf, sizeof(buf), "key=%d value=%d\n", key, value);
    append_and_sync(db_fd, buf);

    printf("Write complete: key=%d value=%d (synced)\n", key, value);
}

/* write to WAL without fsync (fast but risky) */
void write_with_nosync(int wal_fd, int key, int value)
{
    char buf[256];
    int tid = g_tid++;

    snprintf(buf, sizeof(buf), "TRANSACTION %d BEGIN\n", tid);
    append_only(wal_fd, buf);

    snprintf(buf, sizeof(buf), "SET %d %d\n", key, value);
    append_only(wal_fd, buf);

    snprintf(buf, sizeof(buf), "TRANSACTION %d COMMIT\n", tid);
    append_only(wal_fd, buf);

    printf("WAL write (no sync) complete: key=%d value=%d\n", key, value);
}

/* simulate crash after WAL has been synced (before DB apply) */
void crash_after_wal(int wal_fd, int key, int value)
{
    char buf[256];
    int tid = g_tid++;

    snprintf(buf, sizeof(buf), "TRANSACTION %d BEGIN\n", tid);
    append_and_sync(wal_fd, buf);

    snprintf(buf, sizeof(buf), "SET %d %d\n", key, value);
    append_and_sync(wal_fd, buf);

    snprintf(buf, sizeof(buf), "TRANSACTION %d COMMIT\n", tid);
    append_and_sync(wal_fd, buf);

    printf("Simulated crash after WAL (before DB apply). Exiting now.\n");
    _exit(1); /* use _exit to simulate abrupt termination */
}

/* simple dynamic array for SET records during a transaction */
typedef struct {
    int key;
    int value;
} kv_pair;

typedef struct {
    kv_pair *items;
    size_t len;
    size_t cap;
} kv_vec;

void kv_init(kv_vec *v) { v->items = NULL; v->len = 0; v->cap = 0; }
void kv_push(kv_vec *v, int k, int val)
{
    if (v->len == v->cap) {
        size_t newcap = v->cap ? v->cap * 2 : 4;
        kv_pair *p = realloc(v->items, newcap * sizeof(kv_pair));
        if (!p) fatal("realloc");
        v->items = p;
        v->cap = newcap;
    }
    v->items[v->len].key = k;
    v->items[v->len].value = val;
    v->len++;
}
void kv_free(kv_vec *v) { free(v->items); v->items = NULL; v->len = v->cap = 0; }

/* Perform recovery: apply all SETs from committed transactions */
void recover(int db_fd)
{
    int wal_fd = open("wal_log.txt", O_RDONLY);
    if (wal_fd < 0) {
        if (errno == ENOENT) {
            printf("No wal_log.txt found, nothing to recover.\n");
            return;
        }
        fatal("open wal_log.txt for read failed");
    }

    /* rewind */
    if (lseek(wal_fd, 0, SEEK_SET) < 0) fatal("lseek wal_fd");

    char buf[512];
    ssize_t n;
    char line[512];
    size_t line_pos = 0;

    int in_txn = 0;
    kv_vec kvs;
    kv_init(&kvs);

    while ((n = read(wal_fd, buf, sizeof(buf))) > 0)
    {
        for (ssize_t i = 0; i < n; i++)
        {
            char c = buf[i];
            if (c == '\n')
            {
                line[line_pos] = '\0';

                /* Trim leading spaces */
                char *s = line;
                while (*s && isspace((unsigned char)*s)) s++;

                if (strncmp(s, "TRANSACTION", 11) == 0 && strstr(s, "BEGIN")) {
                    in_txn = 1;
                    kv_free(&kvs);
                    kv_init(&kvs);
                }
                else if (strncmp(s, "SET", 3) == 0 && in_txn) {
                    /* parse "SET <key> <value>" robustly using sscanf */
                    int key = 0, value = 0;
                    if (sscanf(s + 3, "%d %d", &key, &value) == 2) {
                        kv_push(&kvs, key, value);
                    } else {
                        fprintf(stderr, "Warning: malformed SET line in WAL: '%s'\n", s);
                    }
                }
                else if (strncmp(s, "TRANSACTION", 11) == 0 && strstr(s, "COMMIT") && in_txn) {
                    /* apply all SETs seen in this transaction */
                    for (size_t j = 0; j < kvs.len; j++) {
                        char out[256];
                        snprintf(out, sizeof(out), "key=%d value=%d\n", kvs.items[j].key, kvs.items[j].value);
                        append_and_sync(db_fd, out);
                        printf("Recovered: key=%d value=%d\n", kvs.items[j].key, kvs.items[j].value);
                    }
                    in_txn = 0;
                    kv_free(&kvs);
                    kv_init(&kvs);
                }

                /* reset line buffer */
                line_pos = 0;
            }
            else
            {
                if (line_pos < sizeof(line) - 1)
                {
                    line[line_pos++] = c;
                }
            }
        }
    }
    if (n < 0) fatal("read wal_fd");
    close(wal_fd);
    kv_free(&kvs);
    printf("Recovery complete.\n");
}

/* display wal and db */
void display_wal_db()
{
    printf("=== WAL LOG ===\n");
    fflush(stdout);
    system("cat wal_log.txt 2>/dev/null || true");
    printf("\n=== DB FILE ===\n");
    fflush(stdout);
    system("cat db.txt 2>/dev/null || true");
    printf("\n");
}

/* delete wal & db (for testing) */
void reset_files()
{
    unlink("wal_log.txt");
    unlink("db.txt");
    printf("wal_log.txt and db.txt removed (if they existed).\n");
}

/* validate integer token */
int validate_integer(const char *token, const char *what)
{
    if (token == NULL || *token == '\0') {
        fprintf(stderr, "Invalid %s: empty\n", what);
        exit(EXIT_FAILURE);
    }

    const char *p = token;
    if (*p == '+' || *p == '-') p++;
    if (!isdigit((unsigned char)*p)) {
        fprintf(stderr, "Invalid %s: not a number (%s)\n", what, token);
        exit(EXIT_FAILURE);
    }
    for (; *p; p++) {
        if (!isdigit((unsigned char)*p)) {
            fprintf(stderr, "Invalid %s: contains non-digit (%s)\n", what, token);
            exit(EXIT_FAILURE);
        }
    }

    errno = 0;
    long val = strtol(token, NULL, 10);
    if (errno == ERANGE || val < INT_MIN || val > INT_MAX) {
        fprintf(stderr, "Invalid %s: out of int range (%s)\n", what, token);
        exit(EXIT_FAILURE);
    }
    return (int)val;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        fprintf(stderr,
            "Usage:\n"
            "  %s write <key> <value>\n"
            "  %s write-nosync <key> <value>\n"
            "  %s crash-after-wal <key> <value>\n"
            "  %s recover\n"
            "  %s display\n"
            "  %s reset\n",
            argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "reset") == 0) {
        reset_files();
        return 0;
    }

    /* open WAL and DB (create if missing). Use O_APPEND to append. */
    int wal_fd = open("wal_log.txt", O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (wal_fd < 0) fatal("wal fd not opened");

    int db_fd = open("db.txt", O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (db_fd < 0) fatal("db fd not opened");

    if (strcmp(argv[1], "write") == 0)
    {
        if (argc < 4) fatal("Need key and value");
        int key = validate_integer(argv[2], "key");
        int value = validate_integer(argv[3], "value");
        write_with_sync(wal_fd, db_fd, key, value);
    }
    else if (strcmp(argv[1], "write-nosync") == 0)
    {
        if (argc < 4) fatal("Need key and value");
        int key = validate_integer(argv[2], "key");
        int value = validate_integer(argv[3], "value");
        write_with_nosync(wal_fd, key, value);
    }
    else if (strcmp(argv[1], "crash-after-wal") == 0)
    {
        if (argc < 4) fatal("Need key and value");
        int key = validate_integer(argv[2], "key");
        int value = validate_integer(argv[3], "value");
        crash_after_wal(wal_fd, key, value);
    }
    else if (strcmp(argv[1], "recover") == 0)
    {
        /* For recovery we need a writable (append+sync) db fd */
        recover(db_fd);
    }
    else if (strcmp(argv[1], "display") == 0)
    {
        display_wal_db();
    }
    else
    {
        printf("Unknown command: %s\n", argv[1]);
    }

    close(wal_fd);
    close(db_fd);
    return 0;
}

