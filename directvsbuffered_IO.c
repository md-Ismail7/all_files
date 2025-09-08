#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <fcntl.h>

#ifndef __linux__
#define O_DIRECT 0
#endif

static ssize_t read_all(int fd, void *buffer, size_t count) {
    uint8_t *pointer = buffer;
    size_t left = count;

    while (left > 0) {
        ssize_t bytes_read = read(fd, pointer, left);

        if (bytes_read < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (bytes_read == 0) {
            break; // EOF
        }

        left -= (size_t)bytes_read;
        pointer += bytes_read;
    }

    return (ssize_t)(count - left);
}

static ssize_t write_all(int fd, const void *buffer, size_t count, int do_fsync) {
    const uint8_t *pointer = buffer;
    size_t left = count;

    while (left > 0) {
        ssize_t bytes_written = write(fd, pointer, left);

        if (bytes_written < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        left -= (size_t)bytes_written;
        pointer += bytes_written;
    }

    if (do_fsync) {
        if (fsync(fd) < 0) {
            perror("fsync");
            exit(1);
        }
    }
    return (ssize_t)count;
}

static void fill_pattern(unsigned char *buffer, size_t length) {
    for (size_t i = 0; i < length; ++i) {
        buffer[i] = (unsigned char)(i & 0xFF);
    }
}

//calculate time difference in ns
static long long elapsed_ns(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) * 1000000000LL + (end.tv_nsec - start.tv_nsec);
}

int main(int argc, char **argv) {
    const char *filename = "BufferedVsDirect.txt";
    if (argc >= 2) {
        filename = argv[1];
    }

    size_t write_size = 4096 * 1024; 

    struct timespec t1, t2;

    // Buffered write 
    int fd_buffer = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd_buffer < 0) {
        perror("buffered open");
        return 1;
    }

    unsigned char *buffer = malloc(write_size);
    if (!buffer) {
        perror("malloc");
        return 1;
    }
    fill_pattern(buffer, write_size);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ssize_t buffered_write = write_all(fd_buffer, buffer, write_size, 1);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    if (buffered_write != (ssize_t)write_size) {
        perror("write buffered");
        close(fd_buffer);
        return 1;
    }
    close(fd_buffer);

    printf("Buffered write time: %lld ns\n", elapsed_ns(t1, t2));

    // Direct write 
    int fd_direct = open(filename, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT, 0644);
    if (fd_direct < 0) {
        perror("open O_DIRECT");
        fprintf(stderr, "O_DIRECT may have failed due to alignment issues.\n");
        return 1;
    }

    void *dbuffer = NULL;
    if (posix_memalign(&dbuffer, 4096, write_size) != 0) {
        perror("posix_memalign");
        return 1;
    }
    fill_pattern((unsigned char *)dbuffer, write_size);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ssize_t direct_write = write_all(fd_direct, dbuffer, write_size, 1);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    if (direct_write != (ssize_t)write_size) {
        perror("write direct");
        close(fd_direct);
        return 1;
    }
    close(fd_direct);

    printf("Direct write time:   %lld ns\n", elapsed_ns(t1, t2));

    //  Buffered read 
    int fd_rbuf = open(filename, O_RDONLY);
    if (fd_rbuf < 0) {
        perror("open for read");
        return 1;
    }

    unsigned char *read_buffer = malloc(write_size);
    if (!read_buffer) {
        perror("malloc");
        return 1;
    }

#ifdef __linux__
    posix_fadvise(fd_rbuf, 0, 0, POSIX_FADV_DONTNEED);
#endif

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ssize_t buffered_read = read_all(fd_rbuf, read_buffer, write_size);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    if (buffered_read != (ssize_t)write_size) {
        perror("read buffered");
        close(fd_rbuf);
        return 1;
    }
    close(fd_rbuf);

    printf("Buffered read time:  %lld ns\n", elapsed_ns(t1, t2));

    // Direct read 
    int fd_rdirect = open(filename, O_RDONLY | O_DIRECT);
    if (fd_rdirect < 0) {
        perror("open O_DIRECT read");
        return 1;
    }

    void *rd_buffer = NULL;
    if (posix_memalign(&rd_buffer, 4096, write_size) != 0) {
        perror("posix_memalign rdbuffer");
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ssize_t direct_read = read_all(fd_rdirect, rd_buffer, write_size);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    if (direct_read != (ssize_t)write_size) {
        perror("read direct");
        close(fd_rdirect);
        return 1;
    }
    close(fd_rdirect);

    printf("Direct read time:    %lld ns\n", elapsed_ns(t1, t2));

    // cleanup
    free(buffer);
    free(dbuffer);
    free(read_buffer);
    free(rd_buffer);

    printf("Done\n");
    return 0;
}