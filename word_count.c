#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>

#define THREAD_COUNT 4   // you can change this to any number of threads

// Shared global counter
int total_words = 0;
pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;

// Struct to pass thread work info
typedef struct {
    char* buffer;   // the file contents
    long start;     // start index for this thread
    long end;       // end index for this thread
    int tid;        // thread id (for printing)
} thread_arg_t;

// Function: check if char is separator
int is_separator(char c) {
    return (c == ' ' || c == '\n' || c == '\t' || c == '\0' || c == '\r' || c == '.' || c == ',');
}

// Thread function: count words in assigned chunk
void* count_words(void* arg) {
    thread_arg_t* data = (thread_arg_t*)arg;
    char* buf = data->buffer;
    long start = data->start;
    long end = data->end;

    int local_count = 0;
    int in_word = 0;

    for (long i = start; i < end; i++) {
        if (is_separator(buf[i])) {
            if (in_word) {
                local_count++;
                in_word = 0;
            }
        } else {
            in_word = 1;
        }
    }

    // Handle last word if it ends at boundary
    if (in_word) {
        local_count++;
    }

    // Print per-thread result
    printf("[Thread %d] counted %d words in range [%ld - %ld)\n",
           data->tid, local_count, start, end);

    // Update global total (protected by mutex)
    pthread_mutex_lock(&count_mutex);
    total_words += local_count;
    pthread_mutex_unlock(&count_mutex);

    free(data);
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s <filename>\n", argv[0]);
        return 1;
    }

    // Open file
    FILE* f = fopen(argv[1], "r");
    if (!f) {
        perror("fopen");
        return 1;
    }

    // Find file size
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    // Read file into memory
    char* buffer = malloc(fsize + 1);
    fread(buffer, 1, fsize, f);
    buffer[fsize] = '\0';
    fclose(f);

    // Create threads
    pthread_t threads[THREAD_COUNT];
    long chunk = fsize / THREAD_COUNT;

    for (int i = 0; i < THREAD_COUNT; i++) {
        thread_arg_t* arg = malloc(sizeof(thread_arg_t));
        arg->buffer = buffer;
        arg->start = i * chunk;
        arg->end = (i == THREAD_COUNT - 1) ? fsize : (i + 1) * chunk;
        arg->tid = i + 1;

        pthread_create(&threads[i], NULL, count_words, arg);
    }

    // Join threads
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }

    // Print result
    printf("\nTotal words in file = %d\n", total_words);

    // Cleanup
    free(buffer);
    pthread_mutex_destroy(&count_mutex);

    return 0;
}

