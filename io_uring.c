#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<stdbool.h>
#include<unistd.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<fcntl.h>
#include<liburing.h>
#include<time.h>

static long long timespec_to_ns(const struct timespec *t){
     return (long long)t->tv_sec*1000000000LL + t->tv_nsec;
}

int main(int argc, char **argv){
     char *fileName = "io_uring.bin";

     size_t write_size = 4096; //block size 4KB
     size_t total_mb = 64; //total size 64MB

     if(argc>=2){
          fileName = argv[1];
     }
     if(argc>=3)
     {
          write_size = (size_t)atoi(argv[2]);
     }
      if(argc>=4)
     {
          total_mb = (size_t)atoi(argv[3]);
     }

     size_t iterations = (total_mb*1024*1024)/write_size;
     if (iterations == 0) {
        fprintf(stderr, "Bad args: too few iterations\n");
        return 1;
     }

     int fd = open(fileName, O_CREAT | O_WRONLY | O_TRUNC, 0644);
     if(fd<0)
     {
          perror("open");
          return 1;
     }

     unsigned char *buffer = malloc(write_size);
     if(!buffer)
     {
          perror("malloc");
          return 1;
     }

     for(int i=0; i<write_size; i++)
     {
          buffer[i] = (unsigned char)(i & 0xFF);
     }

     struct io_uring ring;
     //initalise the ring queue
     if(io_uring_queue_init(32, &ring, 0) < 0)
     {
          perror("io ring queue init");
          return 1;
     }

     struct timespec tstart, tend;
     long long min_ns = (1LL<<62), max_ns = 0, sum_ns = 0;

     printf("io_uring demo:: total ops: %zu, bytes/write: %zu\n", iterations, write_size);
     
     for(size_t i=0; i<iterations; i++)
     {
          struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
          if(!sqe)
          {
               fprintf(stderr, "uring get sqe failed");
               return 1;
          }

          off_t offset = i*(off_t)(write_size);

          clock_gettime(CLOCK_MONOTONIC, &tstart);

          io_uring_prep_write(sqe, fd, buffer, write_size, offset);
          io_uring_submit(&ring);

          struct io_uring_cqe *cqe;
          int ret = io_uring_wait_cqe(&ring, &cqe);
          if(ret<0)
          {
               fprintf(stderr, "wait cqe error\n");
               return 1;
          }
          if(cqe->res != (int)write_size)
          {
               fprintf(stderr, "short write: %d\n", cqe->res);
               return 1;
          }

          clock_gettime(CLOCK_MONOTONIC, &tend);

          long long ns = timespec_to_ns(&tend) - timespec_to_ns(&tstart);
          sum_ns += ns;
          if (ns < min_ns) min_ns = ns;
          if (ns > max_ns) max_ns = ns;

          io_uring_cqe_seen(&ring, cqe);
     }
     long long avg_ns = sum_ns / iterations;
     printf("total ops: %zu, avg: %lld ns, fastest: %.6f s, slowest: %.3f s\n",
           iterations, avg_ns, min_ns/1e9, max_ns/1e9);
     
     io_uring_queue_exit(&ring);
     close(fd);
     free(buffer);
     return 0;
}

