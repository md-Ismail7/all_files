#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<unistd.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<fcntl.h>
#include<errno.h>
#include<time.h>
#include<aio.h>
#include<string.h>


static long long timespec_to_ns(const struct timespec *t)
{
     return (long long)t->tv_sec*1000000000LL + t->tv_nsec;
}

int main(int argc, char **argv){
     char *fileName = "posixio.bin";
     size_t write_size = 4096;
     size_t write_mb = 64;

     if(argc>=2){
          fileName = argv[1];
     }

      if(argc>=3)
     {
          write_size = (size_t)strtoull(argv[2], NULL, 10);
     }
     if(argc>=4)
     {
          write_mb = (size_t)strtoull(argv[3], NULL, 10);
     }

     size_t iterations = (write_mb*1024*1024)/write_size;
     if(iterations==0){
          fprintf(stderr, "plz increase the write mb: %zu or decrease the write size: %zu\n", write_mb, write_size);
          return 1;
     }

     int fd = open(fileName, O_CREAT | O_WRONLY | O_TRUNC, 0664);
     if(fd<0)
     {
          perror("file open");
          return 1;
     }

     unsigned char *buffer = malloc(write_size);
     if(!buffer)
     {
          perror("malloc");
          return 1;
     }
     //filling the buffer 
     for(size_t iterator=0; iterator<write_size; iterator++)
     {
          buffer[iterator] = (unsigned char)(iterator & 0xFF);
     }
     
     struct aiocb *cbs = calloc(iterations, sizeof(struct aiocb));
     if(!cbs)
     {
          perror("calloc");
          return 1;
     }

     struct timespec tstart, tend;
     long long min_ns = (1LL<<62), max_ns = 0, sum_ns = 0;

     printf("Posix AIO demo :: total operations: %zu total bytes write: %zu\n", iterations, write_size);

     for(size_t iterator=0; iterator<iterations; iterator++)
     {
          off_t offset = iterator * (off_t)write_size;

          memset(&cbs[iterator], 0, sizeof(struct aiocb));

          cbs[iterator].aio_fildes = fd;
          cbs[iterator].aio_buf = buffer;
          cbs[iterator].aio_nbytes = write_size;
          cbs[iterator].aio_offset = offset;

          if(clock_gettime(CLOCK_MONOTONIC, &tstart) < 0)
          {
               perror("clock get time");
               return 1;
          }

          if(aio_write(&cbs[iterator]) < 0)
          {
               perror("aio write");
               return 1;
          }

          const struct aiocb *list[1] = {&cbs[iterator]};
          int ret = aio_suspend(list, 1, NULL);
          if(ret < 0)
          {
               perror("aio suspend");
               return 1;
          }

          int err = aio_error(&cbs[iterator]);
          if(err!=0)
          {
               fprintf(stderr, "aio_error\n");
               return 1;
          }

          ssize_t return_size = aio_return(&cbs[iterator]);
          if(return_size != (ssize_t)write_size)
          {
               fprintf(stderr, "aio_return size is short than expected: %zd\n", return_size);
               return 1;
          }

          if(clock_gettime(CLOCK_MONOTONIC, &tend) < 0)
          {
               perror("clock get time end");
               return 1;
          }

          long long ns = timespec_to_ns(&tend) - timespec_to_ns(&tstart);
          sum_ns += ns;
          if(ns<min_ns)
          {
               min_ns = ns;
          }
          if(ns>max_ns)
          {
               max_ns = ns;
          }  
     }
     long long avg_ns = (long long)sum_ns/iterations;
     printf("total operations: %zu, avg operation time: %lld ns, fastest: %.3f s, slowest: %.3f s\n",
               iterations, avg_ns, min_ns/1e9, max_ns/1e9);
     return 0;
}
