#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#define MAX_FILES 3
#define BUF_SIZE 8192 // read chunk size

enum op_type
{
    OP_READ,
    OP_WRITE
};

struct file_ctx
{
    int fd;
    char *buf;
    const char *name;
    off_t offset;
    int done;
};

struct io_data
{
    enum op_type type;
    struct file_ctx *ctx;
    int len;
    off_t offset;
};

int main()
{
    struct io_uring ring;
    io_uring_queue_init(64, &ring, 0);

    const char *input_files[MAX_FILES] = {"file1.txt", "file2.txt", "file3.txt"};
    struct file_ctx ctxs[MAX_FILES];
    int output_fd = open("merged_output.txt", O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (output_fd < 0)
    {
        perror("open output");
        return 1;
    }

    // Submit first reads
    for (int i = 0; i < MAX_FILES; i++)
    {
        ctxs[i].fd = open(input_files[i], O_RDONLY);
        if (ctxs[i].fd < 0)
        {
            perror("open input");
            return 1;
        }
        ctxs[i].buf = malloc(BUF_SIZE);
        ctxs[i].name = input_files[i];
        ctxs[i].offset = 0;
        ctxs[i].done = 0;

        struct io_data *data = malloc(sizeof(*data));
        data->type = OP_READ;
        data->ctx = &ctxs[i];
        data->offset = 0;
        data->len = BUF_SIZE;

        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, ctxs[i].fd, ctxs[i].buf, BUF_SIZE, 0);
        io_uring_sqe_set_data(sqe, data);
    }
    io_uring_submit(&ring);

    int remaining_reads = MAX_FILES;
    int pending_ops = MAX_FILES; // initial reads

    while (pending_ops > 0)
    {
        struct io_uring_cqe *cqe;
        io_uring_wait_cqe(&ring, &cqe);

        struct io_data *data = (struct io_data *)io_uring_cqe_get_data(cqe);
        struct file_ctx *ctx = data ? data->ctx : NULL;

        if (cqe->res < 0)
        {
            fprintf(stderr, "I/O error: %s\n", strerror(-cqe->res));
            if (data && data->type == OP_READ && !ctx->done)
            {
                ctx->done = 1;
                remaining_reads--;
            }
        }
        else if (data && data->type == OP_READ)
        {
            if (cqe->res == 0)
            {
                // EOF
                ctx->done = 1;
                remaining_reads--;
            }
            else
            {
                int bytes = cqe->res;

                // Submit async write
                struct io_data *wdata = malloc(sizeof(*wdata));
                wdata->type = OP_WRITE;
                wdata->ctx = ctx;
                wdata->len = bytes;

                struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
                io_uring_prep_write(sqe, output_fd, ctx->buf, bytes, -1);
                io_uring_sqe_set_data(sqe, wdata);
                io_uring_submit(&ring);
                pending_ops++;

                // Next read
                ctx->offset += bytes;
                struct io_data *rdata = malloc(sizeof(*rdata));
                rdata->type = OP_READ;
                rdata->ctx = ctx;
                rdata->offset = ctx->offset;
                rdata->len = BUF_SIZE;

                struct io_uring_sqe *sqe2 = io_uring_get_sqe(&ring);
                io_uring_prep_read(sqe2, ctx->fd, ctx->buf, BUF_SIZE, ctx->offset);
                io_uring_sqe_set_data(sqe2, rdata);
                io_uring_submit(&ring);
                pending_ops++;
            }
        }
        // For writes, nothing special (just wait until all finish)

        if (data)
            free(data);
        io_uring_cqe_seen(&ring, cqe);
        pending_ops--;
    }

    // Cleanup
    for (int i = 0; i < MAX_FILES; i++)
    {
        close(ctxs[i].fd);
        free(ctxs[i].buf);
    }
    close(output_fd);
    io_uring_queue_exit(&ring);

    printf("Merged into merged_output.txt (completion order).\n");
    return 0;
}

