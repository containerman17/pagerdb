//go:build linux && cgo

#define _GNU_SOURCE
#include "viewmap_native_linux.h"
#include <errno.h>
#include <fcntl.h>
#include <linux/userfaultfd.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>

struct pagerdb_fault {
    int uffd, wake, backing, started;
    pthread_t thread;
    uint64_t address, length, size, offset;
    uint64_t *segments;
    size_t nsegments, page_size;
    unsigned char *page;
};

// No code on this thread calls Go. A Go faulting thread can block a GC stop,
// so resolving its fault must not depend on the Go scheduler or allocator.
static void fault_fatal(const char *operation, int error) {
    fprintf(stderr, "pagerdb: native userfaultfd %s: %s\n", operation, strerror(error));
    abort();
}

static void read_snapshot(struct pagerdb_fault *ctx, uint64_t logical, size_t count) {
    size_t done = 0;
    while (done < count) {
        uint64_t position = logical + done;
        uint64_t file_offset;
        size_t take = count - done;
        if (ctx->nsegments) {
            const uint64_t segment_size = 2ULL << 20;
            uint64_t segment = position / segment_size;
            uint64_t within = position % segment_size;
            if (segment >= ctx->nsegments) fault_fatal("segment bounds", EINVAL);
            file_offset = ctx->segments[segment] * segment_size + within;
            if (take > segment_size - within) take = segment_size - within;
        } else {
            file_offset = ctx->offset + position;
        }
        ssize_t n = pread(ctx->backing, ctx->page + done, take, (off_t)file_offset);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) fault_fatal("snapshot read", n == 0 ? EIO : errno);
        done += (size_t)n;
    }
}

static void *fault_thread(void *argument) {
    struct pagerdb_fault *ctx = argument;
    struct pollfd fds[2] = {
        {.fd = ctx->uffd, .events = POLLIN},
        {.fd = ctx->wake, .events = POLLIN}
    };
    for (;;) {
        int n = poll(fds, 2, -1);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) fault_fatal("poll", errno);
        if (fds[1].revents & POLLIN) return NULL;
        if (fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) fault_fatal("poll descriptor", EIO);
        if (!(fds[0].revents & POLLIN)) continue;
        struct uffd_msg msg;
        ssize_t got = read(ctx->uffd, &msg, sizeof(msg));
        if (got < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (got != sizeof(msg) || msg.event != UFFD_EVENT_PAGEFAULT)
            fault_fatal("event read", got < 0 ? errno : EIO);
        uint64_t fault = msg.arg.pagefault.address / ctx->page_size * ctx->page_size;
        if (fault < ctx->address || fault - ctx->address >= ctx->size)
            fault_fatal("address bounds", EINVAL);
        uint64_t logical = fault - ctx->address;
        size_t count = ctx->page_size;
        if (count > ctx->size - logical) count = ctx->size - logical;
        memset(ctx->page, 0, ctx->page_size);
        read_snapshot(ctx, logical, count);
        struct uffdio_copy copy = {
            .dst = fault, .src = (uint64_t)(uintptr_t)ctx->page,
            .len = ctx->page_size, .mode = 0, .copy = 0
        };
        int copied;
        do { copied = ioctl(ctx->uffd, UFFDIO_COPY, &copy); } while (copied < 0 && errno == EINTR);
        if (copied < 0) {
            if (errno == EEXIST) continue;
            fault_fatal("copy", errno);
        }
        if (copy.copy != (int64_t)ctx->page_size) fault_fatal("short copy", EIO);
    }
}

void pagerdb_fault_close(struct pagerdb_fault *ctx) {
    if (!ctx) return;
    if (ctx->started) {
        uint64_t one = 1;
        ssize_t n;
        do { n = write(ctx->wake, &one, sizeof(one)); } while (n < 0 && errno == EINTR);
        if (n != sizeof(one)) fault_fatal("shutdown wake", n < 0 ? errno : EIO);
        int error = pthread_join(ctx->thread, NULL);
        if (error) fault_fatal("thread join", error);
    }
    if (ctx->uffd >= 0) close(ctx->uffd);
    if (ctx->wake >= 0) close(ctx->wake);
    if (ctx->backing >= 0) close(ctx->backing);
    free(ctx->segments);
    free(ctx->page);
    free(ctx);
}

int pagerdb_fault_open(void *address, uint64_t length, struct pagerdb_fault **out) {
    *out = NULL;
    struct pagerdb_fault *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return ENOMEM;
    ctx->uffd = ctx->wake = ctx->backing = -1;
    ctx->address = (uint64_t)(uintptr_t)address;
    ctx->length = length;
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) { pagerdb_fault_close(ctx); return EINVAL; }
    ctx->page_size = (size_t)page_size;
    ctx->uffd = syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK);
    if (ctx->uffd < 0) { int error = errno; pagerdb_fault_close(ctx); return error; }
    struct uffdio_api api = {.api = UFFD_API, .features = 0, .ioctls = 0};
    if (ioctl(ctx->uffd, UFFDIO_API, &api) < 0) {
        int error = errno; pagerdb_fault_close(ctx); return error;
    }
    struct uffdio_register registration = {
        .range = {.start = ctx->address, .len = length},
        .mode = UFFDIO_REGISTER_MODE_MISSING, .ioctls = 0
    };
    if (ioctl(ctx->uffd, UFFDIO_REGISTER, &registration) < 0) {
        int error = errno; pagerdb_fault_close(ctx); return error;
    }
    if (!(registration.ioctls & (1ULL << _UFFDIO_COPY))) {
        pagerdb_fault_close(ctx); return ENOTSUP;
    }
    ctx->wake = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (ctx->wake < 0) { int error = errno; pagerdb_fault_close(ctx); return error; }
    *out = ctx;
    return 0;
}

int pagerdb_fault_start(struct pagerdb_fault *ctx, int backing_fd, uint64_t offset,
                       uint64_t size, const uint64_t *segments, size_t nsegments) {
    if (!size || size > ctx->length || nsegments > SIZE_MAX / sizeof(uint64_t)) return EINVAL;
    ctx->size = size;
    ctx->offset = offset;
    ctx->backing = fcntl(backing_fd, F_DUPFD_CLOEXEC, 0);
    if (ctx->backing < 0) return errno;
    if (nsegments) {
        ctx->segments = malloc(nsegments * sizeof(uint64_t));
        if (!ctx->segments) return ENOMEM;
        memcpy(ctx->segments, segments, nsegments * sizeof(uint64_t));
        ctx->nsegments = nsegments;
    }
    int error = posix_memalign((void **)&ctx->page, ctx->page_size, ctx->page_size);
    if (error) return error;
    error = pthread_create(&ctx->thread, NULL, fault_thread, ctx);
    if (error) return error;
    ctx->started = 1;
    return 0;
}
