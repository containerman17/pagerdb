#ifndef PAGERDB_VIEWMAP_NATIVE_H
#define PAGERDB_VIEWMAP_NATIVE_H
#include <stdint.h>
#include <stddef.h>
struct pagerdb_fault;
int pagerdb_fault_open(void *address, uint64_t length, struct pagerdb_fault **out);
int pagerdb_fault_start(struct pagerdb_fault *ctx, int backing_fd, uint64_t offset,
                       uint64_t size, const uint64_t *segments, size_t nsegments);
void pagerdb_fault_close(struct pagerdb_fault *ctx);
#endif
