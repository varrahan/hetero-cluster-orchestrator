#ifndef AIORCH_RING_ABI_H
#define AIORCH_RING_ABI_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef struct aiorch_ring aiorch_ring;

int aiorch_ring_open(const char *path, int producer, aiorch_ring **out);
ssize_t aiorch_ring_read(aiorch_ring *ring, void *buffer, size_t length, int timeout_ms);
ssize_t aiorch_ring_write(aiorch_ring *ring, const void *buffer, size_t length, int timeout_ms);
uint64_t aiorch_ring_total(const aiorch_ring *ring);
int aiorch_ring_close(aiorch_ring *ring);

#endif
