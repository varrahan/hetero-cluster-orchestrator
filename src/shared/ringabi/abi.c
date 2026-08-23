#define _POSIX_C_SOURCE 200809L

#include "abi.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

enum { HEADER_SIZE = 4096, HEAD_OFFSET = 0, TAIL_OFFSET = 64, MAGIC_OFFSET = 128,
       VERSION_OFFSET = 136, SLOTS_OFFSET = 140, SLOT_SIZE_OFFSET = 144,
       TOTAL_OFFSET = 152, MIN_SLOT_SIZE = 1 << 20, MAX_SLOT_SIZE = 16 << 20 };

struct aiorch_ring {
    int fd;
    unsigned char *data;
    size_t mapping_length;
    _Atomic uint64_t *head;
    _Atomic uint64_t *tail;
    uint64_t slots;
    uint64_t slot_size;
    uint64_t total;
    uint64_t position;
    uint64_t frame_bytes;
    uint64_t frame_position;
    int producer;
};

static const unsigned char magic[8] = {'A', 'I', 'O', 'R', 'I', 'N', 'G', '1'};

static uint32_t load_u32(const unsigned char *value) {
    return (uint32_t)value[0] | (uint32_t)value[1] << 8 | (uint32_t)value[2] << 16 | (uint32_t)value[3] << 24;
}

static uint64_t load_u64(const unsigned char *value) {
    uint64_t result = 0;
    for (unsigned int i = 0; i < 8; ++i) result |= (uint64_t)value[i] << (8 * i);
    return result;
}

static int pause_until(const struct timespec *deadline) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return -errno;
    if (now.tv_sec > deadline->tv_sec || (now.tv_sec == deadline->tv_sec && now.tv_nsec >= deadline->tv_nsec)) return -ETIMEDOUT;
    struct timespec delay = {.tv_sec = 0, .tv_nsec = 1000000};
    nanosleep(&delay, NULL);
    return 0;
}

static struct timespec deadline_after(int timeout_ms) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    value.tv_sec += timeout_ms / 1000;
    value.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (value.tv_nsec >= 1000000000L) { value.tv_sec++; value.tv_nsec -= 1000000000L; }
    return value;
}

int aiorch_ring_open(const char *path, int producer, aiorch_ring **out) {
    if (path == NULL || out == NULL) return -EINVAL;
    int fd = open(path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) return -errno;
    struct stat stat_buffer;
    if (fstat(fd, &stat_buffer) != 0 || !S_ISREG(stat_buffer.st_mode) || stat_buffer.st_size < HEADER_SIZE) {
        int error = errno == 0 ? EINVAL : errno;
        close(fd);
        return -error;
    }
    unsigned char *data = mmap(NULL, (size_t)stat_buffer.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) { int error = errno; close(fd); return -error; }
    uint64_t slots = load_u32(data + SLOTS_OFFSET);
    uint64_t slot_size = load_u64(data + SLOT_SIZE_OFFSET);
    uint64_t total = load_u64(data + TOTAL_OFFSET);
    if (memcmp(data + MAGIC_OFFSET, magic, sizeof(magic)) != 0 || load_u32(data + VERSION_OFFSET) != 1 ||
        slots < 2 || slots > 64 || slot_size < MIN_SLOT_SIZE || slot_size > MAX_SLOT_SIZE || total == 0 || total > (1ULL << 40) ||
        (uint64_t)stat_buffer.st_size != HEADER_SIZE + slots * slot_size) {
        munmap(data, (size_t)stat_buffer.st_size); close(fd); return -EINVAL;
    }
    _Atomic uint64_t *head = (_Atomic uint64_t *)(void *)(data + HEAD_OFFSET);
    _Atomic uint64_t *tail = (_Atomic uint64_t *)(void *)(data + TAIL_OFFSET);
    if (!atomic_is_lock_free(head) || !atomic_is_lock_free(tail)) {
        munmap(data, (size_t)stat_buffer.st_size); close(fd); return -ENOTSUP;
    }
    aiorch_ring *ring = calloc(1, sizeof(*ring));
    if (ring == NULL) { munmap(data, (size_t)stat_buffer.st_size); close(fd); return -ENOMEM; }
    *ring = (aiorch_ring){.fd = fd, .data = data, .mapping_length = (size_t)stat_buffer.st_size,
        .head = head, .tail = tail, .slots = slots, .slot_size = slot_size, .total = total, .producer = producer != 0};
    *out = ring;
    return 0;
}

ssize_t aiorch_ring_write(aiorch_ring *ring, const void *source, size_t length, int timeout_ms) {
    if (ring == NULL || !ring->producer || source == NULL || timeout_ms < 1 || ring->position + length > ring->total) return -EINVAL;
    struct timespec deadline = deadline_after(timeout_ms);
    size_t written = 0;
    while (written < length) {
        if (ring->frame_position == ring->frame_bytes) {
            uint64_t head = atomic_load_explicit(ring->head, memory_order_relaxed);
            uint64_t tail = atomic_load_explicit(ring->tail, memory_order_acquire);
            if (head < tail || head - tail > ring->slots) return -EIO;
            if (head - tail == ring->slots) { int result = pause_until(&deadline); if (result != 0) return result; continue; }
            ring->frame_bytes = ring->slot_size < ring->total - ring->position ? ring->slot_size : ring->total - ring->position;
            ring->frame_position = 0;
        }
        uint64_t remaining = ring->frame_bytes - ring->frame_position;
        size_t amount = remaining < length - written ? (size_t)remaining : length - written;
        uint64_t head = atomic_load_explicit(ring->head, memory_order_relaxed);
        unsigned char *target = ring->data + HEADER_SIZE + (head % ring->slots) * ring->slot_size + ring->frame_position;
        memcpy(target, (const unsigned char *)source + written, amount);
        ring->frame_position += amount; ring->position += amount; written += amount;
        if (ring->frame_position == ring->frame_bytes) atomic_store_explicit(ring->head, head + 1, memory_order_release);
    }
    return (ssize_t)written;
}

ssize_t aiorch_ring_read(aiorch_ring *ring, void *target, size_t length, int timeout_ms) {
    if (ring == NULL || ring->producer || target == NULL || timeout_ms < 1) return -EINVAL;
    if (ring->position == ring->total) return 0;
    struct timespec deadline = deadline_after(timeout_ms);
    size_t read_bytes = 0;
    while (read_bytes < length && ring->position < ring->total) {
        if (ring->frame_position == ring->frame_bytes) {
            uint64_t head = atomic_load_explicit(ring->head, memory_order_acquire);
            uint64_t tail = atomic_load_explicit(ring->tail, memory_order_relaxed);
            if (head < tail || head - tail > ring->slots) return -EIO;
            if (head == tail) { int result = pause_until(&deadline); if (result != 0) return result; continue; }
            ring->frame_bytes = ring->slot_size < ring->total - ring->position ? ring->slot_size : ring->total - ring->position;
            ring->frame_position = 0;
        }
        uint64_t remaining = ring->frame_bytes - ring->frame_position;
        size_t amount = remaining < length - read_bytes ? (size_t)remaining : length - read_bytes;
        uint64_t tail = atomic_load_explicit(ring->tail, memory_order_relaxed);
        const unsigned char *source = ring->data + HEADER_SIZE + (tail % ring->slots) * ring->slot_size + ring->frame_position;
        memcpy((unsigned char *)target + read_bytes, source, amount);
        ring->frame_position += amount; ring->position += amount; read_bytes += amount;
        if (ring->frame_position == ring->frame_bytes) atomic_store_explicit(ring->tail, tail + 1, memory_order_release);
    }
    return (ssize_t)read_bytes;
}

uint64_t aiorch_ring_total(const aiorch_ring *ring) { return ring == NULL ? 0 : ring->total; }

int aiorch_ring_close(aiorch_ring *ring) {
    if (ring == NULL) return -EINVAL;
    int result = 0;
    if (ring->producer && ring->position != ring->total) result = -EIO;
    if (munmap(ring->data, ring->mapping_length) != 0 && result == 0) result = -errno;
    if (close(ring->fd) != 0 && result == 0) result = -errno;
    free(ring);
    return result;
}
