// SPDX-License-Identifier: MIT
#ifndef MICRODB_ARENA_H
#define MICRODB_ARENA_H

#include "microdb.h"

static inline void microdb_arena_init(microdb_arena_t *arena, uint8_t *base, size_t capacity) {
    arena->base = base;
    arena->capacity = capacity;
    arena->used = 0;
}

static inline void *microdb_arena_alloc(microdb_arena_t *arena, size_t size, size_t align) {
    size_t aligned = (arena->used + (align - 1u)) & ~(align - 1u);
    void *ptr;

    if (aligned + size > arena->capacity) {
        return NULL;
    }

    ptr = arena->base + aligned;
    arena->used = aligned + size;
    return ptr;
}

static inline size_t microdb_arena_remaining(const microdb_arena_t *arena) {
    return arena->capacity - arena->used;
}

#endif
