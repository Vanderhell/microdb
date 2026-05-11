// SPDX-License-Identifier: MIT
#ifndef LOX_ARENA_H
#define LOX_ARENA_H

#include "lox.h"

static inline void lox_arena_init(lox_arena_t *arena, uint8_t *base, size_t capacity) {
    arena->base = base;
    arena->capacity = capacity;
    arena->used = 0;
}

static inline void *lox_arena_alloc(lox_arena_t *arena, size_t size, size_t align) {
    size_t aligned = (arena->used + (align - 1u)) & ~(align - 1u);
    void *ptr;

    if (aligned + size > arena->capacity) {
        return NULL;
    }

    ptr = arena->base + aligned;
    arena->used = aligned + size;
    return ptr;
}

static inline size_t lox_arena_remaining(const lox_arena_t *arena) {
    return arena->capacity - arena->used;
}

#endif
