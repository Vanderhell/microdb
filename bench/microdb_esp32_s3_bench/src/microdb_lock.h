// SPDX-License-Identifier: MIT
#ifndef MICRODB_LOCK_H
#define MICRODB_LOCK_H

#include "microdb_internal.h"

#if MICRODB_THREAD_SAFE
#define MICRODB_LOCK(db)                                                                               \
    do {                                                                                               \
        microdb_core_t *microdb_lock_core__ = microdb_core((db));                                     \
        if (microdb_lock_core__->lock != NULL) {                                                       \
            microdb_lock_core__->lock(microdb_lock_core__->lock_handle);                               \
        }                                                                                              \
    } while (0)
#define MICRODB_UNLOCK(db)                                                                             \
    do {                                                                                               \
        microdb_core_t *microdb_lock_core__ = microdb_core((db));                                     \
        if (microdb_lock_core__->unlock != NULL) {                                                     \
            microdb_lock_core__->unlock(microdb_lock_core__->lock_handle);                             \
        }                                                                                              \
    } while (0)
#else
#define MICRODB_LOCK(db) (void)(db)
#define MICRODB_UNLOCK(db) (void)(db)
#endif

#endif
