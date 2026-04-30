// SPDX-License-Identifier: MIT
#ifndef LOX_LOCK_H
#define LOX_LOCK_H

#include "lox_internal.h"

#if LOX_THREAD_SAFE
#define LOX_LOCK(db)                                                                               \
    do {                                                                                               \
        lox_core_t *lox_lock_core__ = lox_core((db));                                     \
        if (lox_lock_core__->lock != NULL) {                                                       \
            lox_lock_core__->lock(lox_lock_core__->lock_handle);                               \
        }                                                                                              \
    } while (0)
#define LOX_UNLOCK(db)                                                                             \
    do {                                                                                               \
        lox_core_t *lox_lock_core__ = lox_core((db));                                     \
        if (lox_lock_core__->unlock != NULL) {                                                     \
            lox_lock_core__->unlock(lox_lock_core__->lock_handle);                             \
        }                                                                                              \
    } while (0)
#else
#define LOX_LOCK(db) (void)(db)
#define LOX_UNLOCK(db) (void)(db)
#endif

#endif
