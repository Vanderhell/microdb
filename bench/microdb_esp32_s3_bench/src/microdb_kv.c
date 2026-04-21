// SPDX-License-Identifier: MIT
#include "microdb_internal.h"
#include "microdb_lock.h"
#include "microdb_arena.h"

#include <string.h>

enum {
    MICRODB_KV_BUCKET_EMPTY = 0,
    MICRODB_KV_BUCKET_LIVE = 1,
    MICRODB_KV_BUCKET_TOMBSTONE = 2,
    MICRODB_TXN_OP_PUT = 0,
    MICRODB_TXN_OP_DEL = 1
};

static uint32_t microdb_kv_entry_limit(void) {
    return MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS;
}

static uint32_t microdb_align4_u32(uint32_t value) {
    return (value + 3u) & ~3u;
}

static uint32_t microdb_kv_hash(const char *key) {
    uint32_t hash = 2166136261u;
    size_t i;

    for (i = 0; key[i] != '\0'; ++i) {
        hash ^= (uint8_t)key[i];
        hash *= 16777619u;
    }

    return hash;
}

static uint32_t microdb_kv_bucket_count(void) {
    uint32_t key_limit = microdb_kv_entry_limit();
    uint32_t required = (key_limit * 4u + 2u) / 3u;
    uint32_t buckets = 1u;

    while (buckets < required) {
        buckets <<= 1u;
    }

    return buckets;
}

static bool microdb_kv_key_valid(const char *key) {
    size_t len;

    if (key == NULL || key[0] == '\0') {
        return false;
    }

    len = strlen(key);
    return len < MICRODB_KV_KEY_MAX_LEN;
}

static microdb_timestamp_t microdb_now(const microdb_core_t *core) {
    if (core->now == NULL) {
        return 0;
    }

    return core->now();
}

static bool microdb_kv_expired(const microdb_core_t *core, const microdb_kv_bucket_t *bucket) {
#if MICRODB_KV_ENABLE_TTL
    if (bucket->expires_at == 0u) {
        return false;
    }

    return microdb_now(core) >= (microdb_timestamp_t)bucket->expires_at;
#else
    (void)core;
    (void)bucket;
    return false;
#endif
}

static uint32_t microdb_kv_live_value_bytes(const microdb_core_t *core) {
    return core->kv.live_value_bytes;
}

static uint32_t microdb_kv_fragmented_bytes(const microdb_core_t *core) {
    return core->kv.value_used - microdb_kv_live_value_bytes(core);
}

static bool microdb_kv_should_compact(const microdb_core_t *core) {
    if (core->kv.value_used == 0u) {
        return false;
    }

    return microdb_kv_fragmented_bytes(core) * 2u > core->kv.value_used;
}

static void microdb_kv_compact(microdb_core_t *core) {
    uint8_t *dst = core->kv.value_store;
    uint32_t i;

    MICRODB_LOG("INFO",
                "KV val_pool compaction: used=%u/%u live=%u",
                (unsigned)core->kv.value_used,
                (unsigned)core->kv.value_capacity,
                (unsigned)core->kv.entry_count);

    for (i = 0; i < core->kv.bucket_count; ++i) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state != MICRODB_KV_BUCKET_LIVE) {
            continue;
        }

        if (bucket->val_len != 0u && dst != &core->kv.value_store[bucket->val_offset]) {
            memmove(dst, &core->kv.value_store[bucket->val_offset], bucket->val_len);
            bucket->val_offset = (uint32_t)(dst - core->kv.value_store);
        }

        dst += bucket->val_len;
    }

    core->kv.value_used = (uint32_t)(dst - core->kv.value_store);
}

static void microdb_kv_maybe_compact(microdb_core_t *core) {
    if (microdb_kv_should_compact(core)) {
        microdb_kv_compact(core);
    }
}

static microdb_err_t microdb_kv_find_slot(microdb_core_t *core,
                                          const char *key,
                                          uint32_t *slot_out,
                                          bool *found_out,
                                          uint32_t *probe_collisions_out) {
    uint32_t search_hash = microdb_kv_hash(key);
    uint32_t mask = core->kv.bucket_count - 1u;
    uint32_t idx = search_hash & mask;
    uint32_t tombstone = UINT32_MAX;
    uint32_t probed;
    uint32_t probe_collisions = 0u;

    for (probed = 0; probed < core->kv.bucket_count; ++probed) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[idx];

        if (bucket->state == MICRODB_KV_BUCKET_EMPTY) {
            *slot_out = (tombstone != UINT32_MAX) ? tombstone : idx;
            *found_out = false;
            return MICRODB_OK;
        }

        if (bucket->state == MICRODB_KV_BUCKET_TOMBSTONE) {
            if (tombstone == UINT32_MAX) {
                tombstone = idx;
            }
        } else if (bucket->key_hash == search_hash &&
                   strncmp(bucket->key, key, MICRODB_KV_KEY_MAX_LEN) == 0) {
            *slot_out = idx;
            *found_out = true;
            if (probe_collisions_out != NULL) {
                *probe_collisions_out = probe_collisions;
            }
            return MICRODB_OK;
        } else {
            probe_collisions++;
        }

        idx = (idx + 1u) & mask;
    }

    if (tombstone != UINT32_MAX) {
        *slot_out = tombstone;
        *found_out = false;
        if (probe_collisions_out != NULL) {
            *probe_collisions_out = probe_collisions;
        }
        return MICRODB_OK;
    }

    if (probe_collisions_out != NULL) {
        *probe_collisions_out = probe_collisions;
    }
    return MICRODB_ERR_FULL;
}

static void microdb_kv_normalize_access_clock(microdb_core_t *core) {
    uint32_t i;

    if (core->kv.access_clock != UINT32_MAX) {
        return;
    }
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == MICRODB_KV_BUCKET_LIVE) {
            bucket->last_access = 1u;
        }
    }
    core->kv.access_clock = 2u;
}

static uint32_t microdb_kv_next_access_clock(microdb_core_t *core) {
    microdb_kv_normalize_access_clock(core);
    return core->kv.access_clock++;
}

static void microdb_kv_remove_slot(microdb_core_t *core, uint32_t idx) {
    microdb_kv_bucket_t *bucket = &core->kv.buckets[idx];

    if (bucket->state == MICRODB_KV_BUCKET_LIVE && core->kv.entry_count != 0u) {
        core->kv.live_value_bytes -= bucket->val_len;
        core->kv.entry_count--;
    }

    bucket->state = MICRODB_KV_BUCKET_TOMBSTONE;
    bucket->key_hash = 0u;
    bucket->key[0] = '\0';
    bucket->val_offset = 0u;
    bucket->val_len = 0u;
    bucket->expires_at = 0u;
    bucket->last_access = 0u;
}

static void microdb_kv_shift_offsets(microdb_core_t *core, uint32_t start_offset, int32_t delta) {
    uint32_t i;

    for (i = 0; i < core->kv.bucket_count; ++i) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == MICRODB_KV_BUCKET_LIVE && bucket->val_offset > start_offset) {
            bucket->val_offset = (uint32_t)((int32_t)bucket->val_offset + delta);
        }
    }
}

static void microdb_kv_write_bytes(uint8_t *dst, const void *val, size_t len) {
    if (len != 0u) {
        memcpy(dst, val, len);
    }
}

static microdb_err_t microdb_kv_overwrite_value(microdb_core_t *core,
                                                microdb_kv_bucket_t *bucket,
                                                const void *val,
                                                size_t len) {
    uint32_t old_offset = bucket->val_offset;
    uint32_t old_len = bucket->val_len;
    uint32_t tail_offset = old_offset + old_len;
    uint32_t tail_len = core->kv.value_used - tail_offset;

    if (len == old_len) {
        microdb_kv_write_bytes(&core->kv.value_store[old_offset], val, len);
        return MICRODB_OK;
    }

    if (len < old_len) {
        microdb_kv_write_bytes(&core->kv.value_store[old_offset], val, len);
        if (tail_len != 0u) {
            memmove(&core->kv.value_store[old_offset + len],
                    &core->kv.value_store[tail_offset],
                    tail_len);
        }
        microdb_kv_shift_offsets(core, old_offset, -((int32_t)(old_len - len)));
        core->kv.value_used -= (old_len - (uint32_t)len);
        bucket->val_len = (uint32_t)len;
        core->kv.live_value_bytes -= old_len;
        core->kv.live_value_bytes += (uint32_t)len;
        return MICRODB_OK;
    }

    if (core->kv.value_used + (len - old_len) > core->kv.value_capacity) {
        return MICRODB_ERR_NO_MEM;
    }

    if (tail_len != 0u) {
        memmove(&core->kv.value_store[old_offset + len],
                &core->kv.value_store[tail_offset],
                tail_len);
    }
    microdb_kv_shift_offsets(core, old_offset, (int32_t)(len - old_len));
    core->kv.value_used += (uint32_t)(len - old_len);
    microdb_kv_write_bytes(&core->kv.value_store[old_offset], val, len);
    bucket->val_len = (uint32_t)len;
    core->kv.live_value_bytes -= old_len;
    core->kv.live_value_bytes += (uint32_t)len;
    return MICRODB_OK;
}

static microdb_err_t microdb_kv_append_value(microdb_core_t *core,
                                             microdb_kv_bucket_t *bucket,
                                             const void *val,
                                             size_t len) {
    if (core->kv.value_used + len > core->kv.value_capacity) {
        microdb_kv_compact(core);
    }

    if (core->kv.value_used + len > core->kv.value_capacity) {
        return MICRODB_ERR_NO_MEM;
    }

    microdb_kv_write_bytes(&core->kv.value_store[core->kv.value_used], val, len);
    bucket->val_offset = core->kv.value_used;
    bucket->val_len = (uint32_t)len;
    core->kv.value_used += (uint32_t)len;
    core->kv.live_value_bytes += (uint32_t)len;
    return MICRODB_OK;
}

static microdb_err_t microdb_kv_evict_lru(microdb_core_t *core) {
    uint32_t i;
    uint32_t best_idx = UINT32_MAX;
    uint32_t best_clock = UINT32_MAX;

    for (i = 0; i < core->kv.bucket_count; ++i) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state != MICRODB_KV_BUCKET_LIVE) {
            continue;
        }

        if (best_idx == UINT32_MAX || bucket->last_access < best_clock) {
            best_idx = i;
            best_clock = bucket->last_access;
        }
    }

    if (best_idx == UINT32_MAX) {
        return MICRODB_ERR_FULL;
    }

    MICRODB_LOG("WARN",
                "KV LRU eviction: key=%s last_access=%u",
                core->kv.buckets[best_idx].key,
                (unsigned)core->kv.buckets[best_idx].last_access);
    core->kv.eviction_count++;
    microdb_kv_remove_slot(core, best_idx);
    microdb_kv_maybe_compact(core);
    return MICRODB_OK;
}

microdb_err_t microdb_kv_init(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
#if MICRODB_ENABLE_KV
    size_t bucket_bytes;
    size_t stage_bytes;
    uint32_t entry_limit;
#endif

    memset(&core->kv, 0, sizeof(core->kv));
    core->txn_stage = NULL;
    core->txn_active = 0u;
    core->txn_stage_count = 0u;

#if MICRODB_ENABLE_KV
    entry_limit = microdb_kv_entry_limit();
    if (entry_limit == 0u) {
        return MICRODB_ERR_NO_MEM;
    }
    core->kv.bucket_count = microdb_kv_bucket_count();
    bucket_bytes = (size_t)core->kv.bucket_count * sizeof(microdb_kv_bucket_t);
    stage_bytes = (size_t)MICRODB_TXN_STAGE_KEYS * sizeof(microdb_txn_stage_entry_t);

    core->kv.buckets = (microdb_kv_bucket_t *)microdb_arena_alloc(&core->kv_arena, bucket_bytes, 8u);
    core->txn_stage = (microdb_txn_stage_entry_t *)microdb_arena_alloc(&core->kv_arena, stage_bytes, 8u);
    if (core->kv.buckets == NULL || core->txn_stage == NULL) {
        return MICRODB_ERR_NO_MEM;
    }

    memset(core->kv.buckets, 0, bucket_bytes);
    memset(core->txn_stage, 0, stage_bytes);
    core->kv.value_store = core->kv_arena.base + core->kv_arena.used;
    core->kv.value_capacity = (uint32_t)microdb_arena_remaining(&core->kv_arena);
    if (core->kv.value_capacity == 0u) {
        return MICRODB_ERR_NO_MEM;
    }
    core->kv.access_clock = 1u;
    core->kv.live_value_bytes = 0u;
#endif

    return MICRODB_OK;
}

size_t microdb_kv_live_bytes(const microdb_t *db) {
    const microdb_core_t *core = microdb_core_const(db);
    return microdb_kv_live_value_bytes(core) + ((size_t)core->kv.bucket_count * sizeof(microdb_kv_bucket_t));
}

#if MICRODB_ENABLE_KV
static microdb_err_t microdb_kv_set_at_internal(microdb_t *db,
                                                const char *key,
                                                const void *val,
                                                size_t len,
                                                uint32_t expires_at,
                                                bool persist) {
    microdb_core_t *core;
    microdb_kv_bucket_t *bucket;
    uint32_t slot;
    uint32_t probe_collisions = 0u;
    bool found;
    microdb_err_t err;

    if (db == NULL || val == NULL || !microdb_kv_key_valid(key)) {
        return MICRODB_ERR_INVALID;
    }

    if (len > MICRODB_KV_VAL_MAX_LEN) {
        return MICRODB_ERR_OVERFLOW;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_kv_find_slot(core, key, &slot, &found, &probe_collisions);
    if (err != MICRODB_OK && err != MICRODB_ERR_FULL) {
        return err;
    }

    if (!found && core->kv.entry_count >= microdb_kv_entry_limit()) {
#if MICRODB_KV_OVERFLOW_POLICY == MICRODB_KV_POLICY_REJECT
        return MICRODB_ERR_FULL;
#else
        err = microdb_kv_evict_lru(core);
        if (err != MICRODB_OK) {
            return err;
        }
        err = microdb_kv_find_slot(core, key, &slot, &found, &probe_collisions);
        if (err != MICRODB_OK) {
            return err;
        }
#endif
    }

    bucket = &core->kv.buckets[slot];
    if (!found) {
        core->kv.collision_count += probe_collisions;
        core->kv.entry_count++;
    }

    if (found) {
        err = microdb_kv_overwrite_value(core, bucket, val, len);
    } else {
        err = microdb_kv_append_value(core, bucket, val, len);
    }

    if (err != MICRODB_OK) {
        if (!found && core->kv.entry_count != 0u) {
            core->kv.entry_count--;
        }
        return err;
    }

    bucket->state = MICRODB_KV_BUCKET_LIVE;
    bucket->key_hash = microdb_kv_hash(key);
    memcpy(bucket->key, key, strlen(key) + 1u);
    bucket->expires_at = expires_at;
    bucket->last_access = microdb_kv_next_access_clock(core);
    core->live_bytes = microdb_kv_live_bytes(db);
    if (persist) {
        err = microdb_persist_kv_set(db, key, val, len, expires_at);
        if (err != MICRODB_OK) {
            return err;
        }
    }
    return MICRODB_OK;
}

microdb_err_t microdb_kv_set_at(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    return microdb_kv_set_at_internal(db, key, val, len, expires_at, true);
}

microdb_err_t microdb_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t ttl) {
    microdb_core_t *core;
    uint32_t expires_at = 0u;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }
    if (val == NULL || !microdb_kv_key_valid(key)) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }
    if (len > MICRODB_KV_VAL_MAX_LEN) {
        rc = MICRODB_ERR_OVERFLOW;
        goto unlock;
    }

#if MICRODB_KV_ENABLE_TTL
    if (ttl != 0u) {
        expires_at = (uint32_t)(microdb_now(core) + ttl);
    }
#else
    (void)ttl;
#endif

    if (core->txn_active == 1u) {
        microdb_txn_stage_entry_t *entry;
        if (core->txn_stage_count >= MICRODB_TXN_STAGE_KEYS) {
            rc = MICRODB_ERR_FULL;
            goto unlock;
        }
        entry = &core->txn_stage[core->txn_stage_count];
        memset(entry, 0, sizeof(*entry));
        memcpy(entry->key, key, strlen(key) + 1u);
        entry->val_len = len;
        entry->expires_at = expires_at;
        entry->op = MICRODB_TXN_OP_PUT;
        if (len != 0u) {
            memcpy(entry->val_buf, val, len);
        }
        entry->val_ptr = entry->val_buf;
        core->txn_stage_count++;
        rc = MICRODB_OK;
    } else {
        bool wal_first = core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying;
        if (wal_first) {
            rc = microdb_persist_kv_set(db, key, val, len, expires_at);
            if (rc == MICRODB_OK) {
                rc = microdb_kv_set_at_internal(db, key, val, len, expires_at, false);
            }
        } else {
            rc = microdb_kv_set_at_internal(db, key, val, len, expires_at, true);
        }
        if (rc == MICRODB_OK) {
            microdb__maybe_compact(db);
        }
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_kv_get(microdb_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len) {
    microdb_core_t *core;
    microdb_kv_bucket_t *bucket;
    uint32_t slot;
    bool found;
    microdb_err_t err;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL || buf == NULL || !microdb_kv_key_valid(key)) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }
    if (core->txn_active == 1u) {
        int32_t i;
        for (i = (int32_t)core->txn_stage_count - 1; i >= 0; --i) {
            microdb_txn_stage_entry_t *entry = &core->txn_stage[i];
            if (strncmp(entry->key, key, MICRODB_KV_KEY_MAX_LEN) != 0) {
                continue;
            }
            if (entry->op == MICRODB_TXN_OP_DEL) {
                rc = MICRODB_ERR_NOT_FOUND;
                goto unlock;
            }
            if (out_len != NULL) {
                *out_len = entry->val_len;
            }
            if (buf_len < entry->val_len) {
                rc = MICRODB_ERR_OVERFLOW;
                goto unlock;
            }
            if (entry->val_len != 0u) {
                memcpy(buf, entry->val_ptr, entry->val_len);
            }
            rc = MICRODB_OK;
            goto unlock;
        }
    }

    err = microdb_kv_find_slot(core, key, &slot, &found, NULL);
    if (err != MICRODB_OK || !found) {
        rc = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    bucket = &core->kv.buckets[slot];
    if (microdb_kv_expired(core, bucket)) {
        rc = MICRODB_ERR_EXPIRED;
        goto unlock;
    }

    if (out_len != NULL) {
        *out_len = bucket->val_len;
    }

    if (buf_len < bucket->val_len) {
        rc = MICRODB_ERR_OVERFLOW;
        goto unlock;
    }

    if (bucket->val_len != 0u) {
        memcpy(buf, &core->kv.value_store[bucket->val_offset], bucket->val_len);
    }

    bucket->last_access = microdb_kv_next_access_clock(core);
    rc = MICRODB_OK;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

static microdb_err_t microdb_kv_del_internal(microdb_t *db, const char *key, bool persist) {
    microdb_core_t *core;
    uint32_t slot;
    bool found;
    microdb_err_t err;

    core = microdb_core(db);
    err = microdb_kv_find_slot(core, key, &slot, &found, NULL);
    if (err != MICRODB_OK || !found) {
        return MICRODB_ERR_NOT_FOUND;
    }

    microdb_kv_remove_slot(core, slot);
    microdb_kv_maybe_compact(core);
    core->live_bytes = microdb_kv_live_bytes(db);
    if (persist) {
        return microdb_persist_kv_del(db, key);
    }
    return MICRODB_OK;
}

microdb_err_t microdb_kv_del(microdb_t *db, const char *key) {
    microdb_core_t *core;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL || !microdb_kv_key_valid(key)) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    if (core->txn_active == 1u) {
        if (core->txn_stage_count >= MICRODB_TXN_STAGE_KEYS) {
            rc = MICRODB_ERR_FULL;
            goto unlock;
        }
        memset(&core->txn_stage[core->txn_stage_count], 0, sizeof(core->txn_stage[core->txn_stage_count]));
        memcpy(core->txn_stage[core->txn_stage_count].key, key, strlen(key) + 1u);
        core->txn_stage[core->txn_stage_count].op = MICRODB_TXN_OP_DEL;
        core->txn_stage_count++;
        rc = MICRODB_OK;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = microdb_persist_kv_del(db, key);
        if (rc == MICRODB_OK) {
            rc = microdb_kv_del_internal(db, key, false);
        }
    } else {
        rc = microdb_kv_del_internal(db, key, true);
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_kv_exists(microdb_t *db, const char *key) {
    microdb_core_t *core;
    microdb_kv_bucket_t *bucket;
    uint32_t slot;
    bool found;
    microdb_err_t err;

    if (db == NULL || !microdb_kv_key_valid(key)) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        err = MICRODB_ERR_INVALID;
        goto unlock;
    }

    err = microdb_kv_find_slot(core, key, &slot, &found, NULL);
    if (err != MICRODB_OK || !found) {
        err = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    bucket = &core->kv.buckets[slot];
    if (microdb_kv_expired(core, bucket)) {
        err = MICRODB_ERR_EXPIRED;
        goto unlock;
    }

    bucket->last_access = microdb_kv_next_access_clock(core);
    err = MICRODB_OK;

unlock:
    MICRODB_UNLOCK(db);
    return err;
}

microdb_err_t microdb_kv_iter(microdb_t *db, microdb_kv_iter_cb_t cb, void *ctx) {
    uint32_t i = 0u;
    bool done = false;
    bool keep_running = true;

    if (db == NULL || cb == NULL) {
        return MICRODB_ERR_INVALID;
    }

    while (!done && keep_running) {
        char key_copy[MICRODB_KV_KEY_MAX_LEN];
        uint8_t val_copy[MICRODB_KV_VAL_MAX_LEN];
        size_t val_len_copy = 0u;
        uint32_t ttl_remaining = UINT32_MAX;
        bool have_item = false;
        microdb_core_t *core;

        MICRODB_LOCK(db);
        core = microdb_core(db);
        if (core->magic != MICRODB_MAGIC) {
            MICRODB_UNLOCK(db);
            return MICRODB_ERR_INVALID;
        }

        while (i < core->kv.bucket_count) {
            microdb_kv_bucket_t *bucket = &core->kv.buckets[i++];
            if (bucket->state != MICRODB_KV_BUCKET_LIVE || microdb_kv_expired(core, bucket)) {
                continue;
            }

#if MICRODB_KV_ENABLE_TTL
            if (bucket->expires_at != 0u) {
                microdb_timestamp_t now = microdb_now(core);
                ttl_remaining = bucket->expires_at > now ? (uint32_t)(bucket->expires_at - now) : 0u;
            }
#endif
            memcpy(key_copy, bucket->key, sizeof(key_copy));
            val_len_copy = bucket->val_len;
            if (val_len_copy != 0u) {
                memcpy(val_copy, &core->kv.value_store[bucket->val_offset], val_len_copy);
            }
            have_item = true;
            break;
        }

        done = (i >= core->kv.bucket_count) && !have_item;
        /* Callback/lock invariant: user callback is invoked without DB lock held. */
        MICRODB_UNLOCK(db);

        if (have_item) {
            keep_running = cb(key_copy, val_copy, val_len_copy, ttl_remaining, ctx);
        }
    }

    return MICRODB_OK;
}

microdb_err_t microdb_kv_purge_expired(microdb_t *db) {
    microdb_core_t *core;
    uint32_t i;
    bool wal_mode;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    wal_mode = core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying;
    for (i = 0; i < core->kv.bucket_count; ++i) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == MICRODB_KV_BUCKET_LIVE && microdb_kv_expired(core, bucket)) {
            if (wal_mode) {
                rc = microdb_persist_kv_del(db, bucket->key);
                if (rc != MICRODB_OK) {
                    goto unlock;
                }
            }
            microdb_kv_remove_slot(core, i);
        }
    }

    microdb_kv_maybe_compact(core);
    core->live_bytes = microdb_kv_live_bytes(db);
    if (!wal_mode) {
        rc = microdb_storage_flush(db);
    } else {
        rc = MICRODB_OK;
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_kv_clear(microdb_t *db) {
    microdb_core_t *core;
    size_t bucket_bytes;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = microdb_persist_kv_clear(db);
        if (rc != MICRODB_OK) {
            goto unlock;
        }
    }

    bucket_bytes = (size_t)core->kv.bucket_count * sizeof(microdb_kv_bucket_t);
    memset(core->kv.buckets, 0, bucket_bytes);
    core->kv.entry_count = 0u;
    core->kv.value_used = 0u;
    core->kv.live_value_bytes = 0u;
    core->kv.access_clock = 1u;
    core->live_bytes = microdb_kv_live_bytes(db);
    if (!(core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying)) {
        rc = microdb_persist_kv_clear(db);
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_txn_begin(microdb_t *db) {
    microdb_core_t *core;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }
    if (core->txn_active == 1u) {
        rc = MICRODB_ERR_TXN_ACTIVE;
        goto unlock;
    }
    core->txn_active = 1u;
    core->txn_stage_count = 0u;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_txn_commit(microdb_t *db) {
    microdb_core_t *core;
    uint32_t i;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }
    if (core->txn_active == 0u) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    /* TXN visibility invariant:
     * - stage entries are durable in WAL before commit marker.
     * - staged entries become visible in live KV only after durable TXN_COMMIT marker.
     */
    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        uint32_t needed = 16u; /* TXN_COMMIT marker */
        for (i = 0u; i < core->txn_stage_count; ++i) {
            microdb_txn_stage_entry_t *entry = &core->txn_stage[i];
            uint32_t key_len = (uint32_t)strlen(entry->key);
            uint32_t payload_len = (entry->op == MICRODB_TXN_OP_PUT) ? (1u + key_len + 4u + (uint32_t)entry->val_len + 4u)
                                                                     : (1u + key_len);
            needed += 16u + microdb_align4_u32(payload_len);
        }
        if (core->wal_used + needed > core->layout.wal_size) {
            rc = microdb_storage_flush(db);
            if (rc != MICRODB_OK) {
                goto unlock;
            }
        }
    }

    for (i = 0u; i < core->txn_stage_count; ++i) {
        microdb_txn_stage_entry_t *entry = &core->txn_stage[i];
        if (entry->op == MICRODB_TXN_OP_PUT) {
            rc = microdb_persist_kv_set_txn(db, entry->key, entry->val_ptr, entry->val_len, entry->expires_at);
            if (rc != MICRODB_OK) {
                goto unlock;
            }
        } else {
            rc = microdb_persist_kv_del_txn(db, entry->key);
            if (rc != MICRODB_OK) {
                goto unlock;
            }
        }
    }

    rc = microdb_persist_txn_commit(db);
    if (rc != MICRODB_OK) {
        goto unlock;
    }

    for (i = 0u; i < core->txn_stage_count; ++i) {
        microdb_txn_stage_entry_t *entry = &core->txn_stage[i];
        if (entry->op == MICRODB_TXN_OP_PUT) {
            rc = microdb_kv_set_at_internal(db, entry->key, entry->val_ptr, entry->val_len, entry->expires_at, false);
            if (rc != MICRODB_OK) {
                goto unlock;
            }
        } else {
            rc = microdb_kv_del_internal(db, entry->key, false);
            if (rc != MICRODB_OK && rc != MICRODB_ERR_NOT_FOUND) {
                goto unlock;
            }
        }
    }

    core->txn_active = 0u;
    core->txn_stage_count = 0u;
    rc = MICRODB_OK;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_txn_rollback(microdb_t *db) {
    microdb_core_t *core;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }
    core->txn_active = 0u;
    core->txn_stage_count = 0u;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}
#else
microdb_err_t microdb_kv_set_at(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    (void)db;
    (void)key;
    (void)val;
    (void)len;
    (void)expires_at;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t ttl) {
    (void)db;
    (void)key;
    (void)val;
    (void)len;
    (void)ttl;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_get(microdb_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len) {
    (void)db;
    (void)key;
    (void)buf;
    (void)buf_len;
    (void)out_len;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_del(microdb_t *db, const char *key) {
    (void)db;
    (void)key;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_exists(microdb_t *db, const char *key) {
    (void)db;
    (void)key;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_iter(microdb_t *db, microdb_kv_iter_cb_t cb, void *ctx) {
    (void)db;
    (void)cb;
    (void)ctx;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_purge_expired(microdb_t *db) {
    (void)db;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_kv_clear(microdb_t *db) {
    (void)db;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_txn_begin(microdb_t *db) {
    (void)db;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_txn_commit(microdb_t *db) {
    (void)db;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_txn_rollback(microdb_t *db) {
    (void)db;
    return MICRODB_ERR_DISABLED;
}
#endif
