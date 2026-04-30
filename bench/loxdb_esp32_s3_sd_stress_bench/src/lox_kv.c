// SPDX-License-Identifier: MIT
#include "lox_internal.h"
#include "lox_lock.h"
#include "lox_arena.h"

#include <string.h>

enum {
    LOX_KV_BUCKET_EMPTY = 0,
    LOX_KV_BUCKET_LIVE = 1,
    LOX_KV_BUCKET_TOMBSTONE = 2,
    LOX_TXN_OP_PUT = 0,
    LOX_TXN_OP_DEL = 1
};

static uint32_t lox_kv_entry_limit(void) {
    return LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS;
}

static uint32_t lox_align4_u32(uint32_t value) {
    return (value + 3u) & ~3u;
}

static uint32_t lox_kv_hash(const char *key) {
    uint32_t hash = 2166136261u;
    size_t i;

    for (i = 0; key[i] != '\0'; ++i) {
        hash ^= (uint8_t)key[i];
        hash *= 16777619u;
    }

    return hash;
}

static uint32_t lox_kv_bucket_count(void) {
    uint32_t key_limit = lox_kv_entry_limit();
    uint32_t required = (key_limit * 4u + 2u) / 3u;
    uint32_t buckets = 1u;

    while (buckets < required) {
        buckets <<= 1u;
    }

    return buckets;
}

static bool lox_kv_key_valid(const char *key) {
    size_t len;

    if (key == NULL || key[0] == '\0') {
        return false;
    }

    len = strlen(key);
    return len < LOX_KV_KEY_MAX_LEN;
}

static lox_timestamp_t lox_now(const lox_core_t *core) {
    if (core->now == NULL) {
        return 0;
    }

    return core->now();
}

static bool lox_kv_expired(const lox_core_t *core, const lox_kv_bucket_t *bucket) {
#if LOX_KV_ENABLE_TTL
    if (bucket->expires_at == 0u) {
        return false;
    }

    return lox_now(core) >= (lox_timestamp_t)bucket->expires_at;
#else
    (void)core;
    (void)bucket;
    return false;
#endif
}

static uint32_t lox_kv_live_value_bytes(const lox_core_t *core) {
    return core->kv.live_value_bytes;
}

static uint32_t lox_kv_fragmented_bytes(const lox_core_t *core) {
    return core->kv.value_used - lox_kv_live_value_bytes(core);
}

static bool lox_kv_should_compact(const lox_core_t *core) {
    if (core->kv.value_used == 0u) {
        return false;
    }

    return lox_kv_fragmented_bytes(core) * 2u > core->kv.value_used;
}

static void lox_kv_compact(lox_core_t *core) {
    uint8_t *dst = core->kv.value_store;
    uint32_t i;

    LOX_LOG("INFO",
                "KV val_pool compaction: used=%u/%u live=%u",
                (unsigned)core->kv.value_used,
                (unsigned)core->kv.value_capacity,
                (unsigned)core->kv.entry_count);

    for (i = 0; i < core->kv.bucket_count; ++i) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state != LOX_KV_BUCKET_LIVE) {
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

static void lox_kv_maybe_compact(lox_core_t *core) {
    if (lox_kv_should_compact(core)) {
        lox_kv_compact(core);
    }
}

static lox_err_t lox_kv_find_slot(lox_core_t *core,
                                          const char *key,
                                          uint32_t *slot_out,
                                          bool *found_out,
                                          uint32_t *probe_collisions_out) {
    uint32_t search_hash = lox_kv_hash(key);
    uint32_t mask = core->kv.bucket_count - 1u;
    uint32_t idx = search_hash & mask;
    uint32_t tombstone = UINT32_MAX;
    uint32_t probed;
    uint32_t probe_collisions = 0u;

    for (probed = 0; probed < core->kv.bucket_count; ++probed) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[idx];

        if (bucket->state == LOX_KV_BUCKET_EMPTY) {
            *slot_out = (tombstone != UINT32_MAX) ? tombstone : idx;
            *found_out = false;
            return LOX_OK;
        }

        if (bucket->state == LOX_KV_BUCKET_TOMBSTONE) {
            if (tombstone == UINT32_MAX) {
                tombstone = idx;
            }
        } else if (bucket->key_hash == search_hash &&
                   strncmp(bucket->key, key, LOX_KV_KEY_MAX_LEN) == 0) {
            *slot_out = idx;
            *found_out = true;
            if (probe_collisions_out != NULL) {
                *probe_collisions_out = probe_collisions;
            }
            return LOX_OK;
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
        return LOX_OK;
    }

    if (probe_collisions_out != NULL) {
        *probe_collisions_out = probe_collisions;
    }
    return LOX_ERR_FULL;
}

static void lox_kv_normalize_access_clock(lox_core_t *core) {
    uint32_t i;

    if (core->kv.access_clock != UINT32_MAX) {
        return;
    }
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == LOX_KV_BUCKET_LIVE) {
            bucket->last_access = 1u;
        }
    }
    core->kv.access_clock = 2u;
}

static uint32_t lox_kv_next_access_clock(lox_core_t *core) {
    lox_kv_normalize_access_clock(core);
    return core->kv.access_clock++;
}

static void lox_kv_remove_slot(lox_core_t *core, uint32_t idx) {
    lox_kv_bucket_t *bucket = &core->kv.buckets[idx];

    if (bucket->state == LOX_KV_BUCKET_LIVE && core->kv.entry_count != 0u) {
        core->kv.live_value_bytes -= bucket->val_len;
        core->kv.entry_count--;
    }

    bucket->state = LOX_KV_BUCKET_TOMBSTONE;
    bucket->key_hash = 0u;
    bucket->key[0] = '\0';
    bucket->val_offset = 0u;
    bucket->val_len = 0u;
    bucket->expires_at = 0u;
    bucket->last_access = 0u;
}

static void lox_kv_shift_offsets(lox_core_t *core, uint32_t start_offset, int32_t delta) {
    uint32_t i;

    for (i = 0; i < core->kv.bucket_count; ++i) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == LOX_KV_BUCKET_LIVE && bucket->val_offset > start_offset) {
            bucket->val_offset = (uint32_t)((int32_t)bucket->val_offset + delta);
        }
    }
}

static void lox_kv_write_bytes(uint8_t *dst, const void *val, size_t len) {
    if (len != 0u) {
        memcpy(dst, val, len);
    }
}

static lox_err_t lox_kv_overwrite_value(lox_core_t *core,
                                                lox_kv_bucket_t *bucket,
                                                const void *val,
                                                size_t len) {
    uint32_t old_offset = bucket->val_offset;
    uint32_t old_len = bucket->val_len;
    uint32_t tail_offset = old_offset + old_len;
    uint32_t tail_len = core->kv.value_used - tail_offset;

    if (len == old_len) {
        lox_kv_write_bytes(&core->kv.value_store[old_offset], val, len);
        return LOX_OK;
    }

    if (len < old_len) {
        lox_kv_write_bytes(&core->kv.value_store[old_offset], val, len);
        if (tail_len != 0u) {
            memmove(&core->kv.value_store[old_offset + len],
                    &core->kv.value_store[tail_offset],
                    tail_len);
        }
        lox_kv_shift_offsets(core, old_offset, -((int32_t)(old_len - len)));
        core->kv.value_used -= (old_len - (uint32_t)len);
        bucket->val_len = (uint32_t)len;
        core->kv.live_value_bytes -= old_len;
        core->kv.live_value_bytes += (uint32_t)len;
        return LOX_OK;
    }

    if (core->kv.value_used + (len - old_len) > core->kv.value_capacity) {
        return LOX_ERR_NO_MEM;
    }

    if (tail_len != 0u) {
        memmove(&core->kv.value_store[old_offset + len],
                &core->kv.value_store[tail_offset],
                tail_len);
    }
    lox_kv_shift_offsets(core, old_offset, (int32_t)(len - old_len));
    core->kv.value_used += (uint32_t)(len - old_len);
    lox_kv_write_bytes(&core->kv.value_store[old_offset], val, len);
    bucket->val_len = (uint32_t)len;
    core->kv.live_value_bytes -= old_len;
    core->kv.live_value_bytes += (uint32_t)len;
    return LOX_OK;
}

static lox_err_t lox_kv_append_value(lox_core_t *core,
                                             lox_kv_bucket_t *bucket,
                                             const void *val,
                                             size_t len) {
    if (core->kv.value_used + len > core->kv.value_capacity) {
        lox_kv_compact(core);
    }

    if (core->kv.value_used + len > core->kv.value_capacity) {
        return LOX_ERR_NO_MEM;
    }

    lox_kv_write_bytes(&core->kv.value_store[core->kv.value_used], val, len);
    bucket->val_offset = core->kv.value_used;
    bucket->val_len = (uint32_t)len;
    core->kv.value_used += (uint32_t)len;
    core->kv.live_value_bytes += (uint32_t)len;
    return LOX_OK;
}

static lox_err_t lox_kv_evict_lru(lox_core_t *core) {
    uint32_t i;
    uint32_t best_idx = UINT32_MAX;
    uint32_t best_clock = UINT32_MAX;

    for (i = 0; i < core->kv.bucket_count; ++i) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state != LOX_KV_BUCKET_LIVE) {
            continue;
        }

        if (best_idx == UINT32_MAX || bucket->last_access < best_clock) {
            best_idx = i;
            best_clock = bucket->last_access;
        }
    }

    if (best_idx == UINT32_MAX) {
        return LOX_ERR_FULL;
    }

    LOX_LOG("WARN",
                "KV LRU eviction: key=%s last_access=%u",
                core->kv.buckets[best_idx].key,
                (unsigned)core->kv.buckets[best_idx].last_access);
    core->kv.eviction_count++;
    lox_kv_remove_slot(core, best_idx);
    lox_kv_maybe_compact(core);
    return LOX_OK;
}

lox_err_t lox_kv_init(lox_t *db) {
    lox_core_t *core = lox_core(db);
#if LOX_ENABLE_KV
    size_t bucket_bytes;
    size_t stage_bytes;
    uint32_t entry_limit;
#endif

    memset(&core->kv, 0, sizeof(core->kv));
    core->txn_stage = NULL;
    core->txn_active = 0u;
    core->txn_stage_count = 0u;

#if LOX_ENABLE_KV
    entry_limit = lox_kv_entry_limit();
    if (entry_limit == 0u) {
        return LOX_ERR_NO_MEM;
    }
    core->kv.bucket_count = lox_kv_bucket_count();
    bucket_bytes = (size_t)core->kv.bucket_count * sizeof(lox_kv_bucket_t);
    stage_bytes = (size_t)LOX_TXN_STAGE_KEYS * sizeof(lox_txn_stage_entry_t);

    core->kv.buckets = (lox_kv_bucket_t *)lox_arena_alloc(&core->kv_arena, bucket_bytes, 8u);
    core->txn_stage = (lox_txn_stage_entry_t *)lox_arena_alloc(&core->kv_arena, stage_bytes, 8u);
    if (core->kv.buckets == NULL || core->txn_stage == NULL) {
        return LOX_ERR_NO_MEM;
    }

    memset(core->kv.buckets, 0, bucket_bytes);
    memset(core->txn_stage, 0, stage_bytes);
    core->kv.value_store = core->kv_arena.base + core->kv_arena.used;
    core->kv.value_capacity = (uint32_t)lox_arena_remaining(&core->kv_arena);
    if (core->kv.value_capacity == 0u) {
        return LOX_ERR_NO_MEM;
    }
    core->kv.access_clock = 1u;
    core->kv.live_value_bytes = 0u;
#endif

    return LOX_OK;
}

size_t lox_kv_live_bytes(const lox_t *db) {
    const lox_core_t *core = lox_core_const(db);
    return lox_kv_live_value_bytes(core) + ((size_t)core->kv.bucket_count * sizeof(lox_kv_bucket_t));
}

#if LOX_ENABLE_KV
static lox_err_t lox_kv_set_at_internal(lox_t *db,
                                                const char *key,
                                                const void *val,
                                                size_t len,
                                                uint32_t expires_at,
                                                bool persist) {
    lox_core_t *core;
    lox_kv_bucket_t *bucket;
    uint32_t slot;
    uint32_t probe_collisions = 0u;
    bool found;
    lox_err_t err;

    if (db == NULL || val == NULL || !lox_kv_key_valid(key)) {
        return LOX_ERR_INVALID;
    }

    if (len > LOX_KV_VAL_MAX_LEN) {
        return LOX_ERR_OVERFLOW;
    }

    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        return LOX_ERR_INVALID;
    }

    err = lox_kv_find_slot(core, key, &slot, &found, &probe_collisions);
    if (err != LOX_OK && err != LOX_ERR_FULL) {
        return err;
    }

    if (!found && core->kv.entry_count >= lox_kv_entry_limit()) {
#if LOX_KV_OVERFLOW_POLICY == LOX_KV_POLICY_REJECT
        return LOX_ERR_FULL;
#else
        err = lox_kv_evict_lru(core);
        if (err != LOX_OK) {
            return err;
        }
        err = lox_kv_find_slot(core, key, &slot, &found, &probe_collisions);
        if (err != LOX_OK) {
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
        err = lox_kv_overwrite_value(core, bucket, val, len);
    } else {
        err = lox_kv_append_value(core, bucket, val, len);
    }

    if (err != LOX_OK) {
        if (!found && core->kv.entry_count != 0u) {
            core->kv.entry_count--;
        }
        return err;
    }

    bucket->state = LOX_KV_BUCKET_LIVE;
    bucket->key_hash = lox_kv_hash(key);
    memcpy(bucket->key, key, strlen(key) + 1u);
    bucket->expires_at = expires_at;
    bucket->last_access = lox_kv_next_access_clock(core);
    core->live_bytes = lox_kv_live_bytes(db);
    if (persist) {
        err = lox_persist_kv_set(db, key, val, len, expires_at);
        if (err != LOX_OK) {
            return err;
        }
    }
    return LOX_OK;
}

lox_err_t lox_kv_set_at(lox_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    return lox_kv_set_at_internal(db, key, val, len, expires_at, true);
}

lox_err_t lox_kv_set(lox_t *db, const char *key, const void *val, size_t len, uint32_t ttl) {
    lox_core_t *core;
    uint32_t expires_at = 0u;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (val == NULL || !lox_kv_key_valid(key)) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (len > LOX_KV_VAL_MAX_LEN) {
        rc = LOX_ERR_OVERFLOW;
        goto unlock;
    }

#if LOX_KV_ENABLE_TTL
    if (ttl != 0u) {
        expires_at = (uint32_t)(lox_now(core) + ttl);
    }
#else
    (void)ttl;
#endif

    if (core->txn_active == 1u) {
        lox_txn_stage_entry_t *entry;
        if (core->txn_stage_count >= LOX_TXN_STAGE_KEYS) {
            rc = LOX_ERR_FULL;
            goto unlock;
        }
        entry = &core->txn_stage[core->txn_stage_count];
        memset(entry, 0, sizeof(*entry));
        memcpy(entry->key, key, strlen(key) + 1u);
        entry->val_len = len;
        entry->expires_at = expires_at;
        entry->op = LOX_TXN_OP_PUT;
        if (len != 0u) {
            memcpy(entry->val_buf, val, len);
        }
        entry->val_ptr = entry->val_buf;
        core->txn_stage_count++;
        rc = LOX_OK;
    } else {
        bool wal_first = core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying;
        if (wal_first) {
            rc = lox_persist_kv_set(db, key, val, len, expires_at);
            if (rc == LOX_OK) {
                rc = lox_kv_set_at_internal(db, key, val, len, expires_at, false);
            }
        } else {
            rc = lox_kv_set_at_internal(db, key, val, len, expires_at, true);
        }
        if (rc == LOX_OK) {
            lox__maybe_compact(db);
        }
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_kv_get(lox_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len) {
    lox_core_t *core;
    lox_kv_bucket_t *bucket;
    uint32_t slot;
    bool found;
    lox_err_t err;
    lox_err_t rc = LOX_OK;

    if (db == NULL || buf == NULL || !lox_kv_key_valid(key)) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (core->txn_active == 1u) {
        int32_t i;
        for (i = (int32_t)core->txn_stage_count - 1; i >= 0; --i) {
            lox_txn_stage_entry_t *entry = &core->txn_stage[i];
            if (strncmp(entry->key, key, LOX_KV_KEY_MAX_LEN) != 0) {
                continue;
            }
            if (entry->op == LOX_TXN_OP_DEL) {
                rc = LOX_ERR_NOT_FOUND;
                goto unlock;
            }
            if (out_len != NULL) {
                *out_len = entry->val_len;
            }
            if (buf_len < entry->val_len) {
                rc = LOX_ERR_OVERFLOW;
                goto unlock;
            }
            if (entry->val_len != 0u) {
                memcpy(buf, entry->val_ptr, entry->val_len);
            }
            rc = LOX_OK;
            goto unlock;
        }
    }

    err = lox_kv_find_slot(core, key, &slot, &found, NULL);
    if (err != LOX_OK || !found) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    bucket = &core->kv.buckets[slot];
    if (lox_kv_expired(core, bucket)) {
        rc = LOX_ERR_EXPIRED;
        goto unlock;
    }

    if (out_len != NULL) {
        *out_len = bucket->val_len;
    }

    if (buf_len < bucket->val_len) {
        rc = LOX_ERR_OVERFLOW;
        goto unlock;
    }

    if (bucket->val_len != 0u) {
        memcpy(buf, &core->kv.value_store[bucket->val_offset], bucket->val_len);
    }

    bucket->last_access = lox_kv_next_access_clock(core);
    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

static lox_err_t lox_kv_del_internal(lox_t *db, const char *key, bool persist) {
    lox_core_t *core;
    uint32_t slot;
    bool found;
    lox_err_t err;

    core = lox_core(db);
    err = lox_kv_find_slot(core, key, &slot, &found, NULL);
    if (err != LOX_OK || !found) {
        return LOX_ERR_NOT_FOUND;
    }

    lox_kv_remove_slot(core, slot);
    lox_kv_maybe_compact(core);
    core->live_bytes = lox_kv_live_bytes(db);
    if (persist) {
        return lox_persist_kv_del(db, key);
    }
    return LOX_OK;
}

lox_err_t lox_kv_del(lox_t *db, const char *key) {
    lox_core_t *core;
    lox_err_t rc = LOX_OK;

    if (db == NULL || !lox_kv_key_valid(key)) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    if (core->txn_active == 1u) {
        if (core->txn_stage_count >= LOX_TXN_STAGE_KEYS) {
            rc = LOX_ERR_FULL;
            goto unlock;
        }
        memset(&core->txn_stage[core->txn_stage_count], 0, sizeof(core->txn_stage[core->txn_stage_count]));
        memcpy(core->txn_stage[core->txn_stage_count].key, key, strlen(key) + 1u);
        core->txn_stage[core->txn_stage_count].op = LOX_TXN_OP_DEL;
        core->txn_stage_count++;
        rc = LOX_OK;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = lox_persist_kv_del(db, key);
        if (rc == LOX_OK) {
            rc = lox_kv_del_internal(db, key, false);
        }
    } else {
        rc = lox_kv_del_internal(db, key, true);
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_kv_exists(lox_t *db, const char *key) {
    lox_core_t *core;
    lox_kv_bucket_t *bucket;
    uint32_t slot;
    bool found;
    lox_err_t err;

    if (db == NULL || !lox_kv_key_valid(key)) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        err = LOX_ERR_INVALID;
        goto unlock;
    }

    err = lox_kv_find_slot(core, key, &slot, &found, NULL);
    if (err != LOX_OK || !found) {
        err = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    bucket = &core->kv.buckets[slot];
    if (lox_kv_expired(core, bucket)) {
        err = LOX_ERR_EXPIRED;
        goto unlock;
    }

    bucket->last_access = lox_kv_next_access_clock(core);
    err = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return err;
}

lox_err_t lox_kv_iter(lox_t *db, lox_kv_iter_cb_t cb, void *ctx) {
    uint32_t i = 0u;
    bool done = false;
    bool keep_running = true;

    if (db == NULL || cb == NULL) {
        return LOX_ERR_INVALID;
    }

    while (!done && keep_running) {
        char key_copy[LOX_KV_KEY_MAX_LEN];
        uint8_t val_copy[LOX_KV_VAL_MAX_LEN];
        size_t val_len_copy = 0u;
        uint32_t ttl_remaining = UINT32_MAX;
        bool have_item = false;
        lox_core_t *core;

        LOX_LOCK(db);
        core = lox_core(db);
        if (core->magic != LOX_MAGIC) {
            LOX_UNLOCK(db);
            return LOX_ERR_INVALID;
        }

        while (i < core->kv.bucket_count) {
            lox_kv_bucket_t *bucket = &core->kv.buckets[i++];
            if (bucket->state != LOX_KV_BUCKET_LIVE || lox_kv_expired(core, bucket)) {
                continue;
            }

#if LOX_KV_ENABLE_TTL
            if (bucket->expires_at != 0u) {
                lox_timestamp_t now = lox_now(core);
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
        LOX_UNLOCK(db);

        if (have_item) {
            keep_running = cb(key_copy, val_copy, val_len_copy, ttl_remaining, ctx);
        }
    }

    return LOX_OK;
}

lox_err_t lox_kv_purge_expired(lox_t *db) {
    lox_core_t *core;
    uint32_t i;
    bool wal_mode;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    wal_mode = core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying;
    for (i = 0; i < core->kv.bucket_count; ++i) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == LOX_KV_BUCKET_LIVE && lox_kv_expired(core, bucket)) {
            if (wal_mode) {
                rc = lox_persist_kv_del(db, bucket->key);
                if (rc != LOX_OK) {
                    goto unlock;
                }
            }
            lox_kv_remove_slot(core, i);
        }
    }

    lox_kv_maybe_compact(core);
    core->live_bytes = lox_kv_live_bytes(db);
    if (!wal_mode) {
        rc = lox_storage_flush(db);
    } else {
        rc = LOX_OK;
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_kv_clear(lox_t *db) {
    lox_core_t *core;
    size_t bucket_bytes;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = lox_persist_kv_clear(db);
        if (rc != LOX_OK) {
            goto unlock;
        }
    }

    bucket_bytes = (size_t)core->kv.bucket_count * sizeof(lox_kv_bucket_t);
    memset(core->kv.buckets, 0, bucket_bytes);
    core->kv.entry_count = 0u;
    core->kv.value_used = 0u;
    core->kv.live_value_bytes = 0u;
    core->kv.access_clock = 1u;
    core->live_bytes = lox_kv_live_bytes(db);
    if (!(core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying)) {
        rc = lox_persist_kv_clear(db);
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_txn_begin(lox_t *db) {
    lox_core_t *core;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (core->txn_active == 1u) {
        rc = LOX_ERR_TXN_ACTIVE;
        goto unlock;
    }
    core->txn_active = 1u;
    core->txn_stage_count = 0u;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_txn_commit(lox_t *db) {
    lox_core_t *core;
    uint32_t i;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (core->txn_active == 0u) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    /* TXN visibility invariant:
     * - stage entries are durable in WAL before commit marker.
     * - staged entries become visible in live KV only after durable TXN_COMMIT marker.
     */
    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        uint32_t needed = 16u; /* TXN_COMMIT marker */
        for (i = 0u; i < core->txn_stage_count; ++i) {
            lox_txn_stage_entry_t *entry = &core->txn_stage[i];
            uint32_t key_len = (uint32_t)strlen(entry->key);
            uint32_t payload_len = (entry->op == LOX_TXN_OP_PUT) ? (1u + key_len + 4u + (uint32_t)entry->val_len + 4u)
                                                                     : (1u + key_len);
            needed += 16u + lox_align4_u32(payload_len);
        }
        if (core->wal_used + needed > core->layout.wal_size) {
            rc = lox_storage_flush(db);
            if (rc != LOX_OK) {
                goto unlock;
            }
        }
    }

    for (i = 0u; i < core->txn_stage_count; ++i) {
        lox_txn_stage_entry_t *entry = &core->txn_stage[i];
        if (entry->op == LOX_TXN_OP_PUT) {
            rc = lox_persist_kv_set_txn(db, entry->key, entry->val_ptr, entry->val_len, entry->expires_at);
            if (rc != LOX_OK) {
                goto unlock;
            }
        } else {
            rc = lox_persist_kv_del_txn(db, entry->key);
            if (rc != LOX_OK) {
                goto unlock;
            }
        }
    }

    rc = lox_persist_txn_commit(db);
    if (rc != LOX_OK) {
        goto unlock;
    }

    for (i = 0u; i < core->txn_stage_count; ++i) {
        lox_txn_stage_entry_t *entry = &core->txn_stage[i];
        if (entry->op == LOX_TXN_OP_PUT) {
            rc = lox_kv_set_at_internal(db, entry->key, entry->val_ptr, entry->val_len, entry->expires_at, false);
            if (rc != LOX_OK) {
                goto unlock;
            }
        } else {
            rc = lox_kv_del_internal(db, entry->key, false);
            if (rc != LOX_OK && rc != LOX_ERR_NOT_FOUND) {
                goto unlock;
            }
        }
    }

    core->txn_active = 0u;
    core->txn_stage_count = 0u;
    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_txn_rollback(lox_t *db) {
    lox_core_t *core;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    core->txn_active = 0u;
    core->txn_stage_count = 0u;

unlock:
    LOX_UNLOCK(db);
    return rc;
}
#else
lox_err_t lox_kv_set_at(lox_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    (void)db;
    (void)key;
    (void)val;
    (void)len;
    (void)expires_at;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_set(lox_t *db, const char *key, const void *val, size_t len, uint32_t ttl) {
    (void)db;
    (void)key;
    (void)val;
    (void)len;
    (void)ttl;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_get(lox_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len) {
    (void)db;
    (void)key;
    (void)buf;
    (void)buf_len;
    (void)out_len;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_del(lox_t *db, const char *key) {
    (void)db;
    (void)key;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_exists(lox_t *db, const char *key) {
    (void)db;
    (void)key;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_iter(lox_t *db, lox_kv_iter_cb_t cb, void *ctx) {
    (void)db;
    (void)cb;
    (void)ctx;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_purge_expired(lox_t *db) {
    (void)db;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_kv_clear(lox_t *db) {
    (void)db;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_txn_begin(lox_t *db) {
    (void)db;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_txn_commit(lox_t *db) {
    (void)db;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_txn_rollback(lox_t *db) {
    (void)db;
    return LOX_ERR_DISABLED;
}
#endif
