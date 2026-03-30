#include "microdb_internal.h"

#include <string.h>

enum {
    MICRODB_KV_BUCKET_EMPTY = 0,
    MICRODB_KV_BUCKET_LIVE = 1,
    MICRODB_KV_BUCKET_TOMBSTONE = 2
};

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
    uint32_t required = (MICRODB_KV_MAX_KEYS * 4u + 2u) / 3u;
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
    uint32_t i;
    uint32_t total = 0u;

    for (i = 0; i < core->kv.bucket_count; ++i) {
        if (core->kv.buckets[i].state == MICRODB_KV_BUCKET_LIVE) {
            total += core->kv.buckets[i].val_len;
        }
    }

    return total;
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

static microdb_err_t microdb_kv_find_slot(microdb_core_t *core, const char *key, uint32_t *slot_out, bool *found_out) {
    uint32_t mask = core->kv.bucket_count - 1u;
    uint32_t idx = microdb_kv_hash(key) & mask;
    uint32_t tombstone = UINT32_MAX;
    uint32_t probed;

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
        } else if (strncmp(bucket->key, key, MICRODB_KV_KEY_MAX_LEN) == 0) {
            *slot_out = idx;
            *found_out = true;
            return MICRODB_OK;
        }

        idx = (idx + 1u) & mask;
    }

    if (tombstone != UINT32_MAX) {
        *slot_out = tombstone;
        *found_out = false;
        return MICRODB_OK;
    }

    return MICRODB_ERR_FULL;
}

static void microdb_kv_remove_slot(microdb_core_t *core, uint32_t idx) {
    microdb_kv_bucket_t *bucket = &core->kv.buckets[idx];

    if (bucket->state == MICRODB_KV_BUCKET_LIVE && core->kv.entry_count != 0u) {
        core->kv.entry_count--;
    }

    bucket->state = MICRODB_KV_BUCKET_TOMBSTONE;
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
    microdb_kv_remove_slot(core, best_idx);
    microdb_kv_maybe_compact(core);
    return MICRODB_OK;
}

microdb_err_t microdb_kv_init(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    size_t bucket_bytes;

    memset(&core->kv, 0, sizeof(core->kv));

#if !MICRODB_ENABLE_KV
    return MICRODB_OK;
#endif

    core->kv.bucket_count = microdb_kv_bucket_count();
    bucket_bytes = (size_t)core->kv.bucket_count * sizeof(microdb_kv_bucket_t);

    if (bucket_bytes >= core->kv_arena.capacity) {
        return MICRODB_ERR_NO_MEM;
    }

    core->kv.buckets = (microdb_kv_bucket_t *)core->kv_arena.base;
    memset(core->kv.buckets, 0, bucket_bytes);
    core->kv.value_store = core->kv_arena.base + bucket_bytes;
    core->kv.value_capacity = (uint32_t)(core->kv_arena.capacity - bucket_bytes);
    core->kv.access_clock = 1u;
    return MICRODB_OK;
}

size_t microdb_kv_live_bytes(const microdb_t *db) {
    const microdb_core_t *core = microdb_core_const(db);
    return microdb_kv_live_value_bytes(core) + ((size_t)core->kv.bucket_count * sizeof(microdb_kv_bucket_t));
}

#if MICRODB_ENABLE_KV
microdb_err_t microdb_kv_set_at(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    microdb_core_t *core;
    microdb_kv_bucket_t *bucket;
    uint32_t slot;
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

    err = microdb_kv_find_slot(core, key, &slot, &found);
    if (err != MICRODB_OK && err != MICRODB_ERR_FULL) {
        return err;
    }

    if (!found && core->kv.entry_count >= MICRODB_KV_MAX_KEYS) {
#if MICRODB_KV_OVERFLOW_POLICY == MICRODB_KV_POLICY_REJECT
        return MICRODB_ERR_FULL;
#else
        err = microdb_kv_evict_lru(core);
        if (err != MICRODB_OK) {
            return err;
        }
        err = microdb_kv_find_slot(core, key, &slot, &found);
        if (err != MICRODB_OK) {
            return err;
        }
#endif
    }

    bucket = &core->kv.buckets[slot];
    if (!found) {
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
    memcpy(bucket->key, key, strlen(key) + 1u);
    bucket->expires_at = expires_at;
    bucket->last_access = core->kv.access_clock++;
    core->live_bytes = microdb_kv_live_bytes(db);
    err = microdb_persist_kv_set(db, key, val, len, expires_at);
    if (err != MICRODB_OK) {
        return err;
    }
    return MICRODB_OK;
}

microdb_err_t microdb_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t ttl) {
    microdb_core_t *core;
    uint32_t expires_at = 0u;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

#if MICRODB_KV_ENABLE_TTL
    if (ttl != 0u) {
        expires_at = (uint32_t)(microdb_now(core) + ttl);
    }
#else
    (void)ttl;
#endif

    return microdb_kv_set_at(db, key, val, len, expires_at);
}

microdb_err_t microdb_kv_get(microdb_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len) {
    microdb_core_t *core;
    microdb_kv_bucket_t *bucket;
    uint32_t slot;
    bool found;
    microdb_err_t err;

    if (db == NULL || buf == NULL || !microdb_kv_key_valid(key)) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_kv_find_slot(core, key, &slot, &found);
    if (err != MICRODB_OK || !found) {
        return MICRODB_ERR_NOT_FOUND;
    }

    bucket = &core->kv.buckets[slot];
    if (microdb_kv_expired(core, bucket)) {
        microdb_kv_remove_slot(core, slot);
        microdb_kv_maybe_compact(core);
        core->live_bytes = microdb_kv_live_bytes(db);
        return MICRODB_ERR_EXPIRED;
    }

    if (out_len != NULL) {
        *out_len = bucket->val_len;
    }

    if (buf_len < bucket->val_len) {
        return MICRODB_ERR_OVERFLOW;
    }

    if (bucket->val_len != 0u) {
        memcpy(buf, &core->kv.value_store[bucket->val_offset], bucket->val_len);
    }

    bucket->last_access = core->kv.access_clock++;
    return MICRODB_OK;
}

microdb_err_t microdb_kv_del(microdb_t *db, const char *key) {
    microdb_core_t *core;
    uint32_t slot;
    bool found;
    microdb_err_t err;

    if (db == NULL || !microdb_kv_key_valid(key)) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_kv_find_slot(core, key, &slot, &found);
    if (err != MICRODB_OK || !found) {
        return MICRODB_ERR_NOT_FOUND;
    }

    microdb_kv_remove_slot(core, slot);
    microdb_kv_maybe_compact(core);
    core->live_bytes = microdb_kv_live_bytes(db);
    return microdb_persist_kv_del(db, key);
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

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_kv_find_slot(core, key, &slot, &found);
    if (err != MICRODB_OK || !found) {
        return MICRODB_ERR_NOT_FOUND;
    }

    bucket = &core->kv.buckets[slot];
    if (microdb_kv_expired(core, bucket)) {
        microdb_kv_remove_slot(core, slot);
        microdb_kv_maybe_compact(core);
        core->live_bytes = microdb_kv_live_bytes(db);
        return MICRODB_ERR_EXPIRED;
    }

    bucket->last_access = core->kv.access_clock++;
    return MICRODB_OK;
}

microdb_err_t microdb_kv_iter(microdb_t *db, microdb_kv_iter_cb_t cb, void *ctx) {
    microdb_core_t *core;
    uint32_t i;

    if (db == NULL || cb == NULL) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    for (i = 0; i < core->kv.bucket_count; ++i) {
        microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        uint32_t ttl_remaining = UINT32_MAX;

        if (bucket->state != MICRODB_KV_BUCKET_LIVE || microdb_kv_expired(core, bucket)) {
            continue;
        }

#if MICRODB_KV_ENABLE_TTL
        if (bucket->expires_at != 0u) {
            microdb_timestamp_t now = microdb_now(core);
            ttl_remaining = bucket->expires_at > now ? (uint32_t)(bucket->expires_at - now) : 0u;
        }
#endif

        if (!cb(bucket->key, &core->kv.value_store[bucket->val_offset], bucket->val_len, ttl_remaining, ctx)) {
            break;
        }
    }

    return MICRODB_OK;
}

microdb_err_t microdb_kv_purge_expired(microdb_t *db) {
    microdb_core_t *core;
    uint32_t i;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    for (i = 0; i < core->kv.bucket_count; ++i) {
        if (core->kv.buckets[i].state == MICRODB_KV_BUCKET_LIVE &&
            microdb_kv_expired(core, &core->kv.buckets[i])) {
            microdb_kv_remove_slot(core, i);
        }
    }

    microdb_kv_maybe_compact(core);
    core->live_bytes = microdb_kv_live_bytes(db);
    return MICRODB_OK;
}

microdb_err_t microdb_kv_clear(microdb_t *db) {
    microdb_core_t *core;
    size_t bucket_bytes;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    bucket_bytes = (size_t)core->kv.bucket_count * sizeof(microdb_kv_bucket_t);
    memset(core->kv.buckets, 0, bucket_bytes);
    core->kv.entry_count = 0u;
    core->kv.value_used = 0u;
    core->kv.access_clock = 1u;
    core->live_bytes = microdb_kv_live_bytes(db);
    return microdb_persist_kv_clear(db);
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
#endif
