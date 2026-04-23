// SPDX-License-Identifier: MIT
#include "lox_internal.h"
#include "lox_lock.h"

#include "lox_arena.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

LOX_STATIC_ASSERT(core_ram_pct_sum, (LOX_RAM_KV_PCT + LOX_RAM_TS_PCT + LOX_RAM_REL_PCT) == 100u);
LOX_STATIC_ASSERT(core_ram_kb_min, LOX_RAM_KB >= 8u);
LOX_STATIC_ASSERT(core_kv_max_keys_min, LOX_KV_MAX_KEYS >= 1u);
LOX_STATIC_ASSERT(core_kv_key_max_len_min, LOX_KV_KEY_MAX_LEN >= 4u);
LOX_STATIC_ASSERT(core_kv_val_max_len_min, LOX_KV_VAL_MAX_LEN >= 1u);
LOX_STATIC_ASSERT(core_ts_max_streams_min, LOX_TS_MAX_STREAMS >= 1u);
LOX_STATIC_ASSERT(core_rel_max_tables_min, LOX_REL_MAX_TABLES >= 1u);
LOX_STATIC_ASSERT(core_rel_max_cols_min, LOX_REL_MAX_COLS >= 1u);

static size_t lox_bytes_from_kb(uint32_t ram_kb) {
    return (size_t)ram_kb * 1024u;
}

static size_t lox_slice_bytes(size_t total, uint32_t pct) {
    return (total * (size_t)pct) / 100u;
}

static uint8_t *lox_align_ptr(uint8_t *ptr, size_t align) {
    uintptr_t p = (uintptr_t)ptr;
    uintptr_t a = (p + (uintptr_t)(align - 1u)) & ~((uintptr_t)align - 1u);
    return (uint8_t *)a;
}

static uint32_t lox_popcount8(uint8_t v) {
    uint32_t c = 0u;
    while (v != 0u) {
        c += (uint32_t)(v & 1u);
        v >>= 1u;
    }
    return c;
}

static void lox_selfcheck_set_first(lox_selfcheck_result_t *out, const char *msg) {
    if (out != NULL && out->first_anomaly[0] == '\0' && msg != NULL) {
        (void)snprintf(out->first_anomaly, sizeof(out->first_anomaly), "%s", msg);
    }
}

const char *lox_err_to_string(lox_err_t err) {
    switch (err) {
        case LOX_OK:
            return "LOX_OK";
        case LOX_ERR_INVALID:
            return "LOX_ERR_INVALID";
        case LOX_ERR_NO_MEM:
            return "LOX_ERR_NO_MEM";
        case LOX_ERR_FULL:
            return "LOX_ERR_FULL";
        case LOX_ERR_NOT_FOUND:
            return "LOX_ERR_NOT_FOUND";
        case LOX_ERR_EXPIRED:
            return "LOX_ERR_EXPIRED";
        case LOX_ERR_STORAGE:
            return "LOX_ERR_STORAGE";
        case LOX_ERR_CORRUPT:
            return "LOX_ERR_CORRUPT";
        case LOX_ERR_SEALED:
            return "LOX_ERR_SEALED";
        case LOX_ERR_EXISTS:
            return "LOX_ERR_EXISTS";
        case LOX_ERR_DISABLED:
            return "LOX_ERR_DISABLED";
        case LOX_ERR_OVERFLOW:
            return "LOX_ERR_OVERFLOW";
        case LOX_ERR_SCHEMA:
            return "LOX_ERR_SCHEMA";
        case LOX_ERR_TXN_ACTIVE:
            return "LOX_ERR_TXN_ACTIVE";
        case LOX_ERR_MODIFIED:
            return "LOX_ERR_MODIFIED";
        default:
            return "LOX_ERR_UNKNOWN";
    }
}

lox_core_t *lox_core(lox_t *db) {
    return (lox_core_t *)&db->_opaque[0];
}

const lox_core_t *lox_core_const(const lox_t *db) {
    return (const lox_core_t *)&db->_opaque[0];
}

static lox_err_t lox_validate_handle(const lox_t *db) {
    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    if (lox_core_const(db)->magic != LOX_MAGIC) {
        return LOX_ERR_INVALID;
    }

    return LOX_OK;
}

static uint8_t lox_fill_pct_u32(uint32_t used, uint32_t total) {
    if (total == 0u) {
        return 0u;
    }
    return (uint8_t)((used * 100u) / total);
}

static uint32_t lox_kv_tombstone_count(const lox_core_t *core) {
    uint32_t i;
    uint32_t tombstones = 0u;
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        if (core->kv.buckets[i].state == 2u) {
            tombstones++;
        }
    }
    return tombstones;
}

static uint32_t lox_kv_live_value_bytes_local(const lox_core_t *core) {
    return core->kv.live_value_bytes;
}

static const lox_kv_bucket_t *lox_kv_find_bucket_const(const lox_core_t *core, const char *key) {
    uint32_t i;
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        const lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == 1u && strncmp(bucket->key, key, LOX_KV_KEY_MAX_LEN) == 0) {
            return bucket;
        }
    }
    return NULL;
}

static const lox_ts_stream_t *lox_ts_find_const(const lox_core_t *core, const char *name) {
    uint32_t i;
    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        const lox_ts_stream_t *stream = &core->ts.streams[i];
        if (stream->registered && strcmp(stream->name, name) == 0) {
            return stream;
        }
    }
    return NULL;
}

static const lox_table_t *lox_rel_find_table_const(const lox_core_t *core, const char *name) {
    uint32_t i;
    for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
        const lox_table_t *table = &core->rel.tables[i];
        if (table->registered && strcmp(table->name, name) == 0) {
            return table;
        }
    }
    return NULL;
}

static uint32_t lox_wal_entry_size_for_payload(uint32_t payload_len) {
    return 16u + ((payload_len + 3u) & ~3u);
}

static void lox_fill_wal_admission(const lox_core_t *core,
                                       uint32_t required_wal_bytes,
                                       uint8_t *out_would_compact,
                                       uint32_t *out_wal_free) {
    uint32_t wal_free = 0u;
    uint8_t would_compact = 0u;
    if (core->wal_enabled && core->layout.wal_size > core->wal_used) {
        wal_free = core->layout.wal_size - core->wal_used;
        if (required_wal_bytes > wal_free) {
            would_compact = 1u;
        } else if (core->wal_compact_auto != 0u && core->layout.wal_size > 32u) {
            uint32_t threshold = (core->wal_compact_threshold_pct != 0u) ? core->wal_compact_threshold_pct : 75u;
            uint32_t total = core->layout.wal_size - 32u;
            uint32_t used_after = ((core->wal_used > 32u) ? (core->wal_used - 32u) : 0u) + required_wal_bytes;
            uint32_t fill = (total == 0u) ? 0u : ((used_after * 100u) / total);
            if (fill >= threshold) {
                would_compact = 1u;
            }
        }
    }
    *out_would_compact = would_compact;
    *out_wal_free = wal_free;
}

lox_err_t lox_init(lox_t *db, const lox_cfg_t *cfg) {
    lox_core_t *core;
    uint8_t *cursor;
    uint32_t ram_kb;
    uint8_t kv_pct;
    uint8_t ts_pct;
    uint8_t rel_pct;
    bool custom_split;
    size_t total_bytes;
    size_t kv_bytes;
    size_t ts_bytes;
    lox_err_t err;

    if (db == NULL || cfg == NULL) {
        return LOX_ERR_INVALID;
    }

    memset(db, 0, sizeof(*db));
    core = lox_core(db);

    ram_kb = cfg->ram_kb != 0u ? cfg->ram_kb : LOX_RAM_KB;
    custom_split = (cfg->kv_pct != 0u) || (cfg->ts_pct != 0u) || (cfg->rel_pct != 0u);
    if (custom_split) {
        if (cfg->kv_pct == 0u || cfg->ts_pct == 0u || cfg->rel_pct == 0u) {
            return LOX_ERR_INVALID;
        }
        kv_pct = cfg->kv_pct;
        ts_pct = cfg->ts_pct;
        rel_pct = cfg->rel_pct;
    } else {
        kv_pct = (uint8_t)LOX_RAM_KV_PCT;
        ts_pct = (uint8_t)LOX_RAM_TS_PCT;
        rel_pct = (uint8_t)LOX_RAM_REL_PCT;
    }
    if ((uint32_t)kv_pct + (uint32_t)ts_pct + (uint32_t)rel_pct != 100u) {
        return LOX_ERR_INVALID;
    }
    if (cfg->wal_compact_auto != 0u &&
        (cfg->wal_compact_threshold_pct == 0u || cfg->wal_compact_threshold_pct > 100u)) {
        return LOX_ERR_INVALID;
    }
    if (cfg->wal_sync_mode > LOX_WAL_SYNC_FLUSH_ONLY) {
        return LOX_ERR_INVALID;
    }
    total_bytes = lox_bytes_from_kb(ram_kb);

    core->heap = (uint8_t *)malloc(total_bytes);
    if (core->heap == NULL) {
        LOX_LOG("ERROR",
                    "malloc(%u) failed for RAM budget",
                    (unsigned)(ram_kb * 1024u));
        memset(db, 0, sizeof(*db));
        return LOX_ERR_NO_MEM;
    }

    memset(core->heap, 0, total_bytes);
    core->magic = LOX_MAGIC;
    core->heap_size = total_bytes;
    core->storage = cfg->storage;
    core->now = cfg->now;
    core->lock = cfg->lock;
    core->unlock = cfg->unlock;
    core->lock_destroy = cfg->lock_destroy;
    if (cfg->lock_create != NULL) {
        core->lock_handle = cfg->lock_create();
    }
    core->wal_compact_auto = cfg->wal_compact_auto;
    core->wal_compact_threshold_pct = cfg->wal_compact_threshold_pct;
    core->wal_sync_mode = cfg->wal_sync_mode;
    core->on_migrate = cfg->on_migrate;
    core->last_runtime_error = LOX_OK;
    core->last_recovery_status = LOX_OK;
    core->wal_enabled = (cfg->storage != NULL) && (LOX_ENABLE_WAL != 0);
    lox_arena_init(&core->arena, core->heap, total_bytes);

    kv_bytes = lox_slice_bytes(total_bytes, kv_pct);
    ts_bytes = lox_slice_bytes(total_bytes, ts_pct);

    cursor = core->heap;
    lox_arena_init(&core->kv_arena, cursor, kv_bytes);
    cursor += kv_bytes;
    {
        uint8_t *heap_end = core->heap + total_bytes;
        uint8_t *ts_base = lox_align_ptr(cursor, sizeof(uint32_t));
        uint8_t *ts_end;
        uint8_t *rel_base;

        if (ts_base > heap_end) {
            free(core->heap);
            memset(db, 0, sizeof(*db));
            return LOX_ERR_NO_MEM;
        }
        if ((size_t)(heap_end - ts_base) < ts_bytes) {
            free(core->heap);
            memset(db, 0, sizeof(*db));
            return LOX_ERR_NO_MEM;
        }
        ts_end = ts_base + ts_bytes;
        rel_base = lox_align_ptr(ts_end, sizeof(void *));
        if (rel_base > heap_end) {
            free(core->heap);
            memset(db, 0, sizeof(*db));
            return LOX_ERR_NO_MEM;
        }

        lox_arena_init(&core->ts_arena, ts_base, ts_bytes);
        lox_arena_init(&core->rel_arena, rel_base, (size_t)(heap_end - rel_base));
    }

    err = lox_kv_init(db);
    if (err != LOX_OK) {
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }

#if LOX_ENABLE_TS
    err = lox_ts_init(db);
    if (err != LOX_OK) {
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }
#endif

    err = lox_storage_bootstrap(db);
    if (err != LOX_OK) {
        core->last_runtime_error = err;
        if (err == LOX_ERR_STORAGE && cfg->storage != NULL) {
            LOX_LOG("ERROR",
                        "Storage capacity %u too small, need %u bytes",
                        (unsigned)cfg->storage->capacity,
                        (unsigned)core->layout.total_size);
        }
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }

    core->live_bytes = lox_kv_live_bytes(db);
    return LOX_OK;
}

lox_err_t lox_flush(lox_t *db) {
    lox_core_t *core;
    lox_err_t status;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);
    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        LOX_UNLOCK(db);
        return status;
    }

    core = lox_core(db);
    status = lox_storage_flush(db);
    lox_record_error(core, status);
    LOX_UNLOCK(db);
    return status;
}

lox_err_t lox_deinit(lox_t *db) {
    lox_core_t *core;
    uint8_t *heap;
    void (*lock_destroy)(void *hdl);
    void *lock_handle;
    lox_err_t status;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }
    status = lox_storage_flush(db);
    heap = core->heap;
    lock_destroy = core->lock_destroy;
    lock_handle = core->lock_handle;
    core->magic = 0u;
    LOX_UNLOCK(db);

    if (lock_destroy != NULL) {
        lock_destroy(lock_handle);
    }
    lox_record_error(core, status);
    free(heap);
    memset(db, 0, sizeof(*db));
    return status;
}

lox_err_t lox_stats(const lox_t *db, lox_stats_t *out) {
    return lox_inspect((lox_t *)db, out);
}

lox_err_t lox_inspect(lox_t *db, lox_stats_t *out) {
    const lox_core_t *core;
    uint32_t ts_capacity_total = 0u;
    uint32_t ts_samples_total = 0u;
    uint32_t rel_rows_total = 0u;
    uint32_t wal_bytes_used = 0u;
    uint32_t wal_bytes_total = 0u;
    uint32_t i;
    lox_err_t status;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));

    out->kv_entries_used = core->kv.entry_count;
    out->kv_entries_max = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;
    out->kv_fill_pct = lox_fill_pct_u32(out->kv_entries_used, out->kv_entries_max);
    out->kv_collision_count = core->kv.collision_count;
    out->kv_eviction_count = core->kv.eviction_count;

    out->ts_streams_registered = core->ts.registered_streams;
    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        ts_samples_total += core->ts.streams[i].count;
        ts_capacity_total += core->ts.streams[i].capacity;
    }
    out->ts_samples_total = ts_samples_total;
    out->ts_fill_pct = lox_fill_pct_u32(ts_samples_total, ts_capacity_total);

    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_bytes_total = core->layout.wal_size - 32u;
        wal_bytes_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    }
    out->wal_bytes_total = wal_bytes_total;
    out->wal_bytes_used = wal_bytes_used;
    out->wal_fill_pct = lox_fill_pct_u32(wal_bytes_used, wal_bytes_total);

    out->rel_tables_count = core->rel.registered_tables;
    for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
        if (core->rel.tables[i].registered) {
            rel_rows_total += core->rel.tables[i].live_count;
        }
    }
    out->rel_rows_total = rel_rows_total;

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_get_db_stats(lox_t *db, lox_db_stats_t *out) {
    const lox_core_t *core;
    uint32_t wal_bytes_used = 0u;
    uint32_t wal_bytes_total = 0u;
    lox_err_t status;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_bytes_total = core->layout.wal_size - 32u;
        wal_bytes_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    }
    out->effective_capacity_bytes = (core->storage != NULL) ? core->storage->capacity : 0u;
    out->wal_bytes_total = wal_bytes_total;
    out->wal_bytes_used = wal_bytes_used;
    out->wal_fill_pct = lox_fill_pct_u32(wal_bytes_used, wal_bytes_total);
    out->compact_count = core->compact_count;
    out->reopen_count = core->reopen_count;
    out->recovery_count = core->recovery_count;
    out->last_runtime_error = core->last_runtime_error;
    out->last_recovery_status = core->last_recovery_status;
    out->active_generation = core->layout.active_generation;
    out->active_bank = core->layout.active_bank;

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_get_kv_stats(lox_t *db, lox_kv_stats_t *out) {
    const lox_core_t *core;
    lox_err_t status;
    uint32_t entry_limit;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    entry_limit = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;
    memset(out, 0, sizeof(*out));
    out->live_keys = core->kv.entry_count;
    out->collisions = core->kv.collision_count;
    out->evictions = core->kv.eviction_count;
    out->tombstones = lox_kv_tombstone_count(core);
    out->value_bytes_used = core->kv.value_used;
    out->fill_pct = lox_fill_pct_u32(out->live_keys, entry_limit);

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_get_ts_stats(lox_t *db, lox_ts_stats_t *out) {
    const lox_core_t *core;
    lox_err_t status;
    uint32_t ts_capacity_total = 0u;
    uint32_t ts_samples_total = 0u;
    uint32_t i;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->stream_count = core->ts.registered_streams;
    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        ts_samples_total += core->ts.streams[i].count;
        ts_capacity_total += core->ts.streams[i].capacity;
    }
    out->retained_samples = ts_samples_total;
    out->dropped_samples = core->ts_dropped_samples;
    out->fill_pct = lox_fill_pct_u32(ts_samples_total, ts_capacity_total);

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_get_rel_stats(lox_t *db, lox_rel_stats_t *out) {
    const lox_core_t *core;
    lox_err_t status;
    uint32_t i;
    uint32_t rows_live = 0u;
    uint32_t rows_capacity = 0u;
    uint32_t indexed_tables = 0u;
    uint32_t index_entries = 0u;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->table_count = core->rel.registered_tables;
    for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
        const lox_table_t *table = &core->rel.tables[i];
        if (!table->registered) {
            continue;
        }
        rows_live += table->live_count;
        rows_capacity += table->max_rows;
        if (table->index_col != UINT32_MAX) {
            indexed_tables++;
            index_entries += table->index_count;
        }
    }
    out->rows_live = rows_live;
    out->rows_free = (rows_capacity > rows_live) ? (rows_capacity - rows_live) : 0u;
    out->indexed_tables = indexed_tables;
    out->index_entries = index_entries;

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_get_effective_capacity(lox_t *db, lox_effective_capacity_t *out) {
    const lox_core_t *core;
    lox_err_t status;
    uint32_t ts_retained = 0u;
    uint32_t ts_total = 0u;
    uint32_t i;
    uint32_t entry_limit;
    uint32_t kv_free_now;
    uint32_t wal_total = 0u;
    uint32_t wal_used = 0u;
    uint32_t wal_free = 0u;
    uint32_t threshold_pct = 0u;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    entry_limit = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;
    out->kv_entries_usable = entry_limit;
    out->kv_entries_free = (entry_limit > core->kv.entry_count) ? (entry_limit - core->kv.entry_count) : 0u;
    out->kv_value_bytes_usable = core->kv.value_capacity;
    kv_free_now = (core->kv.value_capacity > core->kv.value_used) ? (core->kv.value_capacity - core->kv.value_used) : 0u;
    out->kv_value_bytes_free_now = kv_free_now;

    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        ts_retained += core->ts.streams[i].count;
        ts_total += core->ts.streams[i].capacity;
    }
    out->ts_samples_usable = ts_total;
    out->ts_samples_retained = ts_retained;
    out->ts_samples_free = (ts_total > ts_retained) ? (ts_total - ts_retained) : 0u;

    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_total = core->layout.wal_size - 32u;
        wal_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
        wal_free = (wal_total > wal_used) ? (wal_total - wal_used) : 0u;
    }
    out->wal_budget_total = wal_total;
    out->wal_budget_used = wal_used;
    out->wal_budget_free = wal_free;
    threshold_pct = (core->wal_compact_threshold_pct != 0u) ? core->wal_compact_threshold_pct : 75u;
    out->compact_threshold_pct = threshold_pct;
    out->wal_safety_reserved = (wal_total * threshold_pct) / 100u;

    if (core->storage == NULL) {
        out->limiting_flags |= LOX_CAP_LIMIT_STORAGE_DISABLED;
    }
    if (out->kv_entries_free == 0u) {
        out->limiting_flags |= LOX_CAP_LIMIT_KV_ENTRIES;
    }
    if (out->kv_value_bytes_free_now == 0u) {
        out->limiting_flags |= LOX_CAP_LIMIT_KV_VALUE_BYTES;
    }
    if (out->ts_samples_free == 0u) {
        out->limiting_flags |= LOX_CAP_LIMIT_TS_SAMPLES;
    }
    if (out->wal_budget_total != 0u && out->wal_budget_free == 0u) {
        out->limiting_flags |= LOX_CAP_LIMIT_WAL_BUDGET;
    }

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_get_pressure(lox_t *db, lox_pressure_t *out) {
    const lox_core_t *core;
    lox_err_t status;
    uint32_t ts_total = 0u;
    uint32_t ts_retained = 0u;
    uint32_t rel_rows_live = 0u;
    uint32_t rel_rows_capacity = 0u;
    uint32_t wal_total = 0u;
    uint32_t wal_used = 0u;
    uint32_t threshold_pct = 0u;
    uint32_t i;
    uint32_t max_risk;

    if (out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->kv_fill_pct = lox_fill_pct_u32(core->kv.entry_count,
                                            (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS)
                                                ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS)
                                                : 0u);

    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        ts_total += core->ts.streams[i].capacity;
        ts_retained += core->ts.streams[i].count;
    }
    out->ts_fill_pct = lox_fill_pct_u32(ts_retained, ts_total);

    for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
        const lox_table_t *table = &core->rel.tables[i];
        if (!table->registered) {
            continue;
        }
        rel_rows_live += table->live_count;
        rel_rows_capacity += table->max_rows;
    }
    out->rel_fill_pct = lox_fill_pct_u32(rel_rows_live, rel_rows_capacity);

    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_total = core->layout.wal_size - 32u;
        wal_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    }
    out->wal_fill_pct = lox_fill_pct_u32(wal_used, wal_total);

    threshold_pct = (core->wal_compact_threshold_pct != 0u) ? core->wal_compact_threshold_pct : 75u;
    if (wal_total == 0u) {
        out->compact_pressure_pct = 0u;
    } else if (threshold_pct == 0u) {
        out->compact_pressure_pct = out->wal_fill_pct;
    } else {
        uint32_t pressure = (uint32_t)out->wal_fill_pct * 100u / threshold_pct;
        out->compact_pressure_pct = (uint8_t)((pressure > 100u) ? 100u : pressure);
    }

    out->risk_flags = LOX_CAP_LIMIT_NONE;
    if (core->storage == NULL) {
        out->risk_flags |= LOX_CAP_LIMIT_STORAGE_DISABLED;
    }
    if (out->kv_fill_pct >= 100u) {
        out->risk_flags |= LOX_CAP_LIMIT_KV_ENTRIES;
    }
    if (out->ts_fill_pct >= 100u) {
        out->risk_flags |= LOX_CAP_LIMIT_TS_SAMPLES;
    }
    if (out->wal_fill_pct >= 100u) {
        out->risk_flags |= LOX_CAP_LIMIT_WAL_BUDGET;
    }

    max_risk = out->kv_fill_pct;
    if (out->ts_fill_pct > max_risk) {
        max_risk = out->ts_fill_pct;
    }
    if (out->rel_fill_pct > max_risk) {
        max_risk = out->rel_fill_pct;
    }
    if (out->wal_fill_pct > max_risk) {
        max_risk = out->wal_fill_pct;
    }
    if (out->compact_pressure_pct > max_risk) {
        max_risk = out->compact_pressure_pct;
    }
    out->near_full_risk_pct = (uint8_t)max_risk;

    LOX_UNLOCK(db);
    return LOX_OK;
}

lox_err_t lox_selfcheck(lox_t *db, lox_selfcheck_result_t *out) {
    lox_core_t *core;
    lox_err_t status;
    uint32_t i;
    uint8_t wal_ok = 1u;

    if (db == NULL || out == NULL) {
        return LOX_ERR_INVALID;
    }

    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        return status;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->kv_ok = 1u;
    out->ts_ok = 1u;
    out->rel_ok = 1u;
    out->wal_ok = 1u;

    {
        uint32_t live_count = 0u;
        uint32_t live_value_bytes = 0u;

        for (i = 0u; i < core->kv.bucket_count; ++i) {
            const lox_kv_bucket_t *b = &core->kv.buckets[i];
            if (b->state != 1u) {
                continue;
            }
            live_count++;
            live_value_bytes += b->val_len;
            if (b->val_offset + b->val_len > core->kv.value_capacity) {
                out->kv_anomalies++;
                out->kv_ok = 0u;
                lox_selfcheck_set_first(out, "kv: value range out of capacity");
            }
        }
        if (live_count != core->kv.entry_count) {
            out->kv_anomalies++;
            out->kv_ok = 0u;
            lox_selfcheck_set_first(out, "kv: entry_count mismatch");
        }
        if (live_value_bytes != core->kv.live_value_bytes) {
            out->kv_anomalies++;
            out->kv_ok = 0u;
            lox_selfcheck_set_first(out, "kv: live_value_bytes mismatch");
        }
        for (i = 0u; i < core->kv.bucket_count; ++i) {
            const lox_kv_bucket_t *a = &core->kv.buckets[i];
            uint32_t j;
            if (a->state != 1u || a->val_len == 0u) {
                continue;
            }
            for (j = i + 1u; j < core->kv.bucket_count; ++j) {
                const lox_kv_bucket_t *b = &core->kv.buckets[j];
                uint32_t a0;
                uint32_t a1;
                uint32_t b0;
                uint32_t b1;
                if (b->state != 1u || b->val_len == 0u) {
                    continue;
                }
                a0 = a->val_offset;
                a1 = a->val_offset + a->val_len;
                b0 = b->val_offset;
                b1 = b->val_offset + b->val_len;
                if (a0 < b1 && b0 < a1) {
                    out->kv_anomalies++;
                    out->kv_ok = 0u;
                    lox_selfcheck_set_first(out, "kv: overlapping value ranges");
                }
            }
        }
    }

#if LOX_ENABLE_TS
    {
        uint32_t registered = 0u;
        for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
            const lox_ts_stream_t *s = &core->ts.streams[i];
            if (!s->registered) {
                continue;
            }
            registered++;
            if (s->count > s->capacity) {
                out->ts_anomalies++;
                out->ts_ok = 0u;
                lox_selfcheck_set_first(out, "ts: count > capacity");
            }
            if (s->count > 0u && s->head >= s->capacity) {
                out->ts_anomalies++;
                out->ts_ok = 0u;
                lox_selfcheck_set_first(out, "ts: head out of range");
            }
        }
        if (registered != core->ts.registered_streams) {
            out->ts_anomalies++;
            out->ts_ok = 0u;
            lox_selfcheck_set_first(out, "ts: registered stream count mismatch");
        }
    }
#endif

#if LOX_ENABLE_REL
    {
        uint32_t registered_tables = 0u;
        for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
            const lox_table_t *t = &core->rel.tables[i];
            uint32_t alive = 0u;
            uint32_t b;
            if (!t->registered) {
                continue;
            }
            registered_tables++;
            for (b = 0u; b < (t->max_rows + 7u) / 8u; ++b) {
                alive += lox_popcount8(t->alive_bitmap[b]);
            }
            if (alive != t->live_count) {
                out->rel_anomalies++;
                out->rel_ok = 0u;
                lox_selfcheck_set_first(out, "rel: live_count bitmap mismatch");
            }
            if (t->index_count > t->live_count) {
                out->rel_anomalies++;
                out->rel_ok = 0u;
                lox_selfcheck_set_first(out, "rel: index_count > live_count");
            }
            if (t->index_count > 1u && t->index != NULL && t->index_key_size > 0u) {
                uint32_t k;
                for (k = 1u; k < t->index_count; ++k) {
                    const uint8_t *prev = t->index[k - 1u].key_bytes;
                    const uint8_t *cur = t->index[k].key_bytes;
                    if (memcmp(prev, cur, t->index_key_size) > 0) {
                        out->rel_anomalies++;
                        out->rel_ok = 0u;
                        lox_selfcheck_set_first(out, "rel: index not sorted");
                        break;
                    }
                }
            }
        }
        if (registered_tables != core->rel.registered_tables) {
            out->rel_anomalies++;
            out->rel_ok = 0u;
            lox_selfcheck_set_first(out, "rel: registered table count mismatch");
        }
    }
#endif

    if (core->magic != LOX_MAGIC) {
        wal_ok = 0u;
        out->wal_ok = 0u;
        lox_selfcheck_set_first(out, "wal: invalid handle magic");
    }
    if (core->wal_enabled && core->wal_used > core->layout.wal_size) {
        wal_ok = 0u;
        out->wal_ok = 0u;
        lox_selfcheck_set_first(out, "wal: wal_used exceeds wal_size");
    }

    LOX_UNLOCK(db);

    if (out->kv_anomalies > 0u || out->ts_anomalies > 0u || out->rel_anomalies > 0u || wal_ok == 0u) {
        return LOX_ERR_CORRUPT;
    }
    return LOX_OK;
}

lox_err_t lox_admit_kv_set(lox_t *db, const char *key, size_t val_len, lox_admission_t *out) {
    if (out == NULL || key == NULL || key[0] == '\0') {
        return LOX_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));

#if !LOX_ENABLE_KV
    (void)db;
    (void)val_len;
    out->status = LOX_ERR_DISABLED;
    return LOX_ERR_DISABLED;
#else
    const lox_core_t *core;
    lox_err_t status;
    uint32_t required = 0u;
    uint32_t available = 0u;
    uint32_t compact_available = 0u;
    uint32_t entry_limit;
    uint8_t would_compact = 0u;
    uint32_t wal_free = 0u;
    uint32_t payload_len = 0u;
    uint32_t wal_bytes = 0u;
    const lox_kv_bucket_t *existing;
    if (val_len > LOX_KV_VAL_MAX_LEN || strlen(key) >= LOX_KV_KEY_MAX_LEN) {
        out->status = LOX_ERR_INVALID;
        return LOX_ERR_INVALID;
    }
    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        out->status = status;
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        out->status = LOX_ERR_INVALID;
        return LOX_ERR_INVALID;
    }

    existing = lox_kv_find_bucket_const(core, key);
    if (existing != NULL) {
        required = (val_len > existing->val_len) ? (uint32_t)(val_len - existing->val_len) : 0u;
    } else {
        required = (uint32_t)val_len;
        entry_limit = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;
        if (core->kv.entry_count >= entry_limit) {
#if LOX_KV_OVERFLOW_POLICY == LOX_KV_POLICY_REJECT
            out->status = LOX_ERR_FULL;
            out->deterministic_budget_ok = 0u;
            LOX_UNLOCK(db);
            return LOX_OK;
#else
            out->would_degrade = 1u;
            out->deterministic_budget_ok = 0u;
#endif
        }
    }

    available = (core->kv.value_capacity > core->kv.value_used) ? (core->kv.value_capacity - core->kv.value_used) : 0u;
    compact_available = core->kv.value_capacity - lox_kv_live_value_bytes_local(core);
    out->required_bytes = required;
    out->available_bytes = available;

    if (required > available) {
        if (required <= compact_available) {
            out->would_compact = 1u;
        } else {
            out->status = LOX_ERR_NO_MEM;
            out->deterministic_budget_ok = 0u;
            LOX_UNLOCK(db);
            return LOX_OK;
        }
    }

    if (core->wal_enabled) {
        payload_len = (uint32_t)(1u + strlen(key) + 4u + val_len + 4u);
        wal_bytes = lox_wal_entry_size_for_payload(payload_len);
        out->required_wal_bytes = wal_bytes;
        lox_fill_wal_admission(core, wal_bytes, &would_compact, &wal_free);
        out->wal_bytes_free = wal_free;
        if (would_compact != 0u) {
            out->would_compact = 1u;
        }
    }

    if (out->deterministic_budget_ok == 0u && out->would_degrade == 0u) {
        out->deterministic_budget_ok = 1u;
    }
    out->status = LOX_OK;
    LOX_UNLOCK(db);
    return LOX_OK;
#endif
}

lox_err_t lox_admit_ts_insert(lox_t *db, const char *stream_name, size_t sample_len, lox_admission_t *out) {
    if (out == NULL || stream_name == NULL || stream_name[0] == '\0') {
        return LOX_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));

#if !LOX_ENABLE_TS
    (void)db;
    (void)sample_len;
    out->status = LOX_ERR_DISABLED;
    return LOX_ERR_DISABLED;
#else
    const lox_core_t *core;
    const lox_ts_stream_t *stream;
    lox_err_t status;
    uint32_t expected_len = 0u;
    uint8_t would_compact = 0u;
    uint32_t wal_free = 0u;
    uint32_t wal_bytes = 0u;
    uint32_t payload_len = 0u;
    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        out->status = status;
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        out->status = LOX_ERR_INVALID;
        return LOX_ERR_INVALID;
    }

    stream = lox_ts_find_const(core, stream_name);
    if (stream == NULL) {
        out->status = LOX_ERR_NOT_FOUND;
        LOX_UNLOCK(db);
        return LOX_OK;
    }
    expected_len = (stream->type == LOX_TS_RAW) ? (uint32_t)stream->raw_size : 4u;
    if (sample_len != expected_len) {
        out->status = LOX_ERR_INVALID;
        LOX_UNLOCK(db);
        return LOX_OK;
    }

    out->required_bytes = 1u;
    out->available_bytes = (stream->capacity > stream->count) ? (stream->capacity - stream->count) : 0u;
    if (stream->count >= stream->capacity) {
#if LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_REJECT
        out->status = LOX_ERR_FULL;
        out->deterministic_budget_ok = 0u;
        LOX_UNLOCK(db);
        return LOX_OK;
#else
        out->would_degrade = 1u;
        out->deterministic_budget_ok = 0u;
#endif
    }

    if (core->wal_enabled) {
        payload_len = (uint32_t)(1u + strlen(stream_name) + 9u + sample_len);
        wal_bytes = lox_wal_entry_size_for_payload(payload_len);
        out->required_wal_bytes = wal_bytes;
        lox_fill_wal_admission(core, wal_bytes, &would_compact, &wal_free);
        out->wal_bytes_free = wal_free;
        if (would_compact != 0u) {
            out->would_compact = 1u;
        }
    }

    if (out->deterministic_budget_ok == 0u && out->would_degrade == 0u) {
        out->deterministic_budget_ok = 1u;
    }
    out->status = LOX_OK;
    LOX_UNLOCK(db);
    return LOX_OK;
#endif
}

lox_err_t lox_admit_rel_insert(lox_t *db, const char *table_name, size_t row_len, lox_admission_t *out) {
    if (out == NULL || table_name == NULL || table_name[0] == '\0') {
        return LOX_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));

#if !LOX_ENABLE_REL
    (void)db;
    (void)row_len;
    out->status = LOX_ERR_DISABLED;
    return LOX_ERR_DISABLED;
#else
    const lox_core_t *core;
    const lox_table_t *table;
    lox_err_t status;
    uint8_t would_compact = 0u;
    uint32_t wal_free = 0u;
    uint32_t wal_bytes = 0u;
    uint32_t payload_len = 0u;
    status = lox_validate_handle(db);
    if (status != LOX_OK) {
        out->status = status;
        return status;
    }

    LOX_LOCK(db);
    core = lox_core_const(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        out->status = LOX_ERR_INVALID;
        return LOX_ERR_INVALID;
    }

    table = lox_rel_find_table_const(core, table_name);
    if (table == NULL) {
        out->status = LOX_ERR_NOT_FOUND;
        LOX_UNLOCK(db);
        return LOX_OK;
    }
    if (row_len != table->row_size) {
        out->status = LOX_ERR_INVALID;
        LOX_UNLOCK(db);
        return LOX_OK;
    }

    out->required_bytes = 1u;
    out->available_bytes = (table->max_rows > table->live_count) ? (table->max_rows - table->live_count) : 0u;
    if (table->live_count >= table->max_rows) {
        out->status = LOX_ERR_FULL;
        out->deterministic_budget_ok = 0u;
        LOX_UNLOCK(db);
        return LOX_OK;
    }

    if (core->wal_enabled) {
        payload_len = (uint32_t)(1u + strlen(table_name) + 4u + row_len);
        wal_bytes = lox_wal_entry_size_for_payload(payload_len);
        out->required_wal_bytes = wal_bytes;
        lox_fill_wal_admission(core, wal_bytes, &would_compact, &wal_free);
        out->wal_bytes_free = wal_free;
        if (would_compact != 0u) {
            out->would_compact = 1u;
        }
    }

    if (out->would_compact != 0u || out->would_degrade != 0u) {
        out->deterministic_budget_ok = 0u;
    } else if (out->status == LOX_OK &&
               out->deterministic_budget_ok == 0u &&
               out->would_degrade == 0u) {
        out->deterministic_budget_ok = 1u;
    }
    out->status = LOX_OK;
    LOX_UNLOCK(db);
    return LOX_OK;
#endif
}
