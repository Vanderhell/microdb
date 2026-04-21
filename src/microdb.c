// SPDX-License-Identifier: MIT
#include "microdb_internal.h"
#include "microdb_lock.h"

#include "microdb_arena.h"

#include <stdlib.h>
#include <string.h>

MICRODB_STATIC_ASSERT(core_ram_pct_sum, (MICRODB_RAM_KV_PCT + MICRODB_RAM_TS_PCT + MICRODB_RAM_REL_PCT) == 100u);
MICRODB_STATIC_ASSERT(core_ram_kb_min, MICRODB_RAM_KB >= 8u);
MICRODB_STATIC_ASSERT(core_kv_max_keys_min, MICRODB_KV_MAX_KEYS >= 1u);
MICRODB_STATIC_ASSERT(core_kv_key_max_len_min, MICRODB_KV_KEY_MAX_LEN >= 4u);
MICRODB_STATIC_ASSERT(core_kv_val_max_len_min, MICRODB_KV_VAL_MAX_LEN >= 1u);
MICRODB_STATIC_ASSERT(core_ts_max_streams_min, MICRODB_TS_MAX_STREAMS >= 1u);
MICRODB_STATIC_ASSERT(core_rel_max_tables_min, MICRODB_REL_MAX_TABLES >= 1u);
MICRODB_STATIC_ASSERT(core_rel_max_cols_min, MICRODB_REL_MAX_COLS >= 1u);

static size_t microdb_bytes_from_kb(uint32_t ram_kb) {
    return (size_t)ram_kb * 1024u;
}

static size_t microdb_slice_bytes(size_t total, uint32_t pct) {
    return (total * (size_t)pct) / 100u;
}

static uint8_t *microdb_align_ptr(uint8_t *ptr, size_t align) {
    uintptr_t p = (uintptr_t)ptr;
    uintptr_t a = (p + (uintptr_t)(align - 1u)) & ~((uintptr_t)align - 1u);
    return (uint8_t *)a;
}

const char *microdb_err_to_string(microdb_err_t err) {
    switch (err) {
        case MICRODB_OK:
            return "MICRODB_OK";
        case MICRODB_ERR_INVALID:
            return "MICRODB_ERR_INVALID";
        case MICRODB_ERR_NO_MEM:
            return "MICRODB_ERR_NO_MEM";
        case MICRODB_ERR_FULL:
            return "MICRODB_ERR_FULL";
        case MICRODB_ERR_NOT_FOUND:
            return "MICRODB_ERR_NOT_FOUND";
        case MICRODB_ERR_EXPIRED:
            return "MICRODB_ERR_EXPIRED";
        case MICRODB_ERR_STORAGE:
            return "MICRODB_ERR_STORAGE";
        case MICRODB_ERR_CORRUPT:
            return "MICRODB_ERR_CORRUPT";
        case MICRODB_ERR_SEALED:
            return "MICRODB_ERR_SEALED";
        case MICRODB_ERR_EXISTS:
            return "MICRODB_ERR_EXISTS";
        case MICRODB_ERR_DISABLED:
            return "MICRODB_ERR_DISABLED";
        case MICRODB_ERR_OVERFLOW:
            return "MICRODB_ERR_OVERFLOW";
        case MICRODB_ERR_SCHEMA:
            return "MICRODB_ERR_SCHEMA";
        case MICRODB_ERR_TXN_ACTIVE:
            return "MICRODB_ERR_TXN_ACTIVE";
        case MICRODB_ERR_MODIFIED:
            return "MICRODB_ERR_MODIFIED";
        default:
            return "MICRODB_ERR_UNKNOWN";
    }
}

microdb_core_t *microdb_core(microdb_t *db) {
    return (microdb_core_t *)&db->_opaque[0];
}

const microdb_core_t *microdb_core_const(const microdb_t *db) {
    return (const microdb_core_t *)&db->_opaque[0];
}

static microdb_err_t microdb_validate_handle(const microdb_t *db) {
    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    if (microdb_core_const(db)->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    return MICRODB_OK;
}

static uint8_t microdb_fill_pct_u32(uint32_t used, uint32_t total) {
    if (total == 0u) {
        return 0u;
    }
    return (uint8_t)((used * 100u) / total);
}

static uint32_t microdb_kv_tombstone_count(const microdb_core_t *core) {
    uint32_t i;
    uint32_t tombstones = 0u;
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        if (core->kv.buckets[i].state == 2u) {
            tombstones++;
        }
    }
    return tombstones;
}

static uint32_t microdb_kv_live_value_bytes_local(const microdb_core_t *core) {
    uint32_t i;
    uint32_t total = 0u;
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        if (core->kv.buckets[i].state == 1u) {
            total += core->kv.buckets[i].val_len;
        }
    }
    return total;
}

static const microdb_kv_bucket_t *microdb_kv_find_bucket_const(const microdb_core_t *core, const char *key) {
    uint32_t i;
    for (i = 0u; i < core->kv.bucket_count; ++i) {
        const microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == 1u && strncmp(bucket->key, key, MICRODB_KV_KEY_MAX_LEN) == 0) {
            return bucket;
        }
    }
    return NULL;
}

static const microdb_ts_stream_t *microdb_ts_find_const(const microdb_core_t *core, const char *name) {
    uint32_t i;
    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
        const microdb_ts_stream_t *stream = &core->ts.streams[i];
        if (stream->registered && strcmp(stream->name, name) == 0) {
            return stream;
        }
    }
    return NULL;
}

static const microdb_table_t *microdb_rel_find_table_const(const microdb_core_t *core, const char *name) {
    uint32_t i;
    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        const microdb_table_t *table = &core->rel.tables[i];
        if (table->registered && strcmp(table->name, name) == 0) {
            return table;
        }
    }
    return NULL;
}

static uint32_t microdb_wal_entry_size_for_payload(uint32_t payload_len) {
    return 16u + ((payload_len + 3u) & ~3u);
}

static void microdb_fill_wal_admission(const microdb_core_t *core,
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

microdb_err_t microdb_init(microdb_t *db, const microdb_cfg_t *cfg) {
    microdb_core_t *core;
    uint8_t *cursor;
    uint32_t ram_kb;
    uint8_t kv_pct;
    uint8_t ts_pct;
    uint8_t rel_pct;
    bool custom_split;
    size_t total_bytes;
    size_t kv_bytes;
    size_t ts_bytes;
    microdb_err_t err;

    if (db == NULL || cfg == NULL) {
        return MICRODB_ERR_INVALID;
    }

    memset(db, 0, sizeof(*db));
    core = microdb_core(db);

    ram_kb = cfg->ram_kb != 0u ? cfg->ram_kb : MICRODB_RAM_KB;
    custom_split = (cfg->kv_pct != 0u) || (cfg->ts_pct != 0u) || (cfg->rel_pct != 0u);
    if (custom_split) {
        if (cfg->kv_pct == 0u || cfg->ts_pct == 0u || cfg->rel_pct == 0u) {
            return MICRODB_ERR_INVALID;
        }
        kv_pct = cfg->kv_pct;
        ts_pct = cfg->ts_pct;
        rel_pct = cfg->rel_pct;
    } else {
        kv_pct = (uint8_t)MICRODB_RAM_KV_PCT;
        ts_pct = (uint8_t)MICRODB_RAM_TS_PCT;
        rel_pct = (uint8_t)MICRODB_RAM_REL_PCT;
    }
    if ((uint32_t)kv_pct + (uint32_t)ts_pct + (uint32_t)rel_pct != 100u) {
        return MICRODB_ERR_INVALID;
    }
    if (cfg->wal_compact_auto != 0u &&
        (cfg->wal_compact_threshold_pct == 0u || cfg->wal_compact_threshold_pct > 100u)) {
        return MICRODB_ERR_INVALID;
    }
    if (cfg->wal_sync_mode > MICRODB_WAL_SYNC_FLUSH_ONLY) {
        return MICRODB_ERR_INVALID;
    }
    total_bytes = microdb_bytes_from_kb(ram_kb);

    core->heap = (uint8_t *)malloc(total_bytes);
    if (core->heap == NULL) {
        MICRODB_LOG("ERROR",
                    "malloc(%u) failed for RAM budget",
                    (unsigned)(ram_kb * 1024u));
        memset(db, 0, sizeof(*db));
        return MICRODB_ERR_NO_MEM;
    }

    memset(core->heap, 0, total_bytes);
    core->magic = MICRODB_MAGIC;
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
    core->last_runtime_error = MICRODB_OK;
    core->last_recovery_status = MICRODB_OK;
    core->wal_enabled = (cfg->storage != NULL) && (MICRODB_ENABLE_WAL != 0);
    microdb_arena_init(&core->arena, core->heap, total_bytes);

    kv_bytes = microdb_slice_bytes(total_bytes, kv_pct);
    ts_bytes = microdb_slice_bytes(total_bytes, ts_pct);

    cursor = core->heap;
    microdb_arena_init(&core->kv_arena, cursor, kv_bytes);
    cursor += kv_bytes;
    {
        uint8_t *heap_end = core->heap + total_bytes;
        uint8_t *ts_base = microdb_align_ptr(cursor, sizeof(uint32_t));
        uint8_t *ts_end;
        uint8_t *rel_base;

        if (ts_base > heap_end) {
            free(core->heap);
            memset(db, 0, sizeof(*db));
            return MICRODB_ERR_NO_MEM;
        }
        if ((size_t)(heap_end - ts_base) < ts_bytes) {
            free(core->heap);
            memset(db, 0, sizeof(*db));
            return MICRODB_ERR_NO_MEM;
        }
        ts_end = ts_base + ts_bytes;
        rel_base = microdb_align_ptr(ts_end, sizeof(void *));
        if (rel_base > heap_end) {
            free(core->heap);
            memset(db, 0, sizeof(*db));
            return MICRODB_ERR_NO_MEM;
        }

        microdb_arena_init(&core->ts_arena, ts_base, ts_bytes);
        microdb_arena_init(&core->rel_arena, rel_base, (size_t)(heap_end - rel_base));
    }

    err = microdb_kv_init(db);
    if (err != MICRODB_OK) {
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }

#if MICRODB_ENABLE_TS
    err = microdb_ts_init(db);
    if (err != MICRODB_OK) {
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }
#endif

    err = microdb_storage_bootstrap(db);
    if (err != MICRODB_OK) {
        core->last_runtime_error = err;
        if (err == MICRODB_ERR_STORAGE && cfg->storage != NULL) {
            MICRODB_LOG("ERROR",
                        "Storage capacity %u too small, need %u bytes",
                        (unsigned)cfg->storage->capacity,
                        (unsigned)core->layout.total_size);
        }
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }

    core->live_bytes = microdb_kv_live_bytes(db);
    return MICRODB_OK;
}

microdb_err_t microdb_flush(microdb_t *db) {
    microdb_core_t *core;
    microdb_err_t status;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }
    MICRODB_LOCK(db);
    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        MICRODB_UNLOCK(db);
        return status;
    }

    core = microdb_core(db);
    status = microdb_storage_flush(db);
    microdb_record_error(core, status);
    MICRODB_UNLOCK(db);
    return status;
}

microdb_err_t microdb_deinit(microdb_t *db) {
    microdb_core_t *core;
    uint8_t *heap;
    void (*lock_destroy)(void *hdl);
    void *lock_handle;
    microdb_err_t status;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }
    status = microdb_storage_flush(db);
    heap = core->heap;
    lock_destroy = core->lock_destroy;
    lock_handle = core->lock_handle;
    core->magic = 0u;
    MICRODB_UNLOCK(db);

    if (lock_destroy != NULL) {
        lock_destroy(lock_handle);
    }
    microdb_record_error(core, status);
    free(heap);
    memset(db, 0, sizeof(*db));
    return status;
}

microdb_err_t microdb_stats(const microdb_t *db, microdb_stats_t *out) {
    return microdb_inspect((microdb_t *)db, out);
}

microdb_err_t microdb_inspect(microdb_t *db, microdb_stats_t *out) {
    const microdb_core_t *core;
    uint32_t ts_capacity_total = 0u;
    uint32_t ts_samples_total = 0u;
    uint32_t rel_rows_total = 0u;
    uint32_t wal_bytes_used = 0u;
    uint32_t wal_bytes_total = 0u;
    uint32_t i;
    microdb_err_t status;

    if (out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));

    out->kv_entries_used = core->kv.entry_count;
    out->kv_entries_max = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    out->kv_fill_pct = microdb_fill_pct_u32(out->kv_entries_used, out->kv_entries_max);
    out->kv_collision_count = core->kv.collision_count;
    out->kv_eviction_count = core->kv.eviction_count;

    out->ts_streams_registered = core->ts.registered_streams;
    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
        ts_samples_total += core->ts.streams[i].count;
        ts_capacity_total += core->ts.streams[i].capacity;
    }
    out->ts_samples_total = ts_samples_total;
    out->ts_fill_pct = microdb_fill_pct_u32(ts_samples_total, ts_capacity_total);

    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_bytes_total = core->layout.wal_size - 32u;
        wal_bytes_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    }
    out->wal_bytes_total = wal_bytes_total;
    out->wal_bytes_used = wal_bytes_used;
    out->wal_fill_pct = microdb_fill_pct_u32(wal_bytes_used, wal_bytes_total);

    out->rel_tables_count = core->rel.registered_tables;
    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        if (core->rel.tables[i].registered) {
            rel_rows_total += core->rel.tables[i].live_count;
        }
    }
    out->rel_rows_total = rel_rows_total;

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_get_db_stats(microdb_t *db, microdb_db_stats_t *out) {
    const microdb_core_t *core;
    uint32_t wal_bytes_used = 0u;
    uint32_t wal_bytes_total = 0u;
    microdb_err_t status;

    if (out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_bytes_total = core->layout.wal_size - 32u;
        wal_bytes_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    }
    out->effective_capacity_bytes = (core->storage != NULL) ? core->storage->capacity : 0u;
    out->wal_bytes_total = wal_bytes_total;
    out->wal_bytes_used = wal_bytes_used;
    out->wal_fill_pct = microdb_fill_pct_u32(wal_bytes_used, wal_bytes_total);
    out->compact_count = core->compact_count;
    out->reopen_count = core->reopen_count;
    out->recovery_count = core->recovery_count;
    out->last_runtime_error = core->last_runtime_error;
    out->last_recovery_status = core->last_recovery_status;
    out->active_generation = core->layout.active_generation;
    out->active_bank = core->layout.active_bank;

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_get_kv_stats(microdb_t *db, microdb_kv_stats_t *out) {
    const microdb_core_t *core;
    microdb_err_t status;
    uint32_t entry_limit;

    if (out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    entry_limit = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    memset(out, 0, sizeof(*out));
    out->live_keys = core->kv.entry_count;
    out->collisions = core->kv.collision_count;
    out->evictions = core->kv.eviction_count;
    out->tombstones = microdb_kv_tombstone_count(core);
    out->value_bytes_used = core->kv.value_used;
    out->fill_pct = microdb_fill_pct_u32(out->live_keys, entry_limit);

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_get_ts_stats(microdb_t *db, microdb_ts_stats_t *out) {
    const microdb_core_t *core;
    microdb_err_t status;
    uint32_t ts_capacity_total = 0u;
    uint32_t ts_samples_total = 0u;
    uint32_t i;

    if (out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->stream_count = core->ts.registered_streams;
    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
        ts_samples_total += core->ts.streams[i].count;
        ts_capacity_total += core->ts.streams[i].capacity;
    }
    out->retained_samples = ts_samples_total;
    out->dropped_samples = core->ts_dropped_samples;
    out->fill_pct = microdb_fill_pct_u32(ts_samples_total, ts_capacity_total);

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_get_rel_stats(microdb_t *db, microdb_rel_stats_t *out) {
    const microdb_core_t *core;
    microdb_err_t status;
    uint32_t i;
    uint32_t rows_live = 0u;
    uint32_t rows_capacity = 0u;
    uint32_t indexed_tables = 0u;
    uint32_t index_entries = 0u;

    if (out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->table_count = core->rel.registered_tables;
    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        const microdb_table_t *table = &core->rel.tables[i];
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

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_get_effective_capacity(microdb_t *db, microdb_effective_capacity_t *out) {
    const microdb_core_t *core;
    microdb_err_t status;
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
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    entry_limit = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    out->kv_entries_usable = entry_limit;
    out->kv_entries_free = (entry_limit > core->kv.entry_count) ? (entry_limit - core->kv.entry_count) : 0u;
    out->kv_value_bytes_usable = core->kv.value_capacity;
    kv_free_now = (core->kv.value_capacity > core->kv.value_used) ? (core->kv.value_capacity - core->kv.value_used) : 0u;
    out->kv_value_bytes_free_now = kv_free_now;

    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
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
        out->limiting_flags |= MICRODB_CAP_LIMIT_STORAGE_DISABLED;
    }
    if (out->kv_entries_free == 0u) {
        out->limiting_flags |= MICRODB_CAP_LIMIT_KV_ENTRIES;
    }
    if (out->kv_value_bytes_free_now == 0u) {
        out->limiting_flags |= MICRODB_CAP_LIMIT_KV_VALUE_BYTES;
    }
    if (out->ts_samples_free == 0u) {
        out->limiting_flags |= MICRODB_CAP_LIMIT_TS_SAMPLES;
    }
    if (out->wal_budget_total != 0u && out->wal_budget_free == 0u) {
        out->limiting_flags |= MICRODB_CAP_LIMIT_WAL_BUDGET;
    }

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_get_pressure(microdb_t *db, microdb_pressure_t *out) {
    const microdb_core_t *core;
    microdb_err_t status;
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
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }

    memset(out, 0, sizeof(*out));
    out->kv_fill_pct = microdb_fill_pct_u32(core->kv.entry_count,
                                            (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS)
                                                ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS)
                                                : 0u);

    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
        ts_total += core->ts.streams[i].capacity;
        ts_retained += core->ts.streams[i].count;
    }
    out->ts_fill_pct = microdb_fill_pct_u32(ts_retained, ts_total);

    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        const microdb_table_t *table = &core->rel.tables[i];
        if (!table->registered) {
            continue;
        }
        rel_rows_live += table->live_count;
        rel_rows_capacity += table->max_rows;
    }
    out->rel_fill_pct = microdb_fill_pct_u32(rel_rows_live, rel_rows_capacity);

    if (core->wal_enabled && core->layout.wal_size > 32u) {
        wal_total = core->layout.wal_size - 32u;
        wal_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    }
    out->wal_fill_pct = microdb_fill_pct_u32(wal_used, wal_total);

    threshold_pct = (core->wal_compact_threshold_pct != 0u) ? core->wal_compact_threshold_pct : 75u;
    if (wal_total == 0u) {
        out->compact_pressure_pct = 0u;
    } else if (threshold_pct == 0u) {
        out->compact_pressure_pct = out->wal_fill_pct;
    } else {
        uint32_t pressure = (uint32_t)out->wal_fill_pct * 100u / threshold_pct;
        out->compact_pressure_pct = (uint8_t)((pressure > 100u) ? 100u : pressure);
    }

    out->risk_flags = MICRODB_CAP_LIMIT_NONE;
    if (core->storage == NULL) {
        out->risk_flags |= MICRODB_CAP_LIMIT_STORAGE_DISABLED;
    }
    if (out->kv_fill_pct >= 100u) {
        out->risk_flags |= MICRODB_CAP_LIMIT_KV_ENTRIES;
    }
    if (out->ts_fill_pct >= 100u) {
        out->risk_flags |= MICRODB_CAP_LIMIT_TS_SAMPLES;
    }
    if (out->wal_fill_pct >= 100u) {
        out->risk_flags |= MICRODB_CAP_LIMIT_WAL_BUDGET;
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

    MICRODB_UNLOCK(db);
    return MICRODB_OK;
}

microdb_err_t microdb_admit_kv_set(microdb_t *db, const char *key, size_t val_len, microdb_admission_t *out) {
    if (out == NULL || key == NULL || key[0] == '\0') {
        return MICRODB_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));

#if !MICRODB_ENABLE_KV
    (void)db;
    (void)val_len;
    out->status = MICRODB_ERR_DISABLED;
    return MICRODB_ERR_DISABLED;
#else
    const microdb_core_t *core;
    microdb_err_t status;
    uint32_t required = 0u;
    uint32_t available = 0u;
    uint32_t compact_available = 0u;
    uint32_t entry_limit;
    uint8_t would_compact = 0u;
    uint32_t wal_free = 0u;
    uint32_t payload_len = 0u;
    uint32_t wal_bytes = 0u;
    const microdb_kv_bucket_t *existing;
    if (val_len > MICRODB_KV_VAL_MAX_LEN || strlen(key) >= MICRODB_KV_KEY_MAX_LEN) {
        out->status = MICRODB_ERR_INVALID;
        return MICRODB_ERR_INVALID;
    }
    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        out->status = status;
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        out->status = MICRODB_ERR_INVALID;
        return MICRODB_ERR_INVALID;
    }

    existing = microdb_kv_find_bucket_const(core, key);
    if (existing != NULL) {
        required = (val_len > existing->val_len) ? (uint32_t)(val_len - existing->val_len) : 0u;
    } else {
        required = (uint32_t)val_len;
        entry_limit = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
        if (core->kv.entry_count >= entry_limit) {
#if MICRODB_KV_OVERFLOW_POLICY == MICRODB_KV_POLICY_REJECT
            out->status = MICRODB_ERR_FULL;
            out->deterministic_budget_ok = 0u;
            MICRODB_UNLOCK(db);
            return MICRODB_OK;
#else
            out->would_degrade = 1u;
            out->deterministic_budget_ok = 0u;
#endif
        }
    }

    available = (core->kv.value_capacity > core->kv.value_used) ? (core->kv.value_capacity - core->kv.value_used) : 0u;
    compact_available = core->kv.value_capacity - microdb_kv_live_value_bytes_local(core);
    out->required_bytes = required;
    out->available_bytes = available;

    if (required > available) {
        if (required <= compact_available) {
            out->would_compact = 1u;
        } else {
            out->status = MICRODB_ERR_NO_MEM;
            out->deterministic_budget_ok = 0u;
            MICRODB_UNLOCK(db);
            return MICRODB_OK;
        }
    }

    if (core->wal_enabled) {
        payload_len = (uint32_t)(1u + strlen(key) + 4u + val_len + 4u);
        wal_bytes = microdb_wal_entry_size_for_payload(payload_len);
        out->required_wal_bytes = wal_bytes;
        microdb_fill_wal_admission(core, wal_bytes, &would_compact, &wal_free);
        out->wal_bytes_free = wal_free;
        if (would_compact != 0u) {
            out->would_compact = 1u;
        }
    }

    if (out->deterministic_budget_ok == 0u && out->would_degrade == 0u) {
        out->deterministic_budget_ok = 1u;
    }
    out->status = MICRODB_OK;
    MICRODB_UNLOCK(db);
    return MICRODB_OK;
#endif
}

microdb_err_t microdb_admit_ts_insert(microdb_t *db, const char *stream_name, size_t sample_len, microdb_admission_t *out) {
    if (out == NULL || stream_name == NULL || stream_name[0] == '\0') {
        return MICRODB_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));

#if !MICRODB_ENABLE_TS
    (void)db;
    (void)sample_len;
    out->status = MICRODB_ERR_DISABLED;
    return MICRODB_ERR_DISABLED;
#else
    const microdb_core_t *core;
    const microdb_ts_stream_t *stream;
    microdb_err_t status;
    uint32_t expected_len = 0u;
    uint8_t would_compact = 0u;
    uint32_t wal_free = 0u;
    uint32_t wal_bytes = 0u;
    uint32_t payload_len = 0u;
    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        out->status = status;
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        out->status = MICRODB_ERR_INVALID;
        return MICRODB_ERR_INVALID;
    }

    stream = microdb_ts_find_const(core, stream_name);
    if (stream == NULL) {
        out->status = MICRODB_ERR_NOT_FOUND;
        MICRODB_UNLOCK(db);
        return MICRODB_OK;
    }
    expected_len = (stream->type == MICRODB_TS_RAW) ? (uint32_t)stream->raw_size : 4u;
    if (sample_len != expected_len) {
        out->status = MICRODB_ERR_INVALID;
        MICRODB_UNLOCK(db);
        return MICRODB_OK;
    }

    out->required_bytes = 1u;
    out->available_bytes = (stream->capacity > stream->count) ? (stream->capacity - stream->count) : 0u;
    if (stream->count >= stream->capacity) {
#if MICRODB_TS_OVERFLOW_POLICY == MICRODB_TS_POLICY_REJECT
        out->status = MICRODB_ERR_FULL;
        out->deterministic_budget_ok = 0u;
        MICRODB_UNLOCK(db);
        return MICRODB_OK;
#else
        out->would_degrade = 1u;
        out->deterministic_budget_ok = 0u;
#endif
    }

    if (core->wal_enabled) {
        payload_len = (uint32_t)(1u + strlen(stream_name) + 9u + sample_len);
        wal_bytes = microdb_wal_entry_size_for_payload(payload_len);
        out->required_wal_bytes = wal_bytes;
        microdb_fill_wal_admission(core, wal_bytes, &would_compact, &wal_free);
        out->wal_bytes_free = wal_free;
        if (would_compact != 0u) {
            out->would_compact = 1u;
        }
    }

    if (out->deterministic_budget_ok == 0u && out->would_degrade == 0u) {
        out->deterministic_budget_ok = 1u;
    }
    out->status = MICRODB_OK;
    MICRODB_UNLOCK(db);
    return MICRODB_OK;
#endif
}

microdb_err_t microdb_admit_rel_insert(microdb_t *db, const char *table_name, size_t row_len, microdb_admission_t *out) {
    if (out == NULL || table_name == NULL || table_name[0] == '\0') {
        return MICRODB_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));

#if !MICRODB_ENABLE_REL
    (void)db;
    (void)row_len;
    out->status = MICRODB_ERR_DISABLED;
    return MICRODB_ERR_DISABLED;
#else
    const microdb_core_t *core;
    const microdb_table_t *table;
    microdb_err_t status;
    uint8_t would_compact = 0u;
    uint32_t wal_free = 0u;
    uint32_t wal_bytes = 0u;
    uint32_t payload_len = 0u;
    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        out->status = status;
        return status;
    }

    MICRODB_LOCK(db);
    core = microdb_core_const(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        out->status = MICRODB_ERR_INVALID;
        return MICRODB_ERR_INVALID;
    }

    table = microdb_rel_find_table_const(core, table_name);
    if (table == NULL) {
        out->status = MICRODB_ERR_NOT_FOUND;
        MICRODB_UNLOCK(db);
        return MICRODB_OK;
    }
    if (row_len != table->row_size) {
        out->status = MICRODB_ERR_INVALID;
        MICRODB_UNLOCK(db);
        return MICRODB_OK;
    }

    out->required_bytes = 1u;
    out->available_bytes = (table->max_rows > table->live_count) ? (table->max_rows - table->live_count) : 0u;
    if (table->live_count >= table->max_rows) {
        out->status = MICRODB_ERR_FULL;
        out->deterministic_budget_ok = 0u;
        MICRODB_UNLOCK(db);
        return MICRODB_OK;
    }

    if (core->wal_enabled) {
        payload_len = (uint32_t)(1u + strlen(table_name) + 4u + row_len);
        wal_bytes = microdb_wal_entry_size_for_payload(payload_len);
        out->required_wal_bytes = wal_bytes;
        microdb_fill_wal_admission(core, wal_bytes, &would_compact, &wal_free);
        out->wal_bytes_free = wal_free;
        if (would_compact != 0u) {
            out->would_compact = 1u;
        }
    }

    if (out->would_compact != 0u || out->would_degrade != 0u) {
        out->deterministic_budget_ok = 0u;
    } else if (out->status == MICRODB_OK &&
               out->deterministic_budget_ok == 0u &&
               out->would_degrade == 0u) {
        out->deterministic_budget_ok = 1u;
    }
    out->status = MICRODB_OK;
    MICRODB_UNLOCK(db);
    return MICRODB_OK;
#endif
}
