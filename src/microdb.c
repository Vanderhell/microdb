#include "microdb_internal.h"

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
    size_t rel_bytes;
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
    core->wal_enabled = (cfg->storage != NULL) && (MICRODB_ENABLE_WAL != 0);
    microdb_arena_init(&core->arena, core->heap, total_bytes);

    kv_bytes = microdb_slice_bytes(total_bytes, kv_pct);
    ts_bytes = microdb_slice_bytes(total_bytes, ts_pct);
    rel_bytes = total_bytes - kv_bytes - ts_bytes;

    cursor = core->heap;
    microdb_arena_init(&core->kv_arena, cursor, kv_bytes);
    cursor += kv_bytes;
    microdb_arena_init(&core->ts_arena, cursor, ts_bytes);
    cursor += ts_bytes;
    microdb_arena_init(&core->rel_arena, cursor, rel_bytes);

    err = microdb_kv_init(db);
    if (err != MICRODB_OK) {
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }

    err = microdb_ts_init(db);
    if (err != MICRODB_OK) {
        free(core->heap);
        memset(db, 0, sizeof(*db));
        return err;
    }

    err = microdb_storage_bootstrap(db);
    if (err != MICRODB_OK) {
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
    microdb_err_t status;

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    return microdb_storage_flush(db);
}

microdb_err_t microdb_deinit(microdb_t *db) {
    microdb_core_t *core;
    microdb_err_t status;

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    core = microdb_core(db);
    status = microdb_flush(db);
    free(core->heap);
    memset(db, 0, sizeof(*db));
    return status;
}

microdb_err_t microdb_stats(const microdb_t *db, microdb_stats_t *out) {
    const microdb_core_t *core;
    microdb_err_t status;

    if (out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    status = microdb_validate_handle(db);
    if (status != MICRODB_OK) {
        return status;
    }

    core = microdb_core_const(db);
    memset(out, 0, sizeof(*out));
    out->ram_total_bytes = core->heap_size;
    out->ram_used_bytes = core->live_bytes;
    out->kv_entries = core->kv.entry_count;
    out->kv_capacity = (uint32_t)(core->kv_arena.capacity / sizeof(microdb_kv_bucket_t));
    out->ts_streams = core->ts.registered_streams;
    out->rel_tables = core->rel.registered_tables;
    out->storage_bytes_written = core->storage_bytes_written;
    return MICRODB_OK;
}
