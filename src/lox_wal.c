// SPDX-License-Identifier: MIT
#include "lox_internal.h"
#include "lox_crc.h"
#include "lox_arena.h"
#include "lox_lock.h"

#include <string.h>

#if !LOX_ENABLE_TS
static lox_err_t lox_ts_register_stub(lox_t *db, const char *name, lox_ts_type_t type, size_t raw_size) {
    (void)db;
    (void)name;
    (void)type;
    (void)raw_size;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_ts_insert_stub(lox_t *db, const char *name, lox_timestamp_t ts, const void *val) {
    (void)db;
    (void)name;
    (void)ts;
    (void)val;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_ts_clear_stub(lox_t *db, const char *name) {
    (void)db;
    (void)name;
    return LOX_ERR_DISABLED;
}
#define lox_ts_register lox_ts_register_stub
#define lox_ts_insert lox_ts_insert_stub
#define lox_ts_clear lox_ts_clear_stub
#endif

#if !LOX_ENABLE_REL
static lox_err_t lox_schema_init_stub(lox_schema_t *schema, const char *name, uint32_t max_rows) {
    (void)schema;
    (void)name;
    (void)max_rows;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_schema_add_stub(lox_schema_t *schema,
                                             const char *col_name,
                                             lox_col_type_t type,
                                             size_t size,
                                             bool is_index) {
    (void)schema;
    (void)col_name;
    (void)type;
    (void)size;
    (void)is_index;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_schema_seal_stub(lox_schema_t *schema) {
    (void)schema;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_table_create_stub(lox_t *db, lox_schema_t *schema) {
    (void)db;
    (void)schema;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_table_get_stub(lox_t *db, const char *name, lox_table_t **out_table) {
    (void)db;
    (void)name;
    (void)out_table;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_rel_insert_stub(lox_t *db, lox_table_t *table, const void *row_buf) {
    (void)db;
    (void)table;
    (void)row_buf;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_rel_delete_stub(lox_t *db, lox_table_t *table, const void *search_val, uint32_t *out_deleted) {
    (void)db;
    (void)table;
    (void)search_val;
    (void)out_deleted;
    return LOX_ERR_DISABLED;
}
static lox_err_t lox_rel_clear_stub(lox_t *db, lox_table_t *table) {
    (void)db;
    (void)table;
    return LOX_ERR_DISABLED;
}
#define lox_schema_init lox_schema_init_stub
#define lox_schema_add lox_schema_add_stub
#define lox_schema_seal lox_schema_seal_stub
#define lox_table_create lox_table_create_stub
#define lox_table_get lox_table_get_stub
#define lox_rel_insert lox_rel_insert_stub
#define lox_rel_delete lox_rel_delete_stub
#define lox_rel_clear lox_rel_clear_stub
#endif

enum {
    LOX_WAL_MAGIC = 0x4D44424Cu,
    LOX_WAL_VERSION = 0x00010000u,
    LOX_SNAPSHOT_FORMAT_VERSION = 0x00020000u,
    LOX_WAL_ENTRY_MAGIC = 0x454E5452u,
    LOX_KV_PAGE_MAGIC = 0x4B565047u,
    LOX_TS_PAGE_MAGIC = 0x54535047u,
    LOX_REL_PAGE_MAGIC = 0x524C5047u,
    LOX_SUPER_MAGIC = 0x53555052u,
    LOX_WAL_ENGINE_KV = 0u,
    LOX_WAL_ENGINE_TS = 1u,
    LOX_WAL_ENGINE_REL = 2u,
    LOX_WAL_ENGINE_TXN_KV = 3u,
    LOX_WAL_ENGINE_META = 0xFFu,
    LOX_WAL_OP_SET_INSERT = 0u,
    LOX_WAL_OP_DEL = 1u,
    LOX_WAL_OP_CLEAR = 2u,
    LOX_WAL_OP_TXN_COMMIT = 5u,
    LOX_WAL_OP_TS_REGISTER = 6u,
    LOX_WAL_OP_REL_TABLE_CREATE = 7u
};

#define LOX_WAL_HEADER_SIZE 32u
#define LOX_PAGE_HEADER_SIZE 32u
#define LOX_SUPERBLOCK_SIZE 32u

static uint32_t lox_align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static uint32_t lox_kv_snapshot_payload_max(const lox_core_t *core) {
    uint32_t max_entries;
    uint32_t max_key_len = (LOX_KV_KEY_MAX_LEN > 0u) ? (LOX_KV_KEY_MAX_LEN - 1u) : 0u;
    uint32_t per_entry = 1u + max_key_len + 4u + LOX_KV_VAL_MAX_LEN + 4u;
    (void)core;
    max_entries = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;
    return max_entries * per_entry;
}

static void lox_put_u32(uint8_t *dst, uint32_t value) {
    memcpy(dst, &value, sizeof(value));
}

static void lox_put_u16(uint8_t *dst, uint16_t value) {
    memcpy(dst, &value, sizeof(value));
}

static uint32_t lox_get_u32(const uint8_t *src) {
    uint32_t value = 0u;
    memcpy(&value, src, sizeof(value));
    return value;
}

static uint16_t lox_get_u16(const uint8_t *src) {
    uint16_t value = 0u;
    memcpy(&value, src, sizeof(value));
    return value;
}

static uint32_t lox_bank_kv_offset(const lox_core_t *core, uint32_t bank) {
    return ((bank == 0u) ? core->layout.bank_a_offset : core->layout.bank_b_offset);
}

static uint32_t lox_bank_ts_offset(const lox_core_t *core, uint32_t bank) {
    return lox_bank_kv_offset(core, bank) + core->layout.kv_size;
}

static uint32_t lox_bank_rel_offset(const lox_core_t *core, uint32_t bank) {
    return lox_bank_ts_offset(core, bank) + core->layout.ts_size;
}

static bool lox_storage_ready(const lox_core_t *core) {
    return core->storage != NULL && core->storage->read != NULL && core->storage->write != NULL &&
           core->storage->erase != NULL && core->storage->sync != NULL;
}

static lox_err_t lox_storage_read_bytes(lox_core_t *core, uint32_t offset, void *buf, size_t len) {
    lox_err_t err;
    LOX_IO_BEFORE_READ(offset, len);
    err = core->storage->read(core->storage->ctx, offset, buf, len);
    LOX_IO_AFTER_READ(offset, len, err);
    return err;
}

static lox_err_t lox_storage_write_bytes(lox_core_t *core, uint32_t offset, const void *buf, size_t len) {
    lox_err_t err;
    LOX_IO_BEFORE_WRITE(offset, len);
    err = core->storage->write(core->storage->ctx, offset, buf, len);
    LOX_IO_AFTER_WRITE(offset, len, err);
    if (err == LOX_OK) {
        core->storage_bytes_written += (uint32_t)len;
    }
    return err;
}

static lox_err_t lox_storage_erase_region(lox_core_t *core, uint32_t offset, uint32_t size) {
    uint32_t pos;

    for (pos = 0u; pos < size; pos += core->storage->erase_size) {
        LOX_IO_BEFORE_ERASE(offset + pos, core->storage->erase_size);
        lox_err_t err = core->storage->erase(core->storage->ctx, offset + pos);
        LOX_IO_AFTER_ERASE(offset + pos, core->storage->erase_size, err);
        if (err != LOX_OK) {
            return err;
        }
    }

    return LOX_OK;
}

static lox_err_t lox_storage_sync_core(lox_core_t *core) {
    lox_err_t err;
    LOX_IO_BEFORE_SYNC();
    err = core->storage->sync(core->storage->ctx);
    LOX_IO_AFTER_SYNC(err);
    return err;
}

static lox_err_t lox_write_wal_header(lox_core_t *core) {
    uint8_t header[LOX_WAL_HEADER_SIZE];
    uint32_t crc;

    memset(header, 0, sizeof(header));
    lox_put_u32(header + 0u, LOX_WAL_MAGIC);
    lox_put_u32(header + 4u, LOX_WAL_VERSION);
    lox_put_u32(header + 8u, core->wal_entry_count);
    lox_put_u32(header + 12u, core->wal_sequence);
    crc = LOX_CRC32(header, 16u);
    lox_put_u32(header + 16u, crc);
    return lox_storage_write_bytes(core, core->layout.wal_offset, header, LOX_WAL_HEADER_SIZE);
}

static lox_err_t lox_reset_wal(lox_core_t *core, uint32_t next_sequence) {
    lox_err_t err;

    core->wal_sequence = next_sequence;
    core->wal_entry_count = 0u;
    core->wal_used = LOX_WAL_HEADER_SIZE;

    err = lox_storage_erase_region(core, core->layout.wal_offset, core->layout.wal_size);
    if (err != LOX_OK) {
        return err;
    }

    err = lox_write_wal_header(core);
    if (err != LOX_OK) {
        return err;
    }

    return lox_storage_sync_core(core);
}

static lox_err_t lox_write_page_header(lox_core_t *core,
                                               uint32_t offset,
                                               uint32_t magic,
                                               uint32_t generation,
                                               uint32_t payload_length,
                                               uint32_t entry_count,
                                               uint32_t payload_crc) {
    uint8_t header[LOX_PAGE_HEADER_SIZE];
    uint32_t header_crc;

    memset(header, 0, sizeof(header));
    lox_put_u32(header + 0u, magic);
    lox_put_u32(header + 4u, LOX_SNAPSHOT_FORMAT_VERSION);
    lox_put_u32(header + 8u, generation);
    lox_put_u32(header + 12u, payload_length);
    lox_put_u32(header + 16u, entry_count);
    lox_put_u32(header + 20u, payload_crc);
    header_crc = LOX_CRC32(header, 24u);
    lox_put_u32(header + 24u, header_crc);
    return lox_storage_write_bytes(core, offset, header, sizeof(header));
}

static bool lox_validate_page_header(const uint8_t *header,
                                         uint32_t expected_magic,
                                         uint32_t max_payload_len,
                                         uint32_t *out_generation,
                                         uint32_t *out_payload_len,
                                         uint32_t *out_entry_count,
                                         uint32_t *out_payload_crc) {
    uint32_t header_crc = lox_get_u32(header + 24u);
    uint32_t payload_len = lox_get_u32(header + 12u);

    if (lox_get_u32(header + 0u) != expected_magic) {
        return false;
    }
    if (lox_get_u32(header + 4u) != LOX_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (LOX_CRC32(header, 24u) != header_crc) {
        return false;
    }
    if (payload_len > max_payload_len) {
        return false;
    }
    *out_generation = lox_get_u32(header + 8u);
    *out_payload_len = payload_len;
    *out_entry_count = lox_get_u32(header + 16u);
    *out_payload_crc = lox_get_u32(header + 20u);
    return true;
}

static bool lox_validate_superblock(const uint8_t *super,
                                        uint32_t *out_generation,
                                        uint32_t *out_active_bank) {
    uint32_t header_crc = lox_get_u32(super + 20u);
    if (lox_get_u32(super + 0u) != LOX_SUPER_MAGIC) {
        return false;
    }
    if (lox_get_u32(super + 4u) != LOX_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (LOX_CRC32(super, 20u) != header_crc) {
        return false;
    }
    if (lox_get_u32(super + 16u) > 1u) {
        return false;
    }
    *out_generation = lox_get_u32(super + 12u);
    *out_active_bank = lox_get_u32(super + 16u);
    return true;
}

static lox_err_t lox_write_superblock(lox_core_t *core, uint32_t generation, uint32_t active_bank) {
    uint8_t super[LOX_SUPERBLOCK_SIZE];
    uint32_t header_crc;
    uint32_t offset;

    memset(super, 0, sizeof(super));
    lox_put_u32(super + 0u, LOX_SUPER_MAGIC);
    lox_put_u32(super + 4u, LOX_SNAPSHOT_FORMAT_VERSION);
    lox_put_u32(super + 8u, LOX_WAL_VERSION);
    lox_put_u32(super + 12u, generation);
    lox_put_u32(super + 16u, active_bank);
    header_crc = LOX_CRC32(super, 20u);
    lox_put_u32(super + 20u, header_crc);

    offset = (generation & 1u) == 0u ? core->layout.super_b_offset : core->layout.super_a_offset;
    return lox_storage_write_bytes(core, offset, super, sizeof(super));
}

static lox_err_t lox_write_kv_page(lox_core_t *core, uint32_t bank, uint32_t generation) {
    uint32_t count = 0u;
    uint32_t page_offset = lox_bank_kv_offset(core, bank);
    uint32_t offset = page_offset + LOX_PAGE_HEADER_SIZE;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t max_end = page_offset + core->layout.kv_size;
    uint32_t i;
    lox_err_t err;

    for (i = 0u; i < core->kv.bucket_count; ++i) {
        const lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        uint8_t key_len;
        uint8_t header[4];

        if (bucket->state != 1u) {
            continue;
        }

        key_len = (uint8_t)strlen(bucket->key);
        if (offset + 1u + key_len + 4u + bucket->val_len + 4u > max_end) {
            return LOX_ERR_STORAGE;
        }

        err = lox_storage_write_bytes(core, offset, &key_len, 1u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, &key_len, 1u);
        offset += 1u;

        err = lox_storage_write_bytes(core, offset, bucket->key, key_len);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, bucket->key, key_len);
        offset += key_len;

        lox_put_u32(header, bucket->val_len);
        err = lox_storage_write_bytes(core, offset, header, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, header, 4u);
        offset += 4u;

        err = lox_storage_write_bytes(core, offset, &core->kv.value_store[bucket->val_offset], bucket->val_len);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, &core->kv.value_store[bucket->val_offset], bucket->val_len);
        offset += bucket->val_len;

        lox_put_u32(header, bucket->expires_at);
        err = lox_storage_write_bytes(core, offset, header, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, header, 4u);
        offset += 4u;
        count++;
    }

    return lox_write_page_header(core,
                                     page_offset,
                                     LOX_KV_PAGE_MAGIC,
                                     generation,
                                     offset - (page_offset + LOX_PAGE_HEADER_SIZE),
                                     count,
                                     crc);
}

#if LOX_ENABLE_TS
static uint32_t lox_ts_stream_val_size(const lox_ts_stream_t *stream) {
    return (stream->type == LOX_TS_RAW) ? (uint32_t)stream->raw_size : 4u;
}

static const uint8_t *lox_ts_sample_ptr_const(const lox_ts_stream_t *stream, uint32_t idx) {
    return stream->buf + (idx * stream->sample_stride);
}

static lox_err_t lox_write_ts_page(lox_core_t *core, uint32_t bank, uint32_t generation) {
    uint32_t stream_count = 0u;
    uint32_t page_offset = lox_bank_ts_offset(core, bank);
    uint32_t offset = page_offset + LOX_PAGE_HEADER_SIZE;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t max_end = page_offset + core->layout.ts_size;
    uint32_t i;
    lox_err_t err;

    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        const lox_ts_stream_t *stream = &core->ts.streams[i];
        uint8_t name_len;
        uint8_t one;
        uint8_t u32buf[4];
        uint32_t j;
        uint32_t idx;

        if (!stream->registered) {
            continue;
        }

        name_len = (uint8_t)strlen(stream->name);
        if (offset + 1u + name_len + 1u + 4u + 4u > max_end) {
            return LOX_ERR_STORAGE;
        }
        one = name_len;
        err = lox_storage_write_bytes(core, offset, &one, 1u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, &one, 1u);
        offset += 1u;

        err = lox_storage_write_bytes(core, offset, stream->name, name_len);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, stream->name, name_len);
        offset += name_len;

        one = (uint8_t)stream->type;
        err = lox_storage_write_bytes(core, offset, &one, 1u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, &one, 1u);
        offset += 1u;

        lox_put_u32(u32buf, (uint32_t)stream->raw_size);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        lox_put_u32(u32buf, stream->count);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        idx = stream->tail;
        for (j = 0u; j < stream->count; ++j) {
            const uint8_t *sample_ptr = lox_ts_sample_ptr_const(stream, idx);
            lox_timestamp_t sample_ts = 0;
            uint32_t val_len = lox_ts_stream_val_size(stream);
            uint64_t ts;
            uint32_t ts_low;
            uint32_t ts_high;

            memcpy(&sample_ts, sample_ptr, sizeof(sample_ts));
            ts = (uint64_t)sample_ts;
            ts_low = (uint32_t)(ts & 0xFFFFFFFFu);
            ts_high = (uint32_t)(ts >> 32u);

            if (offset + 8u + val_len > max_end) {
                return LOX_ERR_STORAGE;
            }
            lox_put_u32(u32buf, ts_low);
            err = lox_storage_write_bytes(core, offset, u32buf, 4u);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, u32buf, 4u);
            offset += 4u;

            lox_put_u32(u32buf, ts_high);
            err = lox_storage_write_bytes(core, offset, u32buf, 4u);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, u32buf, 4u);
            offset += 4u;

            err = lox_storage_write_bytes(core, offset, sample_ptr + sizeof(sample_ts), val_len);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, sample_ptr + sizeof(sample_ts), val_len);
            offset += val_len;
            idx = (idx + 1u) % stream->capacity;
        }

        stream_count++;
    }

    return lox_write_page_header(core,
                                     page_offset,
                                     LOX_TS_PAGE_MAGIC,
                                     generation,
                                     offset - (page_offset + LOX_PAGE_HEADER_SIZE),
                                     stream_count,
                                     crc);
}
#endif

#if LOX_ENABLE_REL
static lox_err_t lox_write_rel_page(lox_core_t *core, uint32_t bank, uint32_t generation) {
    uint32_t table_count = 0u;
    uint32_t page_offset = lox_bank_rel_offset(core, bank);
    uint32_t offset = page_offset + LOX_PAGE_HEADER_SIZE;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t max_end = page_offset + core->layout.rel_size;
    uint32_t i;
    lox_err_t err;

    for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
        const lox_table_t *table = &core->rel.tables[i];
        uint8_t name_len;
        uint8_t meta[2];
        uint8_t u16buf[2];
        uint8_t u32buf[4];
        uint32_t j;

        if (!table->registered) {
            continue;
        }

        name_len = (uint8_t)strlen(table->name);
        if (offset + 1u + name_len + 2u + 4u + 4u + 4u + 4u + 4u > max_end) {
            return LOX_ERR_STORAGE;
        }
        err = lox_storage_write_bytes(core, offset, &name_len, 1u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, &name_len, 1u);
        offset += 1u;

        err = lox_storage_write_bytes(core, offset, table->name, name_len);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, table->name, name_len);
        offset += name_len;

        lox_put_u16(u16buf, table->schema_version);
        err = lox_storage_write_bytes(core, offset, u16buf, 2u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u16buf, 2u);
        offset += 2u;

        lox_put_u32(u32buf, table->max_rows);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        lox_put_u32(u32buf, (uint32_t)table->row_size);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        lox_put_u32(u32buf, table->col_count);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        lox_put_u32(u32buf, table->index_col);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        lox_put_u32(u32buf, table->live_count);
        err = lox_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, u32buf, 4u);
        offset += 4u;

        for (j = 0u; j < table->col_count; ++j) {
            const lox_col_desc_t *col = &table->cols[j];
            uint8_t col_name_len = (uint8_t)strlen(col->name);

            if (offset + 1u + col_name_len + 2u + 4u > max_end) {
                return LOX_ERR_STORAGE;
            }
            err = lox_storage_write_bytes(core, offset, &col_name_len, 1u);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, &col_name_len, 1u);
            offset += 1u;

            err = lox_storage_write_bytes(core, offset, col->name, col_name_len);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, col->name, col_name_len);
            offset += col_name_len;

            meta[0] = (uint8_t)col->type;
            meta[1] = (uint8_t)(col->is_index ? 1u : 0u);
            err = lox_storage_write_bytes(core, offset, meta, 2u);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, meta, 2u);
            offset += 2u;

            lox_put_u32(u32buf, (uint32_t)col->size);
            err = lox_storage_write_bytes(core, offset, u32buf, 4u);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, u32buf, 4u);
            offset += 4u;
        }

        for (j = 0u; j < table->order_count; ++j) {
            uint32_t row_idx = table->order[j];
            if (((table->alive_bitmap[row_idx >> 3u] >> (row_idx & 7u)) & 1u) == 0u) {
                continue;
            }
            if (offset + (uint32_t)table->row_size > max_end) {
                return LOX_ERR_STORAGE;
            }
            err = lox_storage_write_bytes(core, offset, table->rows + ((size_t)row_idx * table->row_size), table->row_size);
            if (err != LOX_OK) {
                return err;
            }
            crc = lox_crc32(crc, table->rows + ((size_t)row_idx * table->row_size), table->row_size);
            offset += (uint32_t)table->row_size;
        }

        table_count++;
    }

    return lox_write_page_header(core,
                                     page_offset,
                                     LOX_REL_PAGE_MAGIC,
                                     generation,
                                     offset - (page_offset + LOX_PAGE_HEADER_SIZE),
                                     table_count,
                                     crc);
}
#endif

static lox_err_t lox_write_snapshot_bank(lox_core_t *core, uint32_t bank, uint32_t generation) {
    lox_err_t err;
    uint32_t bank_offset = (bank == 0u) ? core->layout.bank_a_offset : core->layout.bank_b_offset;

    err = lox_storage_erase_region(core, bank_offset, core->layout.bank_size);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_write_kv_page(core, bank, generation);
    if (err != LOX_OK) {
        return err;
    }
#if LOX_ENABLE_TS
    err = lox_write_ts_page(core, bank, generation);
    if (err != LOX_OK) {
        return err;
    }
#endif
#if LOX_ENABLE_REL
    return lox_write_rel_page(core, bank, generation);
#else
    return LOX_OK;
#endif
}

static lox_err_t lox_load_kv_page(lox_t *db, uint32_t bank, uint32_t expected_generation) {
    lox_core_t *core = lox_core(db);
    uint8_t header[LOX_PAGE_HEADER_SIZE];
    uint32_t page_offset = lox_bank_kv_offset(core, bank);
    uint32_t generation = 0u;
    uint32_t payload_len = 0u;
    uint32_t payload_crc = 0u;
    uint32_t offset;
    uint32_t payload_offset;
    uint32_t count;
    uint32_t payload_crc_calc = 0xFFFFFFFFu;
    uint32_t i;
    lox_err_t err;

    err = lox_storage_read_bytes(core, page_offset, header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }
    if (!lox_validate_page_header(header,
                                      LOX_KV_PAGE_MAGIC,
                                      core->layout.kv_size - LOX_PAGE_HEADER_SIZE,
                                      &generation,
                                      &payload_len,
                                      &count,
                                      &payload_crc)) {
        return LOX_ERR_CORRUPT;
    }
    if (generation != expected_generation) {
        return LOX_ERR_CORRUPT;
    }

    offset = page_offset + LOX_PAGE_HEADER_SIZE;
    payload_offset = offset;
    for (i = 0u; i < count; ++i) {
        uint8_t key_len = 0u;
        char key[LOX_KV_KEY_MAX_LEN];
        uint8_t u32buf[4];
        uint32_t val_len;
        uint32_t expires_at;
        uint8_t value[LOX_KV_VAL_MAX_LEN];

        if (offset + 1u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, &key_len, 1u);
        if (err != LOX_OK || key_len >= LOX_KV_KEY_MAX_LEN) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, &key_len, 1u);
        offset += 1u;

        if (offset + key_len > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, key, key_len);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, (const uint8_t *)key, key_len);
        key[key_len] = '\0';
        offset += key_len;

        if (offset + 4u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        val_len = lox_get_u32(u32buf);
        offset += 4u;
        if (val_len > LOX_KV_VAL_MAX_LEN) {
            return LOX_ERR_CORRUPT;
        }

        if (offset + val_len + 4u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, value, val_len);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, value, val_len);
        offset += val_len;

        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        expires_at = lox_get_u32(u32buf);
        offset += 4u;

        err = lox_kv_set_at(db, key, value, val_len, expires_at);
        if (err != LOX_OK) {
            return err;
        }
    }

    if (offset != payload_offset + payload_len) {
        return LOX_ERR_CORRUPT;
    }
    if (payload_crc_calc != payload_crc) {
        return LOX_ERR_CORRUPT;
    }

    return LOX_OK;
}

#if LOX_ENABLE_TS
static lox_err_t lox_load_ts_page(lox_t *db, uint32_t bank, uint32_t expected_generation) {
    lox_core_t *core = lox_core(db);
    uint8_t header[LOX_PAGE_HEADER_SIZE];
    uint32_t page_offset = lox_bank_ts_offset(core, bank);
    uint32_t generation = 0u;
    uint32_t payload_len = 0u;
    uint32_t payload_crc = 0u;
    uint32_t offset;
    uint32_t payload_offset;
    uint32_t stream_count;
    uint32_t payload_crc_calc = 0xFFFFFFFFu;
    uint32_t i;
    lox_err_t err;

    err = lox_storage_read_bytes(core, page_offset, header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }
    if (!lox_validate_page_header(header,
                                      LOX_TS_PAGE_MAGIC,
                                      core->layout.ts_size - LOX_PAGE_HEADER_SIZE,
                                      &generation,
                                      &payload_len,
                                      &stream_count,
                                      &payload_crc)) {
        return LOX_ERR_CORRUPT;
    }
    if (generation != expected_generation) {
        return LOX_ERR_CORRUPT;
    }

    offset = page_offset + LOX_PAGE_HEADER_SIZE;
    payload_offset = offset;
    for (i = 0u; i < stream_count; ++i) {
        uint8_t name_len = 0u;
        char name[LOX_TS_STREAM_NAME_LEN];
        uint8_t type_byte = 0u;
        uint8_t u32buf[4];
        uint32_t raw_size;
        uint32_t sample_count;
        uint32_t j;

        if (offset + 1u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, &name_len, 1u);
        if (err != LOX_OK || name_len >= LOX_TS_STREAM_NAME_LEN) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, &name_len, 1u);
        offset += 1u;

        if (offset + name_len + 1u + 8u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, name, name_len);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, (const uint8_t *)name, name_len);
        name[name_len] = '\0';
        offset += name_len;

        err = lox_storage_read_bytes(core, offset, &type_byte, 1u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, &type_byte, 1u);
        offset += 1u;

        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        raw_size = lox_get_u32(u32buf);
        offset += 4u;
        if ((lox_ts_type_t)type_byte == LOX_TS_RAW && (raw_size == 0u || raw_size > LOX_TS_RAW_MAX)) {
            return LOX_ERR_CORRUPT;
        }

        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        sample_count = lox_get_u32(u32buf);
        offset += 4u;

        err = lox_ts_register(db, name, (lox_ts_type_t)type_byte, raw_size);
        if (err != LOX_OK && err != LOX_ERR_EXISTS) {
            return err;
        }

        for (j = 0u; j < sample_count; ++j) {
            uint32_t ts_low;
            uint32_t ts_high;
            uint8_t value[LOX_TS_RAW_MAX];
            uint32_t val_len = ((lox_ts_type_t)type_byte == LOX_TS_RAW) ? raw_size : 4u;
            uint64_t full_ts;

            err = lox_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
            ts_low = lox_get_u32(u32buf);
            offset += 4u;

            err = lox_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
            ts_high = lox_get_u32(u32buf);
            offset += 4u;

            err = lox_storage_read_bytes(core, offset, value, val_len);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, value, val_len);
            offset += val_len;

            full_ts = ((uint64_t)ts_high << 32u) | ts_low;
            err = lox_ts_insert(db, name, (lox_timestamp_t)full_ts, value);
            if (err != LOX_OK) {
                return err;
            }
        }
    }

    if (offset != payload_offset + payload_len) {
        return LOX_ERR_CORRUPT;
    }
    if (payload_crc_calc != payload_crc) {
        return LOX_ERR_CORRUPT;
    }

    return LOX_OK;
}
#endif

#if LOX_ENABLE_REL
static lox_err_t lox_load_rel_page(lox_t *db, uint32_t bank, uint32_t expected_generation) {
    lox_core_t *core = lox_core(db);
    uint8_t header[LOX_PAGE_HEADER_SIZE];
    uint32_t page_offset = lox_bank_rel_offset(core, bank);
    uint32_t generation = 0u;
    uint32_t payload_len = 0u;
    uint32_t payload_crc = 0u;
    uint32_t offset;
    uint32_t payload_offset;
    uint32_t table_count;
    uint32_t payload_crc_calc = 0xFFFFFFFFu;
    uint32_t i;
    lox_err_t err;

    err = lox_storage_read_bytes(core, page_offset, header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }
    if (!lox_validate_page_header(header,
                                      LOX_REL_PAGE_MAGIC,
                                      core->layout.rel_size - LOX_PAGE_HEADER_SIZE,
                                      &generation,
                                      &payload_len,
                                      &table_count,
                                      &payload_crc)) {
        return LOX_ERR_CORRUPT;
    }
    if (generation != expected_generation) {
        return LOX_ERR_CORRUPT;
    }

    offset = page_offset + LOX_PAGE_HEADER_SIZE;
    payload_offset = offset;
    for (i = 0u; i < table_count; ++i) {
        lox_schema_t schema;
        lox_table_t *table = NULL;
        uint8_t name_len = 0u;
        char table_name[LOX_REL_TABLE_NAME_LEN];
        uint8_t u16buf[2];
        uint8_t u32buf[4];
        uint16_t schema_version;
        uint32_t max_rows;
        uint32_t col_count;
        uint32_t row_count;
        uint32_t j;

        if (offset + 1u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, &name_len, 1u);
        if (err != LOX_OK || name_len >= LOX_REL_TABLE_NAME_LEN) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, &name_len, 1u);
        offset += 1u;

        if (offset + name_len + 2u + 4u + 4u + 4u + 4u + 4u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, table_name, name_len);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, (const uint8_t *)table_name, name_len);
        table_name[name_len] = '\0';
        offset += name_len;

        err = lox_storage_read_bytes(core, offset, u16buf, 2u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u16buf, 2u);
        schema_version = lox_get_u16(u16buf);
        offset += 2u;

        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        max_rows = lox_get_u32(u32buf);
        offset += 4u;

        if (offset + 4u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        offset += 4u;

        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        col_count = lox_get_u32(u32buf);
        offset += 4u;
        if (col_count > LOX_REL_MAX_COLS) {
            return LOX_ERR_CORRUPT;
        }

        if (offset + 4u > payload_offset + payload_len) {
            return LOX_ERR_CORRUPT;
        }
        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        offset += 4u;

        err = lox_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != LOX_OK) {
            return LOX_ERR_CORRUPT;
        }
        payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
        row_count = lox_get_u32(u32buf);
        offset += 4u;
        if (row_count > max_rows) {
            return LOX_ERR_CORRUPT;
        }

        err = lox_schema_init(&schema, table_name, max_rows);
        if (err != LOX_OK) {
            return err;
        }
        schema.schema_version = schema_version;

        for (j = 0u; j < col_count; ++j) {
            uint8_t col_name_len = 0u;
            char col_name[LOX_REL_COL_NAME_LEN];
            uint8_t meta[2];
            uint32_t col_size;

            err = lox_storage_read_bytes(core, offset, &col_name_len, 1u);
            if (err != LOX_OK || col_name_len >= LOX_REL_COL_NAME_LEN) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, &col_name_len, 1u);
            offset += 1u;

            err = lox_storage_read_bytes(core, offset, col_name, col_name_len);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, (const uint8_t *)col_name, col_name_len);
            col_name[col_name_len] = '\0';
            offset += col_name_len;

            err = lox_storage_read_bytes(core, offset, meta, 2u);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, meta, 2u);
            offset += 2u;

            err = lox_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, u32buf, 4u);
            col_size = lox_get_u32(u32buf);
            offset += 4u;

            err = lox_schema_add(&schema,
                                     col_name,
                                     (lox_col_type_t)meta[0],
                                     col_size,
                                     meta[1] != 0u);
            if (err != LOX_OK) {
                return err;
            }
        }

        err = lox_schema_seal(&schema);
        if (err != LOX_OK) {
            return err;
        }
        err = lox_table_create(db, &schema);
        if (err != LOX_OK) {
            return err;
        }
        err = lox_table_get(db, table_name, &table);
        if (err != LOX_OK) {
            return err;
        }

        for (j = 0u; j < row_count; ++j) {
            uint8_t row_buf[1024];
            if (table->row_size > sizeof(row_buf)) {
                return LOX_ERR_SCHEMA;
            }
            err = lox_storage_read_bytes(core, offset, row_buf, table->row_size);
            if (err != LOX_OK) {
                return LOX_ERR_CORRUPT;
            }
            payload_crc_calc = lox_crc32(payload_crc_calc, row_buf, table->row_size);
            offset += (uint32_t)table->row_size;
            err = lox_rel_insert(db, table, row_buf);
            if (err != LOX_OK) {
                return err;
            }
        }
    }

    if (offset != payload_offset + payload_len) {
        return LOX_ERR_CORRUPT;
    }
    if (payload_crc_calc != payload_crc) {
        return LOX_ERR_CORRUPT;
    }

    return LOX_OK;
}
#endif

static lox_err_t lox_apply_wal_entry(lox_t *db,
                                             uint8_t engine,
                                             uint8_t op,
                                             const uint8_t *data,
                                             uint16_t data_len) {
    if (engine == LOX_WAL_ENGINE_KV) {
        if (op == LOX_WAL_OP_SET_INSERT) {
            uint8_t key_len;
            char key[LOX_KV_KEY_MAX_LEN];
            uint32_t val_len;
            uint32_t expires_at;
            const uint8_t *val;

            if (data_len < 1u) {
                return LOX_OK;
            }
            key_len = data[0];
            if ((uint32_t)key_len >= LOX_KV_KEY_MAX_LEN || data_len < (uint16_t)(1u + key_len + 8u)) {
                return LOX_OK;
            }
            memcpy(key, data + 1u, key_len);
            key[key_len] = '\0';
            val_len = lox_get_u32(data + 1u + key_len);
            if (val_len > LOX_KV_VAL_MAX_LEN ||
                data_len < (uint16_t)(1u + key_len + 4u + val_len + 4u)) {
                return LOX_OK;
            }
            val = data + 1u + key_len + 4u;
            expires_at = lox_get_u32(val + val_len);
            return lox_kv_set_at(db, key, val, val_len, expires_at);
        }
        if (op == LOX_WAL_OP_DEL) {
            uint8_t key_len;
            char key[LOX_KV_KEY_MAX_LEN];

            if (data_len < 1u) {
                return LOX_OK;
            }
            key_len = data[0];
            if ((uint32_t)key_len >= LOX_KV_KEY_MAX_LEN || data_len < (uint16_t)(1u + key_len)) {
                return LOX_OK;
            }
            memcpy(key, data + 1u, key_len);
            key[key_len] = '\0';
            (void)lox_kv_del(db, key);
            return LOX_OK;
        }
        if (op == LOX_WAL_OP_CLEAR) {
            return lox_kv_clear(db);
        }
    } else if (engine == LOX_WAL_ENGINE_TS) {
        if (op == LOX_WAL_OP_SET_INSERT) {
            uint8_t name_len;
            char name[LOX_TS_STREAM_NAME_LEN];
            uint32_t ts_low;
            uint32_t ts_high;
            uint8_t type_byte;
            uint32_t value_len;
            uint64_t ts;
            lox_err_t err;

            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_TS_STREAM_NAME_LEN || data_len < (uint16_t)(1u + name_len + 9u)) {
                return LOX_OK;
            }
            memcpy(name, data + 1u, name_len);
            name[name_len] = '\0';
            ts_low = lox_get_u32(data + 1u + name_len);
            ts_high = lox_get_u32(data + 1u + name_len + 4u);
            type_byte = data[1u + name_len + 8u];
            value_len = (uint32_t)data_len - (1u + name_len + 9u);
            err = lox_ts_register(db, name, (lox_ts_type_t)type_byte, value_len);
            if (err != LOX_OK && err != LOX_ERR_EXISTS) {
                return err;
            }
            ts = ((uint64_t)ts_high << 32u) | ts_low;
            return lox_ts_insert(db, name, (lox_timestamp_t)ts, data + 1u + name_len + 9u);
        }
        if (op == LOX_WAL_OP_TS_REGISTER) {
            uint8_t name_len;
            char name[LOX_TS_STREAM_NAME_LEN];
            uint8_t type_byte;
            uint32_t raw_size;
            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_TS_STREAM_NAME_LEN || data_len < (uint16_t)(1u + name_len + 5u)) {
                return LOX_OK;
            }
            memcpy(name, data + 1u, name_len);
            name[name_len] = '\0';
            type_byte = data[1u + name_len];
            raw_size = lox_get_u32(data + 1u + name_len + 1u);
            return lox_ts_register(db, name, (lox_ts_type_t)type_byte, raw_size);
        }
        if (op == LOX_WAL_OP_CLEAR) {
            uint8_t name_len;
            char name[LOX_TS_STREAM_NAME_LEN];
            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_TS_STREAM_NAME_LEN || data_len < (uint16_t)(1u + name_len)) {
                return LOX_OK;
            }
            memcpy(name, data + 1u, name_len);
            name[name_len] = '\0';
            return lox_ts_clear(db, name);
        }
    } else if (engine == LOX_WAL_ENGINE_REL) {
        if (op == LOX_WAL_OP_SET_INSERT) {
            uint8_t name_len;
            char table_name[LOX_REL_TABLE_NAME_LEN];
            uint32_t row_size;
            lox_table_t *table = NULL;

            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len + 4u)) {
                return LOX_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            row_size = lox_get_u32(data + 1u + name_len);
            if (data_len < (uint16_t)(1u + name_len + 4u + row_size)) {
                return LOX_OK;
            }
            if (lox_table_get(db, table_name, &table) != LOX_OK || table->row_size != row_size) {
                return LOX_OK;
            }
            return lox_rel_insert(db, table, data + 1u + name_len + 4u);
        }
        if (op == LOX_WAL_OP_DEL) {
            uint8_t name_len;
            char table_name[LOX_REL_TABLE_NAME_LEN];
            lox_table_t *table = NULL;

            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len)) {
                return LOX_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            if (lox_table_get(db, table_name, &table) != LOX_OK || table->index_key_size == 0u) {
                return LOX_OK;
            }
            (void)lox_rel_delete(db, table, data + 1u + name_len, NULL);
            return LOX_OK;
        }
        if (op == LOX_WAL_OP_CLEAR) {
            uint8_t name_len;
            char table_name[LOX_REL_TABLE_NAME_LEN];
            lox_table_t *table = NULL;

            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len)) {
                return LOX_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            if (lox_table_get(db, table_name, &table) != LOX_OK) {
                return LOX_OK;
            }
            return lox_rel_clear(db, table);
        }
        if (op == LOX_WAL_OP_REL_TABLE_CREATE) {
            lox_schema_t schema;
            uint8_t name_len;
            char table_name[LOX_REL_TABLE_NAME_LEN];
            uint16_t schema_version;
            uint32_t max_rows;
            uint32_t col_count;
            uint16_t off;
            uint32_t c;

            if (data_len < 1u) {
                return LOX_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= LOX_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len + 10u)) {
                return LOX_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            schema_version = lox_get_u16(data + 1u + name_len);
            max_rows = lox_get_u32(data + 1u + name_len + 2u);
            col_count = lox_get_u32(data + 1u + name_len + 6u);
            off = (uint16_t)(1u + name_len + 10u);

            if (lox_schema_init(&schema, table_name, max_rows) != LOX_OK) {
                return LOX_ERR_SCHEMA;
            }
            schema.schema_version = schema_version;
            for (c = 0u; c < col_count; ++c) {
                uint8_t col_name_len;
                char col_name[LOX_REL_COL_NAME_LEN];
                lox_col_type_t type;
                bool is_index;
                uint32_t col_size;
                if (off + 1u > data_len) {
                    return LOX_OK;
                }
                col_name_len = data[off++];
                if ((uint32_t)col_name_len >= LOX_REL_COL_NAME_LEN || off + col_name_len + 6u > data_len) {
                    return LOX_OK;
                }
                memcpy(col_name, data + off, col_name_len);
                col_name[col_name_len] = '\0';
                off = (uint16_t)(off + col_name_len);
                type = (lox_col_type_t)data[off++];
                is_index = data[off++] != 0u;
                col_size = lox_get_u32(data + off);
                off = (uint16_t)(off + 4u);
                if (lox_schema_add(&schema, col_name, type, col_size, is_index) != LOX_OK) {
                    return LOX_ERR_SCHEMA;
                }
            }
            if (lox_schema_seal(&schema) != LOX_OK) {
                return LOX_ERR_SCHEMA;
            }
            return lox_table_create(db, &schema);
        }
    }

    return LOX_OK;
}

static lox_err_t lox_replay_wal(lox_t *db, bool *out_had_entries, bool *out_header_reset) {
    lox_core_t *core = lox_core(db);
    uint8_t header[32];
    uint32_t stored_crc;
    uint32_t entry_count;
    uint32_t block_seq;
    uint32_t offset = core->layout.wal_offset + LOX_WAL_HEADER_SIZE;
    uint32_t i;
    uint32_t replayed_count = 0u;
    uint8_t txn_ops[LOX_TXN_STAGE_KEYS];
    uint16_t txn_lens[LOX_TXN_STAGE_KEYS];
    uint8_t txn_payloads[LOX_TXN_STAGE_KEYS][256];
    uint32_t txn_count = 0u;
    lox_err_t err;

    *out_had_entries = false;
    *out_header_reset = false;

    err = lox_storage_read_bytes(core, core->layout.wal_offset, header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }

    if (lox_get_u32(header + 0u) != LOX_WAL_MAGIC) {
        LOX_LOG("ERROR", "%s", "WAL header corrupt: resetting WAL");
        *out_header_reset = true;
        return LOX_OK;
    }

    stored_crc = lox_get_u32(header + 16u);
    if (LOX_CRC32(header, 16u) != stored_crc) {
        LOX_LOG("ERROR", "%s", "WAL header corrupt: resetting WAL");
        *out_header_reset = true;
        return LOX_OK;
    }

    entry_count = lox_get_u32(header + 8u);
    block_seq = lox_get_u32(header + 12u);
    core->wal_sequence = block_seq;

    if (entry_count == 0u) {
        core->wal_used = LOX_WAL_HEADER_SIZE;
        return LOX_OK;
    }

    *out_had_entries = true;
    /* Recovery invariant:
     * - TXN_KV WAL entries are staged only during replay.
     * - Staged txn entries become visible only after durable TXN_COMMIT marker.
     * - Corrupt/truncated WAL tail is ignored from first invalid entry onward.
     */
    core->wal_replaying = true;
    for (i = 0u; i < entry_count; ++i) {
        uint8_t entry_header[16];
        uint8_t payload[1536];
        uint32_t entry_crc;
        uint16_t data_len;
        uint32_t aligned_len;
        uint32_t crc;

        err = lox_storage_read_bytes(core, offset, entry_header, sizeof(entry_header));
        if (err != LOX_OK || lox_get_u32(entry_header + 0u) != LOX_WAL_ENTRY_MAGIC) {
            break;
        }

        data_len = lox_get_u16(entry_header + 10u);
        aligned_len = lox_align_u32(data_len, 4u);
        if (data_len > sizeof(payload) || offset + 16u + aligned_len > core->layout.wal_offset + core->layout.wal_size) {
            break;
        }

        err = lox_storage_read_bytes(core, offset + 16u, payload, aligned_len);
        if (err != LOX_OK) {
            break;
        }

        entry_crc = lox_get_u32(entry_header + 12u);
        crc = LOX_CRC32(entry_header, 12u);
        crc = lox_crc32(crc, payload, data_len);
        if (crc != entry_crc) {
            LOX_LOG("ERROR",
                        "WAL corrupt entry at seq=%u: CRC mismatch, stopping replay",
                        (unsigned)lox_get_u32(entry_header + 4u));
            break;
        }

        if (entry_header[8] == LOX_WAL_ENGINE_TXN_KV &&
            (entry_header[9] == LOX_WAL_OP_SET_INSERT || entry_header[9] == LOX_WAL_OP_DEL)) {
            if (txn_count < LOX_TXN_STAGE_KEYS && data_len <= sizeof(txn_payloads[0])) {
                txn_ops[txn_count] = entry_header[9];
                txn_lens[txn_count] = data_len;
                memcpy(txn_payloads[txn_count], payload, data_len);
                txn_count++;
            } else {
                txn_count = 0u;
            }
            offset += 16u + aligned_len;
            replayed_count++;
            continue;
        }
        if (entry_header[8] == LOX_WAL_ENGINE_META && entry_header[9] == LOX_WAL_OP_TXN_COMMIT) {
            uint32_t t;
            for (t = 0u; t < txn_count; ++t) {
                err = lox_apply_wal_entry(db, LOX_WAL_ENGINE_KV, txn_ops[t], txn_payloads[t], txn_lens[t]);
                if (err != LOX_OK) {
                    core->wal_replaying = false;
                    return err;
                }
            }
            txn_count = 0u;
            offset += 16u + aligned_len;
            replayed_count++;
            continue;
        }
        if (txn_count != 0u) {
            txn_count = 0u;
        }

        err = lox_apply_wal_entry(db, entry_header[8], entry_header[9], payload, data_len);
        if (err != LOX_OK) {
            core->wal_replaying = false;
            return err;
        }

        offset += 16u + aligned_len;
        replayed_count++;
    }
    core->wal_replaying = false;
    core->wal_used = offset - core->layout.wal_offset;
    LOX_LOG("INFO",
                "WAL recovery complete: replayed %u entries",
                (unsigned)replayed_count);
    return LOX_OK;
}

static lox_err_t lox_crc_storage_region(lox_core_t *core, uint32_t offset, uint32_t len, uint32_t *out_crc) {
    uint8_t chunk[128];
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t pos = 0u;
    lox_err_t err;

    while (pos < len) {
        uint32_t take = len - pos;
        if (take > sizeof(chunk)) {
            take = sizeof(chunk);
        }
        err = lox_storage_read_bytes(core, offset + pos, chunk, take);
        if (err != LOX_OK) {
            return err;
        }
        crc = lox_crc32(crc, chunk, take);
        pos += take;
    }

    *out_crc = crc;
    return LOX_OK;
}

static lox_err_t lox_validate_bank_pages(lox_core_t *core, uint32_t bank, uint32_t *out_generation) {
    uint8_t header[LOX_PAGE_HEADER_SIZE];
    uint32_t gen = 0u;
    uint32_t payload_len = 0u;
    uint32_t entry_count = 0u;
    uint32_t payload_crc = 0u;
    uint32_t calc_crc = 0u;
    lox_err_t err;

    err = lox_storage_read_bytes(core, lox_bank_kv_offset(core, bank), header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }
    if (!lox_validate_page_header(header,
                                      LOX_KV_PAGE_MAGIC,
                                      core->layout.kv_size - LOX_PAGE_HEADER_SIZE,
                                      &gen,
                                      &payload_len,
                                      &entry_count,
                                      &payload_crc)) {
        return LOX_ERR_CORRUPT;
    }
    (void)entry_count;
    err = lox_crc_storage_region(core, lox_bank_kv_offset(core, bank) + LOX_PAGE_HEADER_SIZE, payload_len, &calc_crc);
    if (err != LOX_OK || calc_crc != payload_crc) {
        return LOX_ERR_CORRUPT;
    }

#if LOX_ENABLE_TS
    err = lox_storage_read_bytes(core, lox_bank_ts_offset(core, bank), header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }
    {
        uint32_t gen2 = 0u;
        if (!lox_validate_page_header(header,
                                          LOX_TS_PAGE_MAGIC,
                                          core->layout.ts_size - LOX_PAGE_HEADER_SIZE,
                                          &gen2,
                                          &payload_len,
                                          &entry_count,
                                          &payload_crc)) {
            return LOX_ERR_CORRUPT;
        }
        if (gen2 != gen) {
            return LOX_ERR_CORRUPT;
        }
    }
    err = lox_crc_storage_region(core, lox_bank_ts_offset(core, bank) + LOX_PAGE_HEADER_SIZE, payload_len, &calc_crc);
    if (err != LOX_OK || calc_crc != payload_crc) {
        return LOX_ERR_CORRUPT;
    }
#endif

#if LOX_ENABLE_REL
    err = lox_storage_read_bytes(core, lox_bank_rel_offset(core, bank), header, sizeof(header));
    if (err != LOX_OK) {
        return err;
    }
    {
        uint32_t gen3 = 0u;
        if (!lox_validate_page_header(header,
                                          LOX_REL_PAGE_MAGIC,
                                          core->layout.rel_size - LOX_PAGE_HEADER_SIZE,
                                          &gen3,
                                          &payload_len,
                                          &entry_count,
                                          &payload_crc)) {
            return LOX_ERR_CORRUPT;
        }
        if (gen3 != gen) {
            return LOX_ERR_CORRUPT;
        }
    }
    err = lox_crc_storage_region(core, lox_bank_rel_offset(core, bank) + LOX_PAGE_HEADER_SIZE, payload_len, &calc_crc);
    if (err != LOX_OK || calc_crc != payload_crc) {
        return LOX_ERR_CORRUPT;
    }
#endif

    *out_generation = gen;
    return LOX_OK;
}

lox_err_t lox_storage_bootstrap(lox_t *db) {
    lox_core_t *core = lox_core(db);
    lox_err_t err;
    bool had_entries = false;
    bool reset_header = false;
    bool super_a_valid = false;
    bool super_b_valid = false;
    uint8_t super_a[LOX_SUPERBLOCK_SIZE];
    uint8_t super_b[LOX_SUPERBLOCK_SIZE];
    uint32_t super_a_gen = 0u;
    uint32_t super_b_gen = 0u;
    uint32_t super_a_bank = 0u;
    uint32_t super_b_bank = 0u;
    uint32_t fallback_gen_a = 0u;
    uint32_t fallback_gen_b = 0u;
    bool fallback_a_valid = false;
    bool fallback_b_valid = false;
    uint32_t selected_bank = 0u;
    uint32_t selected_gen = 0u;
    bool have_selected = false;
    uint32_t erase_size;

    memset(&core->layout, 0, sizeof(core->layout));
    core->wal_sequence = 0u;
    core->wal_entry_count = 0u;
    core->wal_used = LOX_WAL_HEADER_SIZE;
    core->last_recovery_status = LOX_OK;

    if (!lox_storage_ready(core)) {
        return LOX_OK;
    }
    if (core->storage->erase_size == 0u) {
        LOX_LOG("ERROR", "%s", "Storage contract violation: erase_size must be > 0");
        return LOX_ERR_INVALID;
    }
    if (core->storage->write_size == 0u) {
        LOX_LOG("ERROR", "%s", "Storage contract violation: write_size must be 1 (got 0)");
        return LOX_ERR_INVALID;
    }
    if (core->storage->write_size != 1u) {
        LOX_LOG("ERROR",
                    "Storage contract violation: write_size=%u unsupported (only 1 is supported in this release)",
                    (unsigned)core->storage->write_size);
        return LOX_ERR_INVALID;
    }

    {
        uint32_t wal_target;
        uint32_t wal_min;
        uint32_t fixed_bytes;
        uint32_t need_without_wal;
        uint32_t max_wal;
        uint32_t max_wal_aligned;

        erase_size = core->storage->erase_size;
        core->layout.wal_offset = 0u;
        core->layout.super_size = erase_size;
        wal_target = erase_size * 8u;
        wal_min = erase_size * 2u;
        core->layout.kv_size =
            lox_align_u32(lox_kv_snapshot_payload_max(core) + LOX_PAGE_HEADER_SIZE, erase_size);
        core->layout.ts_size =
#if LOX_ENABLE_TS
            lox_align_u32((uint32_t)core->ts_arena.capacity + LOX_PAGE_HEADER_SIZE, erase_size);
#else
            0u;
#endif
        core->layout.rel_size =
#if LOX_ENABLE_REL
            lox_align_u32((uint32_t)core->rel_arena.capacity + LOX_PAGE_HEADER_SIZE, erase_size);
#else
            0u;
#endif
        core->layout.bank_size = core->layout.kv_size + core->layout.ts_size + core->layout.rel_size;

        fixed_bytes = core->layout.super_size * 2u;
        need_without_wal = fixed_bytes + (core->layout.bank_size * 2u);
        if (core->storage->capacity < need_without_wal + wal_min) {
            return LOX_ERR_STORAGE;
        }

        max_wal = core->storage->capacity - need_without_wal;
        max_wal_aligned = (max_wal / erase_size) * erase_size;
        if (max_wal_aligned < wal_min) {
            return LOX_ERR_STORAGE;
        }

        core->layout.wal_size = wal_target;
        if (core->layout.wal_size > max_wal_aligned) {
            core->layout.wal_size = max_wal_aligned;
        }
        if (core->layout.wal_size < wal_min) {
            core->layout.wal_size = wal_min;
        }
    }
    core->layout.super_a_offset = core->layout.wal_offset + core->layout.wal_size;
    core->layout.super_b_offset = core->layout.super_a_offset + core->layout.super_size;
    core->layout.bank_a_offset = core->layout.super_b_offset + core->layout.super_size;
    core->layout.bank_b_offset = core->layout.bank_a_offset + core->layout.bank_size;
    core->layout.total_size = core->layout.bank_b_offset + core->layout.bank_size;

    if (core->storage->capacity < core->layout.total_size) {
        return LOX_ERR_STORAGE;
    }

    err = lox_storage_read_bytes(core, core->layout.super_a_offset, super_a, sizeof(super_a));
    if (err != LOX_OK) {
        return err;
    }
    err = lox_storage_read_bytes(core, core->layout.super_b_offset, super_b, sizeof(super_b));
    if (err != LOX_OK) {
        return err;
    }

    super_a_valid = lox_validate_superblock(super_a, &super_a_gen, &super_a_bank);
    super_b_valid = lox_validate_superblock(super_b, &super_b_gen, &super_b_bank);

    /* Boot selection invariant:
     * 1) prefer newest valid superblock;
     * 2) if no valid superblock exists, fallback to fully valid bank scan;
     * 3) selected bank pages must all pass header+payload CRC validation.
     */
    if (super_a_valid || super_b_valid) {
        if (super_a_valid && (!super_b_valid || super_a_gen >= super_b_gen)) {
            selected_bank = super_a_bank;
            selected_gen = super_a_gen;
        } else {
            selected_bank = super_b_bank;
            selected_gen = super_b_gen;
        }
        have_selected = true;
    } else {
        if (lox_validate_bank_pages(core, 0u, &fallback_gen_a) == LOX_OK) {
            fallback_a_valid = true;
        }
        if (lox_validate_bank_pages(core, 1u, &fallback_gen_b) == LOX_OK) {
            fallback_b_valid = true;
        }
        if (fallback_a_valid || fallback_b_valid) {
            if (fallback_a_valid && (!fallback_b_valid || fallback_gen_a >= fallback_gen_b)) {
                selected_bank = 0u;
                selected_gen = fallback_gen_a;
            } else {
                selected_bank = 1u;
                selected_gen = fallback_gen_b;
            }
            have_selected = true;
        }
    }

    if (have_selected) {
        core->reopen_count++;
        core->storage_loading = true;
        err = lox_load_kv_page(db, selected_bank, selected_gen);
#if LOX_ENABLE_TS
        if (err == LOX_OK) {
            err = lox_load_ts_page(db, selected_bank, selected_gen);
        }
#endif
#if LOX_ENABLE_REL
        if (err == LOX_OK) {
            err = lox_load_rel_page(db, selected_bank, selected_gen);
        }
#endif
        core->storage_loading = false;
        if (err != LOX_OK) {
            return err;
        }
        core->layout.active_bank = selected_bank;
        core->layout.active_generation = selected_gen;
    } else {
        uint8_t probe[16];
        bool virgin = true;
        err = lox_storage_read_bytes(core, core->layout.super_a_offset, probe, sizeof(probe));
        if (err != LOX_OK) {
            return err;
        }
        for (uint32_t k = 0u; k < sizeof(probe); ++k) {
            if (probe[k] != 0xFFu) {
                virgin = false;
                break;
            }
        }
        if (virgin) {
            err = lox_storage_read_bytes(core, core->layout.super_b_offset, probe, sizeof(probe));
            if (err != LOX_OK) {
                return err;
            }
            for (uint32_t k = 0u; k < sizeof(probe); ++k) {
                if (probe[k] != 0xFFu) {
                    virgin = false;
                    break;
                }
            }
        }
        if (!virgin) {
            return LOX_ERR_CORRUPT;
        }
        /* Cold start: initialize first durable snapshot bank/superblock. */
        core->layout.active_bank = 0u;
        core->layout.active_generation = 1u;
        err = lox_write_snapshot_bank(core, 0u, core->layout.active_generation);
        if (err != LOX_OK) {
            return err;
        }
        err = lox_storage_sync_core(core);
        if (err != LOX_OK) {
            return err;
        }
        err = lox_write_superblock(core, core->layout.active_generation, 0u);
        if (err != LOX_OK) {
            return err;
        }
        err = lox_storage_sync_core(core);
        if (err != LOX_OK) {
            return err;
        }
    }

    if (!core->wal_enabled) {
        return LOX_OK;
    }

    err = lox_replay_wal(db, &had_entries, &reset_header);
    if (err != LOX_OK) {
        core->last_recovery_status = err;
        lox_record_error(core, err);
        return err;
    }

    if (had_entries || reset_header) {
        err = lox_storage_flush(db);
        core->last_recovery_status = err;
        if (err == LOX_OK) {
            core->recovery_count++;
        } else {
            lox_record_error(core, err);
        }
        return err;
    }

    err = lox_reset_wal(core, core->wal_sequence);
    core->last_recovery_status = err;
    if (err != LOX_OK) {
        lox_record_error(core, err);
    }
    return err;
}

static lox_err_t lox_append_wal_entry(lox_t *db,
                                              uint8_t engine,
                                              uint8_t op,
                                              const uint8_t *payload,
                                              uint16_t payload_len) {
    lox_core_t *core = lox_core(db);
    uint32_t aligned_len = lox_align_u32(payload_len, 4u);
    uint32_t entry_len = 16u + aligned_len;
    uint32_t offset = 0u;
    uint32_t pad_len = aligned_len - payload_len;
    uint8_t header[16];
    uint8_t pad[4] = { 0u, 0u, 0u, 0u };
    uint8_t coalesced[320];
    uint32_t crc;
    lox_err_t err;

    if (core->wal_used + entry_len > core->layout.wal_size) {
        err = lox_storage_flush(db);
        if (err != LOX_OK) {
            return err;
        }
    }

    if (core->wal_used + entry_len > core->layout.wal_size) {
        return LOX_ERR_STORAGE;
    }

    memset(header, 0, sizeof(header));
    lox_put_u32(header + 0u, LOX_WAL_ENTRY_MAGIC);
    lox_put_u32(header + 4u, core->wal_entry_count + 1u);
    header[8] = engine;
    header[9] = op;
    lox_put_u16(header + 10u, payload_len);
    crc = LOX_CRC32(header, 12u);
    if (payload_len != 0u) {
        crc = lox_crc32(crc, payload, payload_len);
    }
    lox_put_u32(header + 12u, crc);

    offset = core->layout.wal_offset + core->wal_used;
    if (entry_len <= sizeof(coalesced)) {
        memcpy(coalesced, header, sizeof(header));
        if (payload_len != 0u) {
            memcpy(coalesced + sizeof(header), payload, payload_len);
        }
        if (pad_len != 0u) {
            memcpy(coalesced + sizeof(header) + payload_len, pad, pad_len);
        }
        err = lox_storage_write_bytes(core, offset, coalesced, entry_len);
        if (err != LOX_OK) {
            return err;
        }
    } else {
        err = lox_storage_write_bytes(core, offset, header, sizeof(header));
        if (err != LOX_OK) {
            return err;
        }
        offset += (uint32_t)sizeof(header);

        if (payload_len != 0u) {
            err = lox_storage_write_bytes(core, offset, payload, payload_len);
            if (err != LOX_OK) {
                return err;
            }
            offset += payload_len;
        }

        if (pad_len != 0u) {
            err = lox_storage_write_bytes(core, offset, pad, pad_len);
            if (err != LOX_OK) {
                return err;
            }
        }
    }

    core->wal_used += entry_len;
    core->wal_entry_count++;
    err = lox_write_wal_header(core);
    if (err != LOX_OK) {
        return err;
    }
    if (core->wal_sync_mode == LOX_WAL_SYNC_ALWAYS) {
        err = lox_storage_sync_core(core);
        if (err != LOX_OK) {
            return err;
        }
    }

    return LOX_OK;
}

static lox_err_t lox_compact_nolock(lox_t *db) {
    lox_core_t *core;
    lox_err_t err;
    uint32_t next_bank;
    uint32_t next_generation;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        return LOX_ERR_INVALID;
    }
    if (!lox_storage_ready(core)) {
        return LOX_OK;
    }
    /* Durability invariant:
     * - active bank is never erased in-place.
     * - compact writes full snapshot into inactive bank, syncs, then switches superblock.
     */
    next_bank = (core->layout.active_bank == 0u) ? 1u : 0u;
    next_generation = core->layout.active_generation + 1u;
    err = lox_write_snapshot_bank(core, next_bank, next_generation);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_storage_sync_core(core);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_write_superblock(core, next_generation, next_bank);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_storage_sync_core(core);
    if (err != LOX_OK) {
        return err;
    }
    core->layout.active_bank = next_bank;
    core->layout.active_generation = next_generation;
    if (core->wal_enabled) {
        err = lox_reset_wal(core, core->wal_sequence + 1u);
        if (err != LOX_OK) {
            return err;
        }
        core->compact_count++;
        return LOX_OK;
    }
    core->compact_count++;
    return LOX_OK;
}

lox_err_t lox_compact(lox_t *db) {
    lox_err_t rc;
    lox_core_t *core;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        LOX_UNLOCK(db);
        return LOX_ERR_INVALID;
    }
    rc = lox_compact_nolock(db);
    lox_record_error(core, rc);
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_storage_flush(lox_t *db) {
    lox_core_t *core = lox_core(db);
    lox_err_t rc;

    /* Ordering invariant:
     * - flush never runs during bootstrap load or WAL replay.
     * - it must not serialize transient replay/load state.
     */
    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }

    if (core->wal_enabled) {
        LOX_LOG("INFO",
                    "WAL compaction triggered: entry_count=%u",
                    (unsigned)core->wal_entry_count);
    }

    rc = lox_compact_nolock(db);
    lox_record_error(core, rc);
    return rc;
}

lox_err_t lox_persist_kv_set(lox_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[256];
    size_t key_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    key_len = strlen(key);
    payload[0] = (uint8_t)key_len;
    memcpy(payload + 1u, key, key_len);
    lox_put_u32(payload + 1u + key_len, (uint32_t)len);
    memcpy(payload + 1u + key_len + 4u, val, len);
    lox_put_u32(payload + 1u + key_len + 4u + len, expires_at);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_KV,
                                    LOX_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + key_len + 4u + len + 4u));
}

lox_err_t lox_persist_kv_set_txn(lox_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[256];
    size_t key_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    key_len = strlen(key);
    payload[0] = (uint8_t)key_len;
    memcpy(payload + 1u, key, key_len);
    lox_put_u32(payload + 1u + key_len, (uint32_t)len);
    memcpy(payload + 1u + key_len + 4u, val, len);
    lox_put_u32(payload + 1u + key_len + 4u + len, expires_at);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_TXN_KV,
                                    LOX_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + key_len + 4u + len + 4u));
}

lox_err_t lox_persist_kv_del(lox_t *db, const char *key) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[LOX_KV_KEY_MAX_LEN];
    size_t key_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    key_len = strlen(key);
    payload[0] = (uint8_t)key_len;
    memcpy(payload + 1u, key, key_len);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_KV,
                                    LOX_WAL_OP_DEL,
                                    payload,
                                    (uint16_t)(1u + key_len));
}

lox_err_t lox_persist_kv_del_txn(lox_t *db, const char *key) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[LOX_KV_KEY_MAX_LEN];
    size_t key_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    key_len = strlen(key);
    payload[0] = (uint8_t)key_len;
    memcpy(payload + 1u, key, key_len);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_TXN_KV,
                                    LOX_WAL_OP_DEL,
                                    payload,
                                    (uint16_t)(1u + key_len));
}

lox_err_t lox_persist_kv_clear(lox_t *db) {
    lox_core_t *core = lox_core(db);

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    return lox_append_wal_entry(db, LOX_WAL_ENGINE_KV, LOX_WAL_OP_CLEAR, NULL, 0u);
}

lox_err_t lox_persist_txn_commit(lox_t *db) {
    lox_core_t *core = lox_core(db);

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    return lox_append_wal_entry(db, LOX_WAL_ENGINE_META, LOX_WAL_OP_TXN_COMMIT, NULL, 0u);
}

lox_err_t lox_persist_ts_insert(lox_t *db, const char *name, lox_timestamp_t ts, const void *val, size_t val_len) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[256];
    size_t idx;
    size_t name_len;
    uint64_t full_ts = (uint64_t)ts;
    lox_ts_stream_t *stream = NULL;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    for (idx = 0u; idx < LOX_TS_MAX_STREAMS; ++idx) {
        if (core->ts.streams[idx].registered && strcmp(core->ts.streams[idx].name, name) == 0) {
            stream = &core->ts.streams[idx];
            break;
        }
    }
    if (stream == NULL) {
        return LOX_ERR_NOT_FOUND;
    }

    name_len = strlen(name);
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, name, name_len);
    lox_put_u32(payload + 1u + name_len, (uint32_t)(full_ts & 0xFFFFFFFFu));
    lox_put_u32(payload + 1u + name_len + 4u, (uint32_t)(full_ts >> 32u));
    payload[1u + name_len + 8u] = (uint8_t)stream->type;
    memcpy(payload + 1u + name_len + 9u, val, val_len);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_TS,
                                    LOX_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + name_len + 9u + val_len));
}

lox_err_t lox_persist_ts_register(lox_t *db, const char *name, lox_ts_type_t type, size_t raw_size) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[64];
    size_t name_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    name_len = strlen(name);
    if (name_len + 6u > sizeof(payload)) {
        return LOX_ERR_INVALID;
    }
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, name, name_len);
    payload[1u + name_len] = (uint8_t)type;
    lox_put_u32(payload + 1u + name_len + 1u, (uint32_t)raw_size);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_TS,
                                    LOX_WAL_OP_TS_REGISTER,
                                    payload,
                                    (uint16_t)(1u + name_len + 1u + 4u));
}

lox_err_t lox_persist_ts_clear(lox_t *db, const char *name) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[LOX_TS_STREAM_NAME_LEN];
    size_t name_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    name_len = strlen(name);
    if (name_len + 1u > sizeof(payload)) {
        return LOX_ERR_INVALID;
    }
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, name, name_len);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_TS,
                                    LOX_WAL_OP_CLEAR,
                                    payload,
                                    (uint16_t)(1u + name_len));
}

lox_err_t lox_persist_rel_insert(lox_t *db, const lox_table_t *table, const void *row_buf) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[1536];
    size_t name_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }
    if (table->row_size + LOX_REL_TABLE_NAME_LEN + 5u > sizeof(payload)) {
        return LOX_ERR_STORAGE;
    }

    name_len = strlen(table->name);
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, table->name, name_len);
    lox_put_u32(payload + 1u + name_len, (uint32_t)table->row_size);
    memcpy(payload + 1u + name_len + 4u, row_buf, table->row_size);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_REL,
                                    LOX_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + name_len + 4u + table->row_size));
}

lox_err_t lox_persist_rel_delete(lox_t *db, const lox_table_t *table, const void *search_val) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[64];
    size_t name_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }

    name_len = strlen(table->name);
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, table->name, name_len);
    memcpy(payload + 1u + name_len, search_val, table->index_key_size);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_REL,
                                    LOX_WAL_OP_DEL,
                                    payload,
                                    (uint16_t)(1u + name_len + table->index_key_size));
}

lox_err_t lox_persist_rel_table_create(lox_t *db, const lox_schema_t *schema) {
    lox_core_t *core = lox_core(db);
    const lox_schema_impl_t *impl;
    uint8_t payload[512];
    uint32_t i;
    uint16_t off = 0u;
    size_t name_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }
    if (schema == NULL) {
        return LOX_ERR_INVALID;
    }

    impl = (const lox_schema_impl_t *)&schema->_opaque[0];
    if (!impl->sealed) {
        return LOX_ERR_INVALID;
    }

    name_len = strlen(impl->name);
    if (name_len >= LOX_REL_TABLE_NAME_LEN) {
        return LOX_ERR_INVALID;
    }
    if (1u + name_len + 2u + 4u + 4u > sizeof(payload)) {
        return LOX_ERR_STORAGE;
    }

    payload[off++] = (uint8_t)name_len;
    memcpy(payload + off, impl->name, name_len);
    off = (uint16_t)(off + name_len);
    lox_put_u16(payload + off, impl->schema_version);
    off = (uint16_t)(off + 2u);
    lox_put_u32(payload + off, impl->max_rows);
    off = (uint16_t)(off + 4u);
    lox_put_u32(payload + off, impl->col_count);
    off = (uint16_t)(off + 4u);

    for (i = 0u; i < impl->col_count; ++i) {
        size_t col_name_len = strlen(impl->cols[i].name);
        if (col_name_len >= LOX_REL_COL_NAME_LEN) {
            return LOX_ERR_SCHEMA;
        }
        if ((size_t)off + 1u + col_name_len + 1u + 1u + 4u > sizeof(payload)) {
            return LOX_ERR_STORAGE;
        }
        payload[off++] = (uint8_t)col_name_len;
        memcpy(payload + off, impl->cols[i].name, col_name_len);
        off = (uint16_t)(off + col_name_len);
        payload[off++] = (uint8_t)impl->cols[i].type;
        payload[off++] = (uint8_t)(impl->cols[i].is_index ? 1u : 0u);
        lox_put_u32(payload + off, (uint32_t)impl->cols[i].size);
        off = (uint16_t)(off + 4u);
    }

    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_REL,
                                    LOX_WAL_OP_REL_TABLE_CREATE,
                                    payload,
                                    off);
}

lox_err_t lox_persist_rel_clear(lox_t *db, const lox_table_t *table) {
    lox_core_t *core = lox_core(db);
    uint8_t payload[LOX_REL_TABLE_NAME_LEN];
    size_t name_len;

    if (!lox_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return LOX_OK;
    }
    if (!core->wal_enabled) {
        return lox_storage_flush(db);
    }
    if (table == NULL) {
        return LOX_ERR_INVALID;
    }

    name_len = strlen(table->name);
    if (name_len + 1u > sizeof(payload)) {
        return LOX_ERR_INVALID;
    }
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, table->name, name_len);
    return lox_append_wal_entry(db,
                                    LOX_WAL_ENGINE_REL,
                                    LOX_WAL_OP_CLEAR,
                                    payload,
                                    (uint16_t)(1u + name_len));
}
