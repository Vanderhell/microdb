#include "microdb_internal.h"
#include "microdb_crc.h"

#include <string.h>

enum {
    MICRODB_WAL_MAGIC = 0x4D44424Cu,
    MICRODB_WAL_VERSION = 0x00010000u,
    MICRODB_WAL_ENTRY_MAGIC = 0x454E5452u,
    MICRODB_KV_PAGE_MAGIC = 0x4B565047u,
    MICRODB_TS_PAGE_MAGIC = 0x54535047u,
    MICRODB_REL_PAGE_MAGIC = 0x524C5047u,
    MICRODB_WAL_ENGINE_KV = 0u,
    MICRODB_WAL_ENGINE_TS = 1u,
    MICRODB_WAL_ENGINE_REL = 2u,
    MICRODB_WAL_OP_SET_INSERT = 0u,
    MICRODB_WAL_OP_DEL = 1u,
    MICRODB_WAL_OP_CLEAR = 2u
};

static uint32_t microdb_align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static void microdb_put_u32(uint8_t *dst, uint32_t value) {
    memcpy(dst, &value, sizeof(value));
}

static void microdb_put_u16(uint8_t *dst, uint16_t value) {
    memcpy(dst, &value, sizeof(value));
}

static uint32_t microdb_get_u32(const uint8_t *src) {
    uint32_t value = 0u;
    memcpy(&value, src, sizeof(value));
    return value;
}

static uint16_t microdb_get_u16(const uint8_t *src) {
    uint16_t value = 0u;
    memcpy(&value, src, sizeof(value));
    return value;
}

static bool microdb_storage_ready(const microdb_core_t *core) {
    return core->storage != NULL && core->storage->read != NULL && core->storage->write != NULL &&
           core->storage->erase != NULL && core->storage->sync != NULL;
}

static microdb_err_t microdb_storage_read_bytes(microdb_core_t *core, uint32_t offset, void *buf, size_t len) {
    return core->storage->read(core->storage->ctx, offset, buf, len);
}

static microdb_err_t microdb_storage_write_bytes(microdb_core_t *core, uint32_t offset, const void *buf, size_t len) {
    microdb_err_t err = core->storage->write(core->storage->ctx, offset, buf, len);
    if (err == MICRODB_OK) {
        core->storage_bytes_written += (uint32_t)len;
    }
    return err;
}

static microdb_err_t microdb_storage_erase_region(microdb_core_t *core, uint32_t offset, uint32_t size) {
    uint32_t pos;

    for (pos = 0u; pos < size; pos += core->storage->erase_size) {
        microdb_err_t err = core->storage->erase(core->storage->ctx, offset + pos);
        if (err != MICRODB_OK) {
            return err;
        }
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_storage_sync_core(microdb_core_t *core) {
    return core->storage->sync(core->storage->ctx);
}

static microdb_err_t microdb_write_wal_header(microdb_core_t *core) {
    uint8_t header[32];
    uint32_t crc;

    memset(header, 0, sizeof(header));
    microdb_put_u32(header + 0u, MICRODB_WAL_MAGIC);
    microdb_put_u32(header + 4u, MICRODB_WAL_VERSION);
    microdb_put_u32(header + 8u, core->wal_entry_count);
    microdb_put_u32(header + 12u, core->wal_sequence);
    crc = MICRODB_CRC32(header, 16u);
    microdb_put_u32(header + 16u, crc);
    return microdb_storage_write_bytes(core, core->layout.wal_offset, header, sizeof(header));
}

static microdb_err_t microdb_reset_wal(microdb_core_t *core, uint32_t next_sequence) {
    microdb_err_t err;

    core->wal_sequence = next_sequence;
    core->wal_entry_count = 0u;
    core->wal_used = 32u;

    err = microdb_storage_erase_region(core, core->layout.wal_offset, core->layout.wal_size);
    if (err != MICRODB_OK) {
        return err;
    }

    err = microdb_write_wal_header(core);
    if (err != MICRODB_OK) {
        return err;
    }

    return microdb_storage_sync_core(core);
}

static microdb_err_t microdb_write_page_prefix(microdb_core_t *core,
                                               uint32_t offset,
                                               uint32_t magic,
                                               uint32_t count,
                                               uint32_t crc) {
    uint8_t header[12];

    microdb_put_u32(header + 0u, magic);
    microdb_put_u32(header + 4u, count);
    microdb_put_u32(header + 8u, crc);
    return microdb_storage_write_bytes(core, offset, header, sizeof(header));
}

static microdb_err_t microdb_write_kv_page(microdb_core_t *core) {
    uint32_t count = 0u;
    uint32_t offset = core->layout.kv_offset + 12u;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_erase_region(core, core->layout.kv_offset, core->layout.kv_size);
    if (err != MICRODB_OK) {
        return err;
    }

    for (i = 0u; i < core->kv.bucket_count; ++i) {
        const microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        uint8_t key_len;
        uint8_t header[4];

        if (bucket->state != 1u) {
            continue;
        }

        key_len = (uint8_t)strlen(bucket->key);
        if (offset + 1u + key_len + 4u + bucket->val_len + 4u > core->layout.kv_offset + core->layout.kv_size) {
            return MICRODB_ERR_STORAGE;
        }

        err = microdb_storage_write_bytes(core, offset, &key_len, 1u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, &key_len, 1u);
        offset += 1u;

        err = microdb_storage_write_bytes(core, offset, bucket->key, key_len);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, bucket->key, key_len);
        offset += key_len;

        microdb_put_u32(header, bucket->val_len);
        err = microdb_storage_write_bytes(core, offset, header, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, header, 4u);
        offset += 4u;

        err = microdb_storage_write_bytes(core, offset, &core->kv.value_store[bucket->val_offset], bucket->val_len);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, &core->kv.value_store[bucket->val_offset], bucket->val_len);
        offset += bucket->val_len;

        microdb_put_u32(header, bucket->expires_at);
        err = microdb_storage_write_bytes(core, offset, header, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, header, 4u);
        offset += 4u;
        count++;
    }

    return microdb_write_page_prefix(core, core->layout.kv_offset, MICRODB_KV_PAGE_MAGIC, count, crc);
}

static uint32_t microdb_ts_stream_val_size(const microdb_ts_stream_t *stream) {
    return (stream->type == MICRODB_TS_RAW) ? (uint32_t)stream->raw_size : 4u;
}

static microdb_err_t microdb_write_ts_page(microdb_core_t *core) {
    uint32_t stream_count = 0u;
    uint32_t offset = core->layout.ts_offset + 12u;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_erase_region(core, core->layout.ts_offset, core->layout.ts_size);
    if (err != MICRODB_OK) {
        return err;
    }

    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
        const microdb_ts_stream_t *stream = &core->ts.streams[i];
        uint8_t name_len;
        uint8_t one;
        uint8_t u32buf[4];
        uint32_t j;
        uint32_t idx;

        if (!stream->registered) {
            continue;
        }

        name_len = (uint8_t)strlen(stream->name);
        one = name_len;
        err = microdb_storage_write_bytes(core, offset, &one, 1u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, &one, 1u);
        offset += 1u;

        err = microdb_storage_write_bytes(core, offset, stream->name, name_len);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, stream->name, name_len);
        offset += name_len;

        one = (uint8_t)stream->type;
        err = microdb_storage_write_bytes(core, offset, &one, 1u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, &one, 1u);
        offset += 1u;

        microdb_put_u32(u32buf, (uint32_t)stream->raw_size);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        microdb_put_u32(u32buf, stream->count);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        idx = stream->tail;
        for (j = 0u; j < stream->count; ++j) {
            const microdb_ts_sample_t *sample = &stream->buf[idx];
            uint64_t ts = (uint64_t)sample->ts;
            uint32_t ts_low = (uint32_t)(ts & 0xFFFFFFFFu);
            uint32_t ts_high = (uint32_t)(ts >> 32u);
            uint32_t val_len = microdb_ts_stream_val_size(stream);

            microdb_put_u32(u32buf, ts_low);
            err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, u32buf, 4u);
            offset += 4u;

            microdb_put_u32(u32buf, ts_high);
            err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, u32buf, 4u);
            offset += 4u;

            err = microdb_storage_write_bytes(core, offset, &sample->v, val_len);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, &sample->v, val_len);
            offset += val_len;
            idx = (idx + 1u) % stream->capacity;
        }

        stream_count++;
    }

    return microdb_write_page_prefix(core, core->layout.ts_offset, MICRODB_TS_PAGE_MAGIC, stream_count, crc);
}

static microdb_err_t microdb_write_rel_page(microdb_core_t *core) {
    uint32_t table_count = 0u;
    uint32_t offset = core->layout.rel_offset + 12u;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_erase_region(core, core->layout.rel_offset, core->layout.rel_size);
    if (err != MICRODB_OK) {
        return err;
    }

    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        const microdb_table_t *table = &core->rel.tables[i];
        uint8_t name_len;
        uint8_t meta[2];
        uint8_t u32buf[4];
        uint32_t j;

        if (!table->registered) {
            continue;
        }

        name_len = (uint8_t)strlen(table->name);
        err = microdb_storage_write_bytes(core, offset, &name_len, 1u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, &name_len, 1u);
        offset += 1u;

        err = microdb_storage_write_bytes(core, offset, table->name, name_len);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, table->name, name_len);
        offset += name_len;

        microdb_put_u32(u32buf, table->max_rows);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        microdb_put_u32(u32buf, (uint32_t)table->row_size);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        microdb_put_u32(u32buf, table->col_count);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        microdb_put_u32(u32buf, table->index_col);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        microdb_put_u32(u32buf, table->live_count);
        err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u32buf, 4u);
        offset += 4u;

        for (j = 0u; j < table->col_count; ++j) {
            const microdb_col_desc_t *col = &table->cols[j];
            uint8_t col_name_len = (uint8_t)strlen(col->name);

            err = microdb_storage_write_bytes(core, offset, &col_name_len, 1u);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, &col_name_len, 1u);
            offset += 1u;

            err = microdb_storage_write_bytes(core, offset, col->name, col_name_len);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, col->name, col_name_len);
            offset += col_name_len;

            meta[0] = (uint8_t)col->type;
            meta[1] = (uint8_t)(col->is_index ? 1u : 0u);
            err = microdb_storage_write_bytes(core, offset, meta, 2u);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, meta, 2u);
            offset += 2u;

            microdb_put_u32(u32buf, (uint32_t)col->size);
            err = microdb_storage_write_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, u32buf, 4u);
            offset += 4u;
        }

        for (j = 0u; j < table->order_count; ++j) {
            uint32_t row_idx = table->order[j];
            if (((table->alive_bitmap[row_idx >> 3u] >> (row_idx & 7u)) & 1u) == 0u) {
                continue;
            }
            err = microdb_storage_write_bytes(core, offset, table->rows + ((size_t)row_idx * table->row_size), table->row_size);
            if (err != MICRODB_OK) {
                return err;
            }
            crc = microdb_crc32(crc, table->rows + ((size_t)row_idx * table->row_size), table->row_size);
            offset += (uint32_t)table->row_size;
        }

        table_count++;
    }

    return microdb_write_page_prefix(core, core->layout.rel_offset, MICRODB_REL_PAGE_MAGIC, table_count, crc);
}

static microdb_err_t microdb_load_kv_page(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[12];
    uint32_t offset;
    uint32_t count;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, core->layout.kv_offset, header, sizeof(header));
    if (err != MICRODB_OK || microdb_get_u32(header) != MICRODB_KV_PAGE_MAGIC) {
        return MICRODB_OK;
    }

    count = microdb_get_u32(header + 4u);
    offset = core->layout.kv_offset + 12u;
    for (i = 0u; i < count; ++i) {
        uint8_t key_len = 0u;
        char key[MICRODB_KV_KEY_MAX_LEN];
        uint8_t u32buf[4];
        uint32_t val_len;
        uint32_t expires_at;
        uint8_t value[MICRODB_KV_VAL_MAX_LEN];

        err = microdb_storage_read_bytes(core, offset, &key_len, 1u);
        if (err != MICRODB_OK || key_len >= MICRODB_KV_KEY_MAX_LEN) {
            return MICRODB_OK;
        }
        offset += 1u;

        err = microdb_storage_read_bytes(core, offset, key, key_len);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        key[key_len] = '\0';
        offset += key_len;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        val_len = microdb_get_u32(u32buf);
        offset += 4u;
        if (val_len > MICRODB_KV_VAL_MAX_LEN) {
            return MICRODB_OK;
        }

        err = microdb_storage_read_bytes(core, offset, value, val_len);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        offset += val_len;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        expires_at = microdb_get_u32(u32buf);
        offset += 4u;

        err = microdb_kv_set_at(db, key, value, val_len, expires_at);
        if (err != MICRODB_OK) {
            return err;
        }
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_load_ts_page(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[12];
    uint32_t offset;
    uint32_t stream_count;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, core->layout.ts_offset, header, sizeof(header));
    if (err != MICRODB_OK || microdb_get_u32(header) != MICRODB_TS_PAGE_MAGIC) {
        return MICRODB_OK;
    }

    stream_count = microdb_get_u32(header + 4u);
    offset = core->layout.ts_offset + 12u;
    for (i = 0u; i < stream_count; ++i) {
        uint8_t name_len = 0u;
        char name[MICRODB_TS_STREAM_NAME_LEN];
        uint8_t type_byte = 0u;
        uint8_t u32buf[4];
        uint32_t raw_size;
        uint32_t sample_count;
        uint32_t j;

        err = microdb_storage_read_bytes(core, offset, &name_len, 1u);
        if (err != MICRODB_OK || name_len >= MICRODB_TS_STREAM_NAME_LEN) {
            return MICRODB_OK;
        }
        offset += 1u;

        err = microdb_storage_read_bytes(core, offset, name, name_len);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        name[name_len] = '\0';
        offset += name_len;

        err = microdb_storage_read_bytes(core, offset, &type_byte, 1u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        offset += 1u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        raw_size = microdb_get_u32(u32buf);
        offset += 4u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        sample_count = microdb_get_u32(u32buf);
        offset += 4u;

        err = microdb_ts_register(db, name, (microdb_ts_type_t)type_byte, raw_size);
        if (err != MICRODB_OK && err != MICRODB_ERR_EXISTS) {
            return err;
        }

        for (j = 0u; j < sample_count; ++j) {
            uint32_t ts_low;
            uint32_t ts_high;
            uint8_t value[MICRODB_TS_RAW_MAX];
            uint32_t val_len = ((microdb_ts_type_t)type_byte == MICRODB_TS_RAW) ? raw_size : 4u;
            uint64_t full_ts;

            err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            ts_low = microdb_get_u32(u32buf);
            offset += 4u;

            err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            ts_high = microdb_get_u32(u32buf);
            offset += 4u;

            err = microdb_storage_read_bytes(core, offset, value, val_len);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            offset += val_len;

            full_ts = ((uint64_t)ts_high << 32u) | ts_low;
            err = microdb_ts_insert(db, name, (microdb_timestamp_t)full_ts, value);
            if (err != MICRODB_OK) {
                return err;
            }
        }
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_load_rel_page(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[12];
    uint32_t offset;
    uint32_t table_count;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, core->layout.rel_offset, header, sizeof(header));
    if (err != MICRODB_OK || microdb_get_u32(header) != MICRODB_REL_PAGE_MAGIC) {
        return MICRODB_OK;
    }

    table_count = microdb_get_u32(header + 4u);
    offset = core->layout.rel_offset + 12u;
    for (i = 0u; i < table_count; ++i) {
        microdb_schema_t schema;
        microdb_table_t *table = NULL;
        uint8_t name_len = 0u;
        char table_name[MICRODB_REL_TABLE_NAME_LEN];
        uint8_t u32buf[4];
        uint32_t max_rows;
        uint32_t col_count;
        uint32_t row_count;
        uint32_t j;

        err = microdb_storage_read_bytes(core, offset, &name_len, 1u);
        if (err != MICRODB_OK || name_len >= MICRODB_REL_TABLE_NAME_LEN) {
            return MICRODB_OK;
        }
        offset += 1u;

        err = microdb_storage_read_bytes(core, offset, table_name, name_len);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        table_name[name_len] = '\0';
        offset += name_len;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        max_rows = microdb_get_u32(u32buf);
        offset += 4u;

        offset += 4u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        col_count = microdb_get_u32(u32buf);
        offset += 4u;

        offset += 4u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_OK;
        }
        row_count = microdb_get_u32(u32buf);
        offset += 4u;

        err = microdb_schema_init(&schema, table_name, max_rows);
        if (err != MICRODB_OK) {
            return err;
        }

        for (j = 0u; j < col_count; ++j) {
            uint8_t col_name_len = 0u;
            char col_name[MICRODB_REL_COL_NAME_LEN];
            uint8_t meta[2];
            uint32_t col_size;

            err = microdb_storage_read_bytes(core, offset, &col_name_len, 1u);
            if (err != MICRODB_OK || col_name_len >= MICRODB_REL_COL_NAME_LEN) {
                return MICRODB_OK;
            }
            offset += 1u;

            err = microdb_storage_read_bytes(core, offset, col_name, col_name_len);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            col_name[col_name_len] = '\0';
            offset += col_name_len;

            err = microdb_storage_read_bytes(core, offset, meta, 2u);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            offset += 2u;

            err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            col_size = microdb_get_u32(u32buf);
            offset += 4u;

            err = microdb_schema_add(&schema,
                                     col_name,
                                     (microdb_col_type_t)meta[0],
                                     col_size,
                                     meta[1] != 0u);
            if (err != MICRODB_OK) {
                return err;
            }
        }

        err = microdb_schema_seal(&schema);
        if (err != MICRODB_OK) {
            return err;
        }
        err = microdb_table_create(db, &schema);
        if (err != MICRODB_OK && err != MICRODB_ERR_EXISTS) {
            return err;
        }
        err = microdb_table_get(db, table_name, &table);
        if (err != MICRODB_OK) {
            return err;
        }

        for (j = 0u; j < row_count; ++j) {
            uint8_t row_buf[1024];
            if (table->row_size > sizeof(row_buf)) {
                return MICRODB_ERR_SCHEMA;
            }
            err = microdb_storage_read_bytes(core, offset, row_buf, table->row_size);
            if (err != MICRODB_OK) {
                return MICRODB_OK;
            }
            offset += (uint32_t)table->row_size;
            err = microdb_rel_insert(db, table, row_buf);
            if (err != MICRODB_OK) {
                return err;
            }
        }
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_apply_wal_entry(microdb_t *db,
                                             uint8_t engine,
                                             uint8_t op,
                                             const uint8_t *data,
                                             uint16_t data_len) {
    if (engine == MICRODB_WAL_ENGINE_KV) {
        if (op == MICRODB_WAL_OP_SET_INSERT) {
            uint8_t key_len;
            char key[MICRODB_KV_KEY_MAX_LEN];
            uint32_t val_len;
            uint32_t expires_at;
            const uint8_t *val;

            if (data_len < 1u) {
                return MICRODB_OK;
            }
            key_len = data[0];
            if ((uint32_t)key_len >= MICRODB_KV_KEY_MAX_LEN || data_len < (uint16_t)(1u + key_len + 8u)) {
                return MICRODB_OK;
            }
            memcpy(key, data + 1u, key_len);
            key[key_len] = '\0';
            val_len = microdb_get_u32(data + 1u + key_len);
            if (val_len > MICRODB_KV_VAL_MAX_LEN ||
                data_len < (uint16_t)(1u + key_len + 4u + val_len + 4u)) {
                return MICRODB_OK;
            }
            val = data + 1u + key_len + 4u;
            expires_at = microdb_get_u32(val + val_len);
            return microdb_kv_set_at(db, key, val, val_len, expires_at);
        }
        if (op == MICRODB_WAL_OP_DEL) {
            uint8_t key_len;
            char key[MICRODB_KV_KEY_MAX_LEN];

            if (data_len < 1u) {
                return MICRODB_OK;
            }
            key_len = data[0];
            if ((uint32_t)key_len >= MICRODB_KV_KEY_MAX_LEN || data_len < (uint16_t)(1u + key_len)) {
                return MICRODB_OK;
            }
            memcpy(key, data + 1u, key_len);
            key[key_len] = '\0';
            (void)microdb_kv_del(db, key);
            return MICRODB_OK;
        }
        if (op == MICRODB_WAL_OP_CLEAR) {
            return microdb_kv_clear(db);
        }
    } else if (engine == MICRODB_WAL_ENGINE_TS && op == MICRODB_WAL_OP_SET_INSERT) {
        uint8_t name_len;
        char name[MICRODB_TS_STREAM_NAME_LEN];
        uint32_t ts_low;
        uint32_t ts_high;
        uint8_t type_byte;
        uint32_t value_len;
        uint64_t ts;
        microdb_err_t err;

        if (data_len < 1u) {
            return MICRODB_OK;
        }
        name_len = data[0];
        if ((uint32_t)name_len >= MICRODB_TS_STREAM_NAME_LEN || data_len < (uint16_t)(1u + name_len + 9u)) {
            return MICRODB_OK;
        }
        memcpy(name, data + 1u, name_len);
        name[name_len] = '\0';
        ts_low = microdb_get_u32(data + 1u + name_len);
        ts_high = microdb_get_u32(data + 1u + name_len + 4u);
        type_byte = data[1u + name_len + 8u];
        value_len = (uint32_t)data_len - (1u + name_len + 9u);
        err = microdb_ts_register(db, name, (microdb_ts_type_t)type_byte, value_len);
        if (err != MICRODB_OK && err != MICRODB_ERR_EXISTS) {
            return err;
        }
        ts = ((uint64_t)ts_high << 32u) | ts_low;
        return microdb_ts_insert(db, name, (microdb_timestamp_t)ts, data + 1u + name_len + 9u);
    } else if (engine == MICRODB_WAL_ENGINE_REL) {
        if (op == MICRODB_WAL_OP_SET_INSERT) {
            uint8_t name_len;
            char table_name[MICRODB_REL_TABLE_NAME_LEN];
            uint32_t row_size;
            microdb_table_t *table = NULL;

            if (data_len < 1u) {
                return MICRODB_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= MICRODB_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len + 4u)) {
                return MICRODB_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            row_size = microdb_get_u32(data + 1u + name_len);
            if (data_len < (uint16_t)(1u + name_len + 4u + row_size)) {
                return MICRODB_OK;
            }
            if (microdb_table_get(db, table_name, &table) != MICRODB_OK || table->row_size != row_size) {
                return MICRODB_OK;
            }
            return microdb_rel_insert(db, table, data + 1u + name_len + 4u);
        }
        if (op == MICRODB_WAL_OP_DEL) {
            uint8_t name_len;
            char table_name[MICRODB_REL_TABLE_NAME_LEN];
            microdb_table_t *table = NULL;

            if (data_len < 1u) {
                return MICRODB_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= MICRODB_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len)) {
                return MICRODB_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            if (microdb_table_get(db, table_name, &table) != MICRODB_OK || table->index_key_size == 0u) {
                return MICRODB_OK;
            }
            (void)microdb_rel_delete(db, table, data + 1u + name_len, NULL);
            return MICRODB_OK;
        }
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_replay_wal(microdb_t *db, bool *out_had_entries, bool *out_header_reset) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[32];
    uint32_t stored_crc;
    uint32_t entry_count;
    uint32_t block_seq;
    uint32_t offset = core->layout.wal_offset + 32u;
    uint32_t i;
    uint32_t replayed_count = 0u;
    microdb_err_t err;

    *out_had_entries = false;
    *out_header_reset = false;

    err = microdb_storage_read_bytes(core, core->layout.wal_offset, header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }

    if (microdb_get_u32(header + 0u) != MICRODB_WAL_MAGIC) {
        MICRODB_LOG("ERROR", "WAL header corrupt: resetting WAL");
        *out_header_reset = true;
        return MICRODB_OK;
    }

    stored_crc = microdb_get_u32(header + 16u);
    if (MICRODB_CRC32(header, 16u) != stored_crc) {
        MICRODB_LOG("ERROR", "WAL header corrupt: resetting WAL");
        *out_header_reset = true;
        return MICRODB_OK;
    }

    entry_count = microdb_get_u32(header + 8u);
    block_seq = microdb_get_u32(header + 12u);
    core->wal_sequence = block_seq;

    if (entry_count == 0u) {
        core->wal_used = 32u;
        return MICRODB_OK;
    }

    *out_had_entries = true;
    core->wal_replaying = true;
    for (i = 0u; i < entry_count; ++i) {
        uint8_t entry_header[16];
        uint8_t payload[1536];
        uint32_t entry_crc;
        uint16_t data_len;
        uint32_t aligned_len;
        uint32_t crc;

        err = microdb_storage_read_bytes(core, offset, entry_header, sizeof(entry_header));
        if (err != MICRODB_OK || microdb_get_u32(entry_header + 0u) != MICRODB_WAL_ENTRY_MAGIC) {
            break;
        }

        data_len = microdb_get_u16(entry_header + 10u);
        aligned_len = microdb_align_u32(data_len, 4u);
        if (data_len > sizeof(payload) || offset + 16u + aligned_len > core->layout.wal_offset + core->layout.wal_size) {
            break;
        }

        err = microdb_storage_read_bytes(core, offset + 16u, payload, aligned_len);
        if (err != MICRODB_OK) {
            break;
        }

        entry_crc = microdb_get_u32(entry_header + 12u);
        crc = MICRODB_CRC32(entry_header, 12u);
        crc = microdb_crc32(crc, payload, data_len);
        if (crc != entry_crc) {
            MICRODB_LOG("ERROR",
                        "WAL corrupt entry at seq=%u: CRC mismatch, stopping replay",
                        (unsigned)microdb_get_u32(entry_header + 4u));
            break;
        }

        err = microdb_apply_wal_entry(db, entry_header[8], entry_header[9], payload, data_len);
        if (err != MICRODB_OK) {
            core->wal_replaying = false;
            return err;
        }

        offset += 16u + aligned_len;
        replayed_count++;
    }
    core->wal_replaying = false;
    core->wal_used = offset - core->layout.wal_offset;
    MICRODB_LOG("INFO",
                "WAL recovery complete: replayed %u entries",
                (unsigned)replayed_count);
    return MICRODB_OK;
}

microdb_err_t microdb_storage_bootstrap(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    microdb_err_t err;
    bool had_entries = false;
    bool reset_header = false;
    uint32_t erase_size;

    memset(&core->layout, 0, sizeof(core->layout));
    core->wal_sequence = 0u;
    core->wal_entry_count = 0u;
    core->wal_used = 32u;

    if (!microdb_storage_ready(core)) {
        return MICRODB_OK;
    }

    erase_size = core->storage->erase_size;
    core->layout.wal_offset = 0u;
    core->layout.wal_size = erase_size * 2u;
    core->layout.kv_offset = core->layout.wal_size;
    core->layout.kv_size = microdb_align_u32((uint32_t)core->kv_arena.capacity, erase_size);
    core->layout.ts_offset = core->layout.kv_offset + core->layout.kv_size;
    core->layout.ts_size = microdb_align_u32((uint32_t)core->ts_arena.capacity, erase_size);
    core->layout.rel_offset = core->layout.ts_offset + core->layout.ts_size;
    core->layout.rel_size = microdb_align_u32((uint32_t)core->rel_arena.capacity, erase_size);
    core->layout.total_size = core->layout.rel_offset + core->layout.rel_size;

    if (core->storage->capacity < core->layout.total_size) {
        return MICRODB_ERR_STORAGE;
    }

    core->storage_loading = true;
    err = microdb_load_kv_page(db);
    if (err == MICRODB_OK) {
        err = microdb_load_ts_page(db);
    }
    if (err == MICRODB_OK) {
        err = microdb_load_rel_page(db);
    }
    core->storage_loading = false;
    if (err != MICRODB_OK) {
        return err;
    }

    if (!core->wal_enabled) {
        return MICRODB_OK;
    }

    err = microdb_replay_wal(db, &had_entries, &reset_header);
    if (err != MICRODB_OK) {
        return err;
    }

    if (had_entries || reset_header) {
        return microdb_storage_flush(db);
    }

    return microdb_reset_wal(core, core->wal_sequence);
}

static microdb_err_t microdb_append_wal_entry(microdb_t *db,
                                              uint8_t engine,
                                              uint8_t op,
                                              const uint8_t *payload,
                                              uint16_t payload_len) {
    microdb_core_t *core = microdb_core(db);
    uint32_t aligned_len = microdb_align_u32(payload_len, 4u);
    uint32_t entry_len = 16u + aligned_len;
    uint8_t entry[1552];
    uint32_t crc;
    microdb_err_t err;

    if (entry_len > sizeof(entry)) {
        return MICRODB_ERR_STORAGE;
    }

    if (core->wal_used + entry_len > core->layout.wal_size) {
        err = microdb_storage_flush(db);
        if (err != MICRODB_OK) {
            return err;
        }
    }

    if (core->wal_used + entry_len > core->layout.wal_size) {
        return MICRODB_ERR_STORAGE;
    }

    memset(entry, 0, sizeof(entry));
    microdb_put_u32(entry + 0u, MICRODB_WAL_ENTRY_MAGIC);
    microdb_put_u32(entry + 4u, core->wal_entry_count + 1u);
    entry[8] = engine;
    entry[9] = op;
    microdb_put_u16(entry + 10u, payload_len);
    if (payload_len != 0u) {
        memcpy(entry + 16u, payload, payload_len);
    }
    crc = MICRODB_CRC32(entry, 12u);
    crc = microdb_crc32(crc, entry + 16u, payload_len);
    microdb_put_u32(entry + 12u, crc);

    err = microdb_storage_write_bytes(core, core->layout.wal_offset + core->wal_used, entry, entry_len);
    if (err != MICRODB_OK) {
        return err;
    }

    core->wal_used += entry_len;
    core->wal_entry_count++;
    err = microdb_write_wal_header(core);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_storage_sync_core(core);
    if (err != MICRODB_OK) {
        return err;
    }

    if (core->wal_used * 4u > core->layout.wal_size * 3u) {
        return microdb_storage_flush(db);
    }

    return MICRODB_OK;
}

microdb_err_t microdb_storage_flush(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    microdb_err_t err;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }

    if (core->wal_enabled) {
        MICRODB_LOG("INFO",
                    "WAL compaction triggered: entry_count=%u",
                    (unsigned)core->wal_entry_count);
    }

    err = microdb_write_kv_page(core);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_write_ts_page(core);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_write_rel_page(core);
    if (err != MICRODB_OK) {
        return err;
    }

    if (core->wal_enabled) {
        return microdb_reset_wal(core, core->wal_sequence + 1u);
    }

    return microdb_storage_sync_core(core);
}

microdb_err_t microdb_persist_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[256];
    size_t key_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    key_len = strlen(key);
    payload[0] = (uint8_t)key_len;
    memcpy(payload + 1u, key, key_len);
    microdb_put_u32(payload + 1u + key_len, (uint32_t)len);
    memcpy(payload + 1u + key_len + 4u, val, len);
    microdb_put_u32(payload + 1u + key_len + 4u + len, expires_at);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_KV,
                                    MICRODB_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + key_len + 4u + len + 4u));
}

microdb_err_t microdb_persist_kv_del(microdb_t *db, const char *key) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[MICRODB_KV_KEY_MAX_LEN];
    size_t key_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    key_len = strlen(key);
    payload[0] = (uint8_t)key_len;
    memcpy(payload + 1u, key, key_len);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_KV,
                                    MICRODB_WAL_OP_DEL,
                                    payload,
                                    (uint16_t)(1u + key_len));
}

microdb_err_t microdb_persist_kv_clear(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    return microdb_append_wal_entry(db, MICRODB_WAL_ENGINE_KV, MICRODB_WAL_OP_CLEAR, NULL, 0u);
}

microdb_err_t microdb_persist_ts_insert(microdb_t *db, const char *name, microdb_timestamp_t ts, const void *val, size_t val_len) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[256];
    size_t idx;
    size_t name_len;
    uint64_t full_ts = (uint64_t)ts;
    microdb_ts_stream_t *stream = NULL;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    for (idx = 0u; idx < MICRODB_TS_MAX_STREAMS; ++idx) {
        if (core->ts.streams[idx].registered && strcmp(core->ts.streams[idx].name, name) == 0) {
            stream = &core->ts.streams[idx];
            break;
        }
    }
    if (stream == NULL) {
        return MICRODB_ERR_NOT_FOUND;
    }

    name_len = strlen(name);
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, name, name_len);
    microdb_put_u32(payload + 1u + name_len, (uint32_t)(full_ts & 0xFFFFFFFFu));
    microdb_put_u32(payload + 1u + name_len + 4u, (uint32_t)(full_ts >> 32u));
    payload[1u + name_len + 8u] = (uint8_t)stream->type;
    memcpy(payload + 1u + name_len + 9u, val, val_len);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_TS,
                                    MICRODB_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + name_len + 9u + val_len));
}

microdb_err_t microdb_persist_rel_insert(microdb_t *db, const microdb_table_t *table, const void *row_buf) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[1536];
    size_t name_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }
    if (table->row_size + MICRODB_REL_TABLE_NAME_LEN + 5u > sizeof(payload)) {
        return MICRODB_ERR_STORAGE;
    }

    name_len = strlen(table->name);
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, table->name, name_len);
    microdb_put_u32(payload + 1u + name_len, (uint32_t)table->row_size);
    memcpy(payload + 1u + name_len + 4u, row_buf, table->row_size);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_REL,
                                    MICRODB_WAL_OP_SET_INSERT,
                                    payload,
                                    (uint16_t)(1u + name_len + 4u + table->row_size));
}

microdb_err_t microdb_persist_rel_delete(microdb_t *db, const microdb_table_t *table, const void *search_val) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[64];
    size_t name_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    name_len = strlen(table->name);
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, table->name, name_len);
    memcpy(payload + 1u + name_len, search_val, table->index_key_size);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_REL,
                                    MICRODB_WAL_OP_DEL,
                                    payload,
                                    (uint16_t)(1u + name_len + table->index_key_size));
}
