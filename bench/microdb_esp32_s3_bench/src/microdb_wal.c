// SPDX-License-Identifier: MIT
#include "microdb_internal.h"
#include "microdb_crc.h"
#include "microdb_arena.h"
#include "microdb_lock.h"

#include <string.h>

enum {
    MICRODB_WAL_MAGIC = 0x4D44424Cu,
    MICRODB_WAL_VERSION = 0x00010000u,
    MICRODB_SNAPSHOT_FORMAT_VERSION = 0x00020000u,
    MICRODB_WAL_ENTRY_MAGIC = 0x454E5452u,
    MICRODB_KV_PAGE_MAGIC = 0x4B565047u,
    MICRODB_TS_PAGE_MAGIC = 0x54535047u,
    MICRODB_REL_PAGE_MAGIC = 0x524C5047u,
    MICRODB_SUPER_MAGIC = 0x53555052u,
    MICRODB_WAL_ENGINE_KV = 0u,
    MICRODB_WAL_ENGINE_TS = 1u,
    MICRODB_WAL_ENGINE_REL = 2u,
    MICRODB_WAL_ENGINE_TXN_KV = 3u,
    MICRODB_WAL_ENGINE_META = 0xFFu,
    MICRODB_WAL_OP_SET_INSERT = 0u,
    MICRODB_WAL_OP_DEL = 1u,
    MICRODB_WAL_OP_CLEAR = 2u,
    MICRODB_WAL_OP_TXN_COMMIT = 5u,
    MICRODB_WAL_OP_TS_REGISTER = 6u,
    MICRODB_WAL_OP_REL_TABLE_CREATE = 7u
};

#define MICRODB_WAL_HEADER_SIZE 32u
#define MICRODB_PAGE_HEADER_SIZE 32u
#define MICRODB_SUPERBLOCK_SIZE 32u

static uint32_t microdb_align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static uint32_t microdb_kv_snapshot_payload_max(const microdb_core_t *core) {
    uint32_t max_entries;
    uint32_t max_key_len = (MICRODB_KV_KEY_MAX_LEN > 0u) ? (MICRODB_KV_KEY_MAX_LEN - 1u) : 0u;
    uint32_t per_entry = 1u + max_key_len + 4u + MICRODB_KV_VAL_MAX_LEN + 4u;
    (void)core;
    max_entries = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    return max_entries * per_entry;
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

static uint32_t microdb_bank_kv_offset(const microdb_core_t *core, uint32_t bank) {
    return ((bank == 0u) ? core->layout.bank_a_offset : core->layout.bank_b_offset);
}

static uint32_t microdb_bank_ts_offset(const microdb_core_t *core, uint32_t bank) {
    return microdb_bank_kv_offset(core, bank) + core->layout.kv_size;
}

static uint32_t microdb_bank_rel_offset(const microdb_core_t *core, uint32_t bank) {
    return microdb_bank_ts_offset(core, bank) + core->layout.ts_size;
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
    uint8_t header[MICRODB_WAL_HEADER_SIZE];
    uint32_t crc;

    memset(header, 0, sizeof(header));
    microdb_put_u32(header + 0u, MICRODB_WAL_MAGIC);
    microdb_put_u32(header + 4u, MICRODB_WAL_VERSION);
    microdb_put_u32(header + 8u, core->wal_entry_count);
    microdb_put_u32(header + 12u, core->wal_sequence);
    crc = MICRODB_CRC32(header, 16u);
    microdb_put_u32(header + 16u, crc);
    return microdb_storage_write_bytes(core, core->layout.wal_offset, header, MICRODB_WAL_HEADER_SIZE);
}

static microdb_err_t microdb_reset_wal(microdb_core_t *core, uint32_t next_sequence) {
    microdb_err_t err;

    core->wal_sequence = next_sequence;
    core->wal_entry_count = 0u;
    core->wal_used = MICRODB_WAL_HEADER_SIZE;

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

static microdb_err_t microdb_write_page_header(microdb_core_t *core,
                                               uint32_t offset,
                                               uint32_t magic,
                                               uint32_t generation,
                                               uint32_t payload_length,
                                               uint32_t entry_count,
                                               uint32_t payload_crc) {
    uint8_t header[MICRODB_PAGE_HEADER_SIZE];
    uint32_t header_crc;

    memset(header, 0, sizeof(header));
    microdb_put_u32(header + 0u, magic);
    microdb_put_u32(header + 4u, MICRODB_SNAPSHOT_FORMAT_VERSION);
    microdb_put_u32(header + 8u, generation);
    microdb_put_u32(header + 12u, payload_length);
    microdb_put_u32(header + 16u, entry_count);
    microdb_put_u32(header + 20u, payload_crc);
    header_crc = MICRODB_CRC32(header, 24u);
    microdb_put_u32(header + 24u, header_crc);
    return microdb_storage_write_bytes(core, offset, header, sizeof(header));
}

static bool microdb_validate_page_header(const uint8_t *header,
                                         uint32_t expected_magic,
                                         uint32_t max_payload_len,
                                         uint32_t *out_generation,
                                         uint32_t *out_payload_len,
                                         uint32_t *out_entry_count,
                                         uint32_t *out_payload_crc) {
    uint32_t header_crc = microdb_get_u32(header + 24u);
    uint32_t payload_len = microdb_get_u32(header + 12u);

    if (microdb_get_u32(header + 0u) != expected_magic) {
        return false;
    }
    if (microdb_get_u32(header + 4u) != MICRODB_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (MICRODB_CRC32(header, 24u) != header_crc) {
        return false;
    }
    if (payload_len > max_payload_len) {
        return false;
    }
    *out_generation = microdb_get_u32(header + 8u);
    *out_payload_len = payload_len;
    *out_entry_count = microdb_get_u32(header + 16u);
    *out_payload_crc = microdb_get_u32(header + 20u);
    return true;
}

static bool microdb_validate_superblock(const uint8_t *super,
                                        uint32_t *out_generation,
                                        uint32_t *out_active_bank) {
    uint32_t header_crc = microdb_get_u32(super + 20u);
    if (microdb_get_u32(super + 0u) != MICRODB_SUPER_MAGIC) {
        return false;
    }
    if (microdb_get_u32(super + 4u) != MICRODB_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (MICRODB_CRC32(super, 20u) != header_crc) {
        return false;
    }
    if (microdb_get_u32(super + 16u) > 1u) {
        return false;
    }
    *out_generation = microdb_get_u32(super + 12u);
    *out_active_bank = microdb_get_u32(super + 16u);
    return true;
}

static microdb_err_t microdb_write_superblock(microdb_core_t *core, uint32_t generation, uint32_t active_bank) {
    uint8_t super[MICRODB_SUPERBLOCK_SIZE];
    uint32_t header_crc;
    uint32_t offset;

    memset(super, 0, sizeof(super));
    microdb_put_u32(super + 0u, MICRODB_SUPER_MAGIC);
    microdb_put_u32(super + 4u, MICRODB_SNAPSHOT_FORMAT_VERSION);
    microdb_put_u32(super + 8u, MICRODB_WAL_VERSION);
    microdb_put_u32(super + 12u, generation);
    microdb_put_u32(super + 16u, active_bank);
    header_crc = MICRODB_CRC32(super, 20u);
    microdb_put_u32(super + 20u, header_crc);

    offset = (generation & 1u) == 0u ? core->layout.super_b_offset : core->layout.super_a_offset;
    return microdb_storage_write_bytes(core, offset, super, sizeof(super));
}

static microdb_err_t microdb_write_kv_page(microdb_core_t *core, uint32_t bank, uint32_t generation) {
    uint32_t count = 0u;
    uint32_t page_offset = microdb_bank_kv_offset(core, bank);
    uint32_t offset = page_offset + MICRODB_PAGE_HEADER_SIZE;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t max_end = page_offset + core->layout.kv_size;
    uint32_t i;
    microdb_err_t err;

    for (i = 0u; i < core->kv.bucket_count; ++i) {
        const microdb_kv_bucket_t *bucket = &core->kv.buckets[i];
        uint8_t key_len;
        uint8_t header[4];

        if (bucket->state != 1u) {
            continue;
        }

        key_len = (uint8_t)strlen(bucket->key);
        if (offset + 1u + key_len + 4u + bucket->val_len + 4u > max_end) {
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

    return microdb_write_page_header(core,
                                     page_offset,
                                     MICRODB_KV_PAGE_MAGIC,
                                     generation,
                                     offset - (page_offset + MICRODB_PAGE_HEADER_SIZE),
                                     count,
                                     crc);
}

static uint32_t microdb_ts_stream_val_size(const microdb_ts_stream_t *stream) {
    return (stream->type == MICRODB_TS_RAW) ? (uint32_t)stream->raw_size : 4u;
}

static microdb_err_t microdb_write_ts_page(microdb_core_t *core, uint32_t bank, uint32_t generation) {
    uint32_t stream_count = 0u;
    uint32_t page_offset = microdb_bank_ts_offset(core, bank);
    uint32_t offset = page_offset + MICRODB_PAGE_HEADER_SIZE;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t max_end = page_offset + core->layout.ts_size;
    uint32_t i;
    microdb_err_t err;

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
        if (offset + 1u + name_len + 1u + 4u + 4u > max_end) {
            return MICRODB_ERR_STORAGE;
        }
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

            if (offset + 8u + val_len > max_end) {
                return MICRODB_ERR_STORAGE;
            }
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

    return microdb_write_page_header(core,
                                     page_offset,
                                     MICRODB_TS_PAGE_MAGIC,
                                     generation,
                                     offset - (page_offset + MICRODB_PAGE_HEADER_SIZE),
                                     stream_count,
                                     crc);
}

static microdb_err_t microdb_write_rel_page(microdb_core_t *core, uint32_t bank, uint32_t generation) {
    uint32_t table_count = 0u;
    uint32_t page_offset = microdb_bank_rel_offset(core, bank);
    uint32_t offset = page_offset + MICRODB_PAGE_HEADER_SIZE;
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t max_end = page_offset + core->layout.rel_size;
    uint32_t i;
    microdb_err_t err;

    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        const microdb_table_t *table = &core->rel.tables[i];
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
            return MICRODB_ERR_STORAGE;
        }
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

        microdb_put_u16(u16buf, table->schema_version);
        err = microdb_storage_write_bytes(core, offset, u16buf, 2u);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, u16buf, 2u);
        offset += 2u;

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

            if (offset + 1u + col_name_len + 2u + 4u > max_end) {
                return MICRODB_ERR_STORAGE;
            }
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
            if (offset + (uint32_t)table->row_size > max_end) {
                return MICRODB_ERR_STORAGE;
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

    return microdb_write_page_header(core,
                                     page_offset,
                                     MICRODB_REL_PAGE_MAGIC,
                                     generation,
                                     offset - (page_offset + MICRODB_PAGE_HEADER_SIZE),
                                     table_count,
                                     crc);
}

static microdb_err_t microdb_write_snapshot_bank(microdb_core_t *core, uint32_t bank, uint32_t generation) {
    microdb_err_t err;
    uint32_t bank_offset = (bank == 0u) ? core->layout.bank_a_offset : core->layout.bank_b_offset;

    err = microdb_storage_erase_region(core, bank_offset, core->layout.bank_size);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_write_kv_page(core, bank, generation);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_write_ts_page(core, bank, generation);
    if (err != MICRODB_OK) {
        return err;
    }
    return microdb_write_rel_page(core, bank, generation);
}

static microdb_err_t microdb_load_kv_page(microdb_t *db, uint32_t bank, uint32_t expected_generation) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[MICRODB_PAGE_HEADER_SIZE];
    uint32_t page_offset = microdb_bank_kv_offset(core, bank);
    uint32_t generation = 0u;
    uint32_t payload_len = 0u;
    uint32_t payload_crc = 0u;
    uint32_t offset;
    uint32_t payload_offset;
    uint32_t count;
    uint32_t payload_crc_calc = 0xFFFFFFFFu;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, page_offset, header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }
    if (!microdb_validate_page_header(header,
                                      MICRODB_KV_PAGE_MAGIC,
                                      core->layout.kv_size - MICRODB_PAGE_HEADER_SIZE,
                                      &generation,
                                      &payload_len,
                                      &count,
                                      &payload_crc)) {
        return MICRODB_ERR_CORRUPT;
    }
    if (generation != expected_generation) {
        return MICRODB_ERR_CORRUPT;
    }

    offset = page_offset + MICRODB_PAGE_HEADER_SIZE;
    payload_offset = offset;
    for (i = 0u; i < count; ++i) {
        uint8_t key_len = 0u;
        char key[MICRODB_KV_KEY_MAX_LEN];
        uint8_t u32buf[4];
        uint32_t val_len;
        uint32_t expires_at;
        uint8_t value[MICRODB_KV_VAL_MAX_LEN];

        if (offset + 1u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, &key_len, 1u);
        if (err != MICRODB_OK || key_len >= MICRODB_KV_KEY_MAX_LEN) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, &key_len, 1u);
        offset += 1u;

        if (offset + key_len > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, key, key_len);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, (const uint8_t *)key, key_len);
        key[key_len] = '\0';
        offset += key_len;

        if (offset + 4u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        val_len = microdb_get_u32(u32buf);
        offset += 4u;
        if (val_len > MICRODB_KV_VAL_MAX_LEN) {
            return MICRODB_ERR_CORRUPT;
        }

        if (offset + val_len + 4u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, value, val_len);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, value, val_len);
        offset += val_len;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        expires_at = microdb_get_u32(u32buf);
        offset += 4u;

        err = microdb_kv_set_at(db, key, value, val_len, expires_at);
        if (err != MICRODB_OK) {
            return err;
        }
    }

    if (offset != payload_offset + payload_len) {
        return MICRODB_ERR_CORRUPT;
    }
    if (payload_crc_calc != payload_crc) {
        return MICRODB_ERR_CORRUPT;
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_load_ts_page(microdb_t *db, uint32_t bank, uint32_t expected_generation) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[MICRODB_PAGE_HEADER_SIZE];
    uint32_t page_offset = microdb_bank_ts_offset(core, bank);
    uint32_t generation = 0u;
    uint32_t payload_len = 0u;
    uint32_t payload_crc = 0u;
    uint32_t offset;
    uint32_t payload_offset;
    uint32_t stream_count;
    uint32_t payload_crc_calc = 0xFFFFFFFFu;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, page_offset, header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }
    if (!microdb_validate_page_header(header,
                                      MICRODB_TS_PAGE_MAGIC,
                                      core->layout.ts_size - MICRODB_PAGE_HEADER_SIZE,
                                      &generation,
                                      &payload_len,
                                      &stream_count,
                                      &payload_crc)) {
        return MICRODB_ERR_CORRUPT;
    }
    if (generation != expected_generation) {
        return MICRODB_ERR_CORRUPT;
    }

    offset = page_offset + MICRODB_PAGE_HEADER_SIZE;
    payload_offset = offset;
    for (i = 0u; i < stream_count; ++i) {
        uint8_t name_len = 0u;
        char name[MICRODB_TS_STREAM_NAME_LEN];
        uint8_t type_byte = 0u;
        uint8_t u32buf[4];
        uint32_t raw_size;
        uint32_t sample_count;
        uint32_t j;

        if (offset + 1u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, &name_len, 1u);
        if (err != MICRODB_OK || name_len >= MICRODB_TS_STREAM_NAME_LEN) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, &name_len, 1u);
        offset += 1u;

        if (offset + name_len + 1u + 8u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, name, name_len);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, (const uint8_t *)name, name_len);
        name[name_len] = '\0';
        offset += name_len;

        err = microdb_storage_read_bytes(core, offset, &type_byte, 1u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, &type_byte, 1u);
        offset += 1u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        raw_size = microdb_get_u32(u32buf);
        offset += 4u;
        if ((microdb_ts_type_t)type_byte == MICRODB_TS_RAW && (raw_size == 0u || raw_size > MICRODB_TS_RAW_MAX)) {
            return MICRODB_ERR_CORRUPT;
        }

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
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
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
            ts_low = microdb_get_u32(u32buf);
            offset += 4u;

            err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
            ts_high = microdb_get_u32(u32buf);
            offset += 4u;

            err = microdb_storage_read_bytes(core, offset, value, val_len);
            if (err != MICRODB_OK) {
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, value, val_len);
            offset += val_len;

            full_ts = ((uint64_t)ts_high << 32u) | ts_low;
            err = microdb_ts_insert(db, name, (microdb_timestamp_t)full_ts, value);
            if (err != MICRODB_OK) {
                return err;
            }
        }
    }

    if (offset != payload_offset + payload_len) {
        return MICRODB_ERR_CORRUPT;
    }
    if (payload_crc_calc != payload_crc) {
        return MICRODB_ERR_CORRUPT;
    }

    return MICRODB_OK;
}

static microdb_err_t microdb_load_rel_page(microdb_t *db, uint32_t bank, uint32_t expected_generation) {
    microdb_core_t *core = microdb_core(db);
    uint8_t header[MICRODB_PAGE_HEADER_SIZE];
    uint32_t page_offset = microdb_bank_rel_offset(core, bank);
    uint32_t generation = 0u;
    uint32_t payload_len = 0u;
    uint32_t payload_crc = 0u;
    uint32_t offset;
    uint32_t payload_offset;
    uint32_t table_count;
    uint32_t payload_crc_calc = 0xFFFFFFFFu;
    uint32_t i;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, page_offset, header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }
    if (!microdb_validate_page_header(header,
                                      MICRODB_REL_PAGE_MAGIC,
                                      core->layout.rel_size - MICRODB_PAGE_HEADER_SIZE,
                                      &generation,
                                      &payload_len,
                                      &table_count,
                                      &payload_crc)) {
        return MICRODB_ERR_CORRUPT;
    }
    if (generation != expected_generation) {
        return MICRODB_ERR_CORRUPT;
    }

    offset = page_offset + MICRODB_PAGE_HEADER_SIZE;
    payload_offset = offset;
    for (i = 0u; i < table_count; ++i) {
        microdb_schema_t schema;
        microdb_table_t *table = NULL;
        uint8_t name_len = 0u;
        char table_name[MICRODB_REL_TABLE_NAME_LEN];
        uint8_t u16buf[2];
        uint8_t u32buf[4];
        uint16_t schema_version;
        uint32_t max_rows;
        uint32_t col_count;
        uint32_t row_count;
        uint32_t j;

        if (offset + 1u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, &name_len, 1u);
        if (err != MICRODB_OK || name_len >= MICRODB_REL_TABLE_NAME_LEN) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, &name_len, 1u);
        offset += 1u;

        if (offset + name_len + 2u + 4u + 4u + 4u + 4u + 4u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, table_name, name_len);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, (const uint8_t *)table_name, name_len);
        table_name[name_len] = '\0';
        offset += name_len;

        err = microdb_storage_read_bytes(core, offset, u16buf, 2u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u16buf, 2u);
        schema_version = microdb_get_u16(u16buf);
        offset += 2u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        max_rows = microdb_get_u32(u32buf);
        offset += 4u;

        if (offset + 4u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        offset += 4u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        col_count = microdb_get_u32(u32buf);
        offset += 4u;
        if (col_count > MICRODB_REL_MAX_COLS) {
            return MICRODB_ERR_CORRUPT;
        }

        if (offset + 4u > payload_offset + payload_len) {
            return MICRODB_ERR_CORRUPT;
        }
        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        offset += 4u;

        err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
        if (err != MICRODB_OK) {
            return MICRODB_ERR_CORRUPT;
        }
        payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
        row_count = microdb_get_u32(u32buf);
        offset += 4u;
        if (row_count > max_rows) {
            return MICRODB_ERR_CORRUPT;
        }

        err = microdb_schema_init(&schema, table_name, max_rows);
        if (err != MICRODB_OK) {
            return err;
        }
        schema.schema_version = schema_version;

        for (j = 0u; j < col_count; ++j) {
            uint8_t col_name_len = 0u;
            char col_name[MICRODB_REL_COL_NAME_LEN];
            uint8_t meta[2];
            uint32_t col_size;

            err = microdb_storage_read_bytes(core, offset, &col_name_len, 1u);
            if (err != MICRODB_OK || col_name_len >= MICRODB_REL_COL_NAME_LEN) {
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, &col_name_len, 1u);
            offset += 1u;

            err = microdb_storage_read_bytes(core, offset, col_name, col_name_len);
            if (err != MICRODB_OK) {
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, (const uint8_t *)col_name, col_name_len);
            col_name[col_name_len] = '\0';
            offset += col_name_len;

            err = microdb_storage_read_bytes(core, offset, meta, 2u);
            if (err != MICRODB_OK) {
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, meta, 2u);
            offset += 2u;

            err = microdb_storage_read_bytes(core, offset, u32buf, 4u);
            if (err != MICRODB_OK) {
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, u32buf, 4u);
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
        if (err != MICRODB_OK) {
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
                return MICRODB_ERR_CORRUPT;
            }
            payload_crc_calc = microdb_crc32(payload_crc_calc, row_buf, table->row_size);
            offset += (uint32_t)table->row_size;
            err = microdb_rel_insert(db, table, row_buf);
            if (err != MICRODB_OK) {
                return err;
            }
        }
    }

    if (offset != payload_offset + payload_len) {
        return MICRODB_ERR_CORRUPT;
    }
    if (payload_crc_calc != payload_crc) {
        return MICRODB_ERR_CORRUPT;
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
    } else if (engine == MICRODB_WAL_ENGINE_TS) {
        if (op == MICRODB_WAL_OP_SET_INSERT) {
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
        }
        if (op == MICRODB_WAL_OP_TS_REGISTER) {
            uint8_t name_len;
            char name[MICRODB_TS_STREAM_NAME_LEN];
            uint8_t type_byte;
            uint32_t raw_size;
            if (data_len < 1u) {
                return MICRODB_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= MICRODB_TS_STREAM_NAME_LEN || data_len < (uint16_t)(1u + name_len + 5u)) {
                return MICRODB_OK;
            }
            memcpy(name, data + 1u, name_len);
            name[name_len] = '\0';
            type_byte = data[1u + name_len];
            raw_size = microdb_get_u32(data + 1u + name_len + 1u);
            return microdb_ts_register(db, name, (microdb_ts_type_t)type_byte, raw_size);
        }
        if (op == MICRODB_WAL_OP_CLEAR) {
            uint8_t name_len;
            char name[MICRODB_TS_STREAM_NAME_LEN];
            if (data_len < 1u) {
                return MICRODB_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= MICRODB_TS_STREAM_NAME_LEN || data_len < (uint16_t)(1u + name_len)) {
                return MICRODB_OK;
            }
            memcpy(name, data + 1u, name_len);
            name[name_len] = '\0';
            return microdb_ts_clear(db, name);
        }
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
        if (op == MICRODB_WAL_OP_CLEAR) {
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
            if (microdb_table_get(db, table_name, &table) != MICRODB_OK) {
                return MICRODB_OK;
            }
            return microdb_rel_clear(db, table);
        }
        if (op == MICRODB_WAL_OP_REL_TABLE_CREATE) {
            microdb_schema_t schema;
            uint8_t name_len;
            char table_name[MICRODB_REL_TABLE_NAME_LEN];
            uint16_t schema_version;
            uint32_t max_rows;
            uint32_t col_count;
            uint16_t off;
            uint32_t c;

            if (data_len < 1u) {
                return MICRODB_OK;
            }
            name_len = data[0];
            if ((uint32_t)name_len >= MICRODB_REL_TABLE_NAME_LEN || data_len < (uint16_t)(1u + name_len + 10u)) {
                return MICRODB_OK;
            }
            memcpy(table_name, data + 1u, name_len);
            table_name[name_len] = '\0';
            schema_version = microdb_get_u16(data + 1u + name_len);
            max_rows = microdb_get_u32(data + 1u + name_len + 2u);
            col_count = microdb_get_u32(data + 1u + name_len + 6u);
            off = (uint16_t)(1u + name_len + 10u);

            if (microdb_schema_init(&schema, table_name, max_rows) != MICRODB_OK) {
                return MICRODB_ERR_SCHEMA;
            }
            schema.schema_version = schema_version;
            for (c = 0u; c < col_count; ++c) {
                uint8_t col_name_len;
                char col_name[MICRODB_REL_COL_NAME_LEN];
                microdb_col_type_t type;
                bool is_index;
                uint32_t col_size;
                if (off + 1u > data_len) {
                    return MICRODB_OK;
                }
                col_name_len = data[off++];
                if ((uint32_t)col_name_len >= MICRODB_REL_COL_NAME_LEN || off + col_name_len + 6u > data_len) {
                    return MICRODB_OK;
                }
                memcpy(col_name, data + off, col_name_len);
                col_name[col_name_len] = '\0';
                off = (uint16_t)(off + col_name_len);
                type = (microdb_col_type_t)data[off++];
                is_index = data[off++] != 0u;
                col_size = microdb_get_u32(data + off);
                off = (uint16_t)(off + 4u);
                if (microdb_schema_add(&schema, col_name, type, col_size, is_index) != MICRODB_OK) {
                    return MICRODB_ERR_SCHEMA;
                }
            }
            if (microdb_schema_seal(&schema) != MICRODB_OK) {
                return MICRODB_ERR_SCHEMA;
            }
            return microdb_table_create(db, &schema);
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
    uint32_t offset = core->layout.wal_offset + MICRODB_WAL_HEADER_SIZE;
    uint32_t i;
    uint32_t replayed_count = 0u;
    uint8_t txn_ops[MICRODB_TXN_STAGE_KEYS];
    uint16_t txn_lens[MICRODB_TXN_STAGE_KEYS];
    uint8_t txn_payloads[MICRODB_TXN_STAGE_KEYS][256];
    uint32_t txn_count = 0u;
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
        core->wal_used = MICRODB_WAL_HEADER_SIZE;
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

        if (entry_header[8] == MICRODB_WAL_ENGINE_TXN_KV &&
            (entry_header[9] == MICRODB_WAL_OP_SET_INSERT || entry_header[9] == MICRODB_WAL_OP_DEL)) {
            if (txn_count < MICRODB_TXN_STAGE_KEYS && data_len <= sizeof(txn_payloads[0])) {
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
        if (entry_header[8] == MICRODB_WAL_ENGINE_META && entry_header[9] == MICRODB_WAL_OP_TXN_COMMIT) {
            uint32_t t;
            for (t = 0u; t < txn_count; ++t) {
                err = microdb_apply_wal_entry(db, MICRODB_WAL_ENGINE_KV, txn_ops[t], txn_payloads[t], txn_lens[t]);
                if (err != MICRODB_OK) {
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

static microdb_err_t microdb_crc_storage_region(microdb_core_t *core, uint32_t offset, uint32_t len, uint32_t *out_crc) {
    uint8_t chunk[128];
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t pos = 0u;
    microdb_err_t err;

    while (pos < len) {
        uint32_t take = len - pos;
        if (take > sizeof(chunk)) {
            take = sizeof(chunk);
        }
        err = microdb_storage_read_bytes(core, offset + pos, chunk, take);
        if (err != MICRODB_OK) {
            return err;
        }
        crc = microdb_crc32(crc, chunk, take);
        pos += take;
    }

    *out_crc = crc;
    return MICRODB_OK;
}

static microdb_err_t microdb_validate_bank_pages(microdb_core_t *core, uint32_t bank, uint32_t *out_generation) {
    uint8_t header[MICRODB_PAGE_HEADER_SIZE];
    uint32_t gen = 0u;
    uint32_t payload_len = 0u;
    uint32_t entry_count = 0u;
    uint32_t payload_crc = 0u;
    uint32_t calc_crc = 0u;
    microdb_err_t err;

    err = microdb_storage_read_bytes(core, microdb_bank_kv_offset(core, bank), header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }
    if (!microdb_validate_page_header(header,
                                      MICRODB_KV_PAGE_MAGIC,
                                      core->layout.kv_size - MICRODB_PAGE_HEADER_SIZE,
                                      &gen,
                                      &payload_len,
                                      &entry_count,
                                      &payload_crc)) {
        return MICRODB_ERR_CORRUPT;
    }
    (void)entry_count;
    err = microdb_crc_storage_region(core, microdb_bank_kv_offset(core, bank) + MICRODB_PAGE_HEADER_SIZE, payload_len, &calc_crc);
    if (err != MICRODB_OK || calc_crc != payload_crc) {
        return MICRODB_ERR_CORRUPT;
    }

    err = microdb_storage_read_bytes(core, microdb_bank_ts_offset(core, bank), header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }
    {
        uint32_t gen2 = 0u;
        if (!microdb_validate_page_header(header,
                                          MICRODB_TS_PAGE_MAGIC,
                                          core->layout.ts_size - MICRODB_PAGE_HEADER_SIZE,
                                          &gen2,
                                          &payload_len,
                                          &entry_count,
                                          &payload_crc)) {
            return MICRODB_ERR_CORRUPT;
        }
        if (gen2 != gen) {
            return MICRODB_ERR_CORRUPT;
        }
    }
    err = microdb_crc_storage_region(core, microdb_bank_ts_offset(core, bank) + MICRODB_PAGE_HEADER_SIZE, payload_len, &calc_crc);
    if (err != MICRODB_OK || calc_crc != payload_crc) {
        return MICRODB_ERR_CORRUPT;
    }

    err = microdb_storage_read_bytes(core, microdb_bank_rel_offset(core, bank), header, sizeof(header));
    if (err != MICRODB_OK) {
        return err;
    }
    {
        uint32_t gen3 = 0u;
        if (!microdb_validate_page_header(header,
                                          MICRODB_REL_PAGE_MAGIC,
                                          core->layout.rel_size - MICRODB_PAGE_HEADER_SIZE,
                                          &gen3,
                                          &payload_len,
                                          &entry_count,
                                          &payload_crc)) {
            return MICRODB_ERR_CORRUPT;
        }
        if (gen3 != gen) {
            return MICRODB_ERR_CORRUPT;
        }
    }
    err = microdb_crc_storage_region(core, microdb_bank_rel_offset(core, bank) + MICRODB_PAGE_HEADER_SIZE, payload_len, &calc_crc);
    if (err != MICRODB_OK || calc_crc != payload_crc) {
        return MICRODB_ERR_CORRUPT;
    }

    *out_generation = gen;
    return MICRODB_OK;
}

microdb_err_t microdb_storage_bootstrap(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    microdb_err_t err;
    bool had_entries = false;
    bool reset_header = false;
    bool super_a_valid = false;
    bool super_b_valid = false;
    uint8_t super_a[MICRODB_SUPERBLOCK_SIZE];
    uint8_t super_b[MICRODB_SUPERBLOCK_SIZE];
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
    core->wal_used = MICRODB_WAL_HEADER_SIZE;

    if (!microdb_storage_ready(core)) {
        return MICRODB_OK;
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
            microdb_align_u32(microdb_kv_snapshot_payload_max(core) + MICRODB_PAGE_HEADER_SIZE, erase_size);
        core->layout.ts_size = microdb_align_u32((uint32_t)core->ts_arena.capacity + MICRODB_PAGE_HEADER_SIZE, erase_size);
        core->layout.rel_size = microdb_align_u32((uint32_t)core->rel_arena.capacity + MICRODB_PAGE_HEADER_SIZE, erase_size);
        core->layout.bank_size = core->layout.kv_size + core->layout.ts_size + core->layout.rel_size;

        fixed_bytes = core->layout.super_size * 2u;
        need_without_wal = fixed_bytes + (core->layout.bank_size * 2u);
        if (core->storage->capacity < need_without_wal + wal_min) {
            return MICRODB_ERR_STORAGE;
        }

        max_wal = core->storage->capacity - need_without_wal;
        max_wal_aligned = (max_wal / erase_size) * erase_size;
        if (max_wal_aligned < wal_min) {
            return MICRODB_ERR_STORAGE;
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
        return MICRODB_ERR_STORAGE;
    }

    err = microdb_storage_read_bytes(core, core->layout.super_a_offset, super_a, sizeof(super_a));
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_storage_read_bytes(core, core->layout.super_b_offset, super_b, sizeof(super_b));
    if (err != MICRODB_OK) {
        return err;
    }

    super_a_valid = microdb_validate_superblock(super_a, &super_a_gen, &super_a_bank);
    super_b_valid = microdb_validate_superblock(super_b, &super_b_gen, &super_b_bank);

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
        if (microdb_validate_bank_pages(core, 0u, &fallback_gen_a) == MICRODB_OK) {
            fallback_a_valid = true;
        }
        if (microdb_validate_bank_pages(core, 1u, &fallback_gen_b) == MICRODB_OK) {
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
        core->storage_loading = true;
        err = microdb_load_kv_page(db, selected_bank, selected_gen);
        if (err == MICRODB_OK) {
            err = microdb_load_ts_page(db, selected_bank, selected_gen);
        }
        if (err == MICRODB_OK) {
            err = microdb_load_rel_page(db, selected_bank, selected_gen);
        }
        core->storage_loading = false;
        if (err != MICRODB_OK) {
            return err;
        }
        core->layout.active_bank = selected_bank;
        core->layout.active_generation = selected_gen;
    } else {
        uint8_t probe[16];
        bool virgin = true;
        err = microdb_storage_read_bytes(core, core->layout.super_a_offset, probe, sizeof(probe));
        if (err != MICRODB_OK) {
            return err;
        }
        for (uint32_t k = 0u; k < sizeof(probe); ++k) {
            if (probe[k] != 0xFFu) {
                virgin = false;
                break;
            }
        }
        if (virgin) {
            err = microdb_storage_read_bytes(core, core->layout.super_b_offset, probe, sizeof(probe));
            if (err != MICRODB_OK) {
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
            return MICRODB_ERR_CORRUPT;
        }
        /* Cold start: initialize first durable snapshot bank/superblock. */
        core->layout.active_bank = 0u;
        core->layout.active_generation = 1u;
        err = microdb_write_snapshot_bank(core, 0u, core->layout.active_generation);
        if (err != MICRODB_OK) {
            return err;
        }
        err = microdb_storage_sync_core(core);
        if (err != MICRODB_OK) {
            return err;
        }
        err = microdb_write_superblock(core, core->layout.active_generation, 0u);
        if (err != MICRODB_OK) {
            return err;
        }
        err = microdb_storage_sync_core(core);
        if (err != MICRODB_OK) {
            return err;
        }
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

    return MICRODB_OK;
}

static microdb_err_t microdb_compact_nolock(microdb_t *db) {
    microdb_core_t *core;
    microdb_err_t err;
    uint32_t next_bank;
    uint32_t next_generation;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }
    if (!microdb_storage_ready(core)) {
        return MICRODB_OK;
    }
    next_bank = (core->layout.active_bank == 0u) ? 1u : 0u;
    next_generation = core->layout.active_generation + 1u;
    err = microdb_write_snapshot_bank(core, next_bank, next_generation);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_storage_sync_core(core);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_write_superblock(core, next_generation, next_bank);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_storage_sync_core(core);
    if (err != MICRODB_OK) {
        return err;
    }
    core->layout.active_bank = next_bank;
    core->layout.active_generation = next_generation;
    if (core->wal_enabled) {
        return microdb_reset_wal(core, core->wal_sequence + 1u);
    }
    return MICRODB_OK;
}

microdb_err_t microdb_compact(microdb_t *db) {
    microdb_err_t rc;
    microdb_core_t *core;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        MICRODB_UNLOCK(db);
        return MICRODB_ERR_INVALID;
    }
    rc = microdb_compact_nolock(db);
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_storage_flush(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }

    if (core->wal_enabled) {
        MICRODB_LOG("INFO",
                    "WAL compaction triggered: entry_count=%u",
                    (unsigned)core->wal_entry_count);
    }

    return microdb_compact_nolock(db);
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

microdb_err_t microdb_persist_kv_set_txn(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at) {
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
                                    MICRODB_WAL_ENGINE_TXN_KV,
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

microdb_err_t microdb_persist_kv_del_txn(microdb_t *db, const char *key) {
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
                                    MICRODB_WAL_ENGINE_TXN_KV,
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

microdb_err_t microdb_persist_txn_commit(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    return microdb_append_wal_entry(db, MICRODB_WAL_ENGINE_META, MICRODB_WAL_OP_TXN_COMMIT, NULL, 0u);
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

microdb_err_t microdb_persist_ts_register(microdb_t *db, const char *name, microdb_ts_type_t type, size_t raw_size) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[64];
    size_t name_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    name_len = strlen(name);
    if (name_len + 6u > sizeof(payload)) {
        return MICRODB_ERR_INVALID;
    }
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, name, name_len);
    payload[1u + name_len] = (uint8_t)type;
    microdb_put_u32(payload + 1u + name_len + 1u, (uint32_t)raw_size);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_TS,
                                    MICRODB_WAL_OP_TS_REGISTER,
                                    payload,
                                    (uint16_t)(1u + name_len + 1u + 4u));
}

microdb_err_t microdb_persist_ts_clear(microdb_t *db, const char *name) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[MICRODB_TS_STREAM_NAME_LEN];
    size_t name_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }

    name_len = strlen(name);
    if (name_len + 1u > sizeof(payload)) {
        return MICRODB_ERR_INVALID;
    }
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, name, name_len);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_TS,
                                    MICRODB_WAL_OP_CLEAR,
                                    payload,
                                    (uint16_t)(1u + name_len));
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

microdb_err_t microdb_persist_rel_table_create(microdb_t *db, const microdb_schema_t *schema) {
    microdb_core_t *core = microdb_core(db);
    const microdb_schema_impl_t *impl;
    uint8_t payload[512];
    uint32_t i;
    uint16_t off = 0u;
    size_t name_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }
    if (schema == NULL) {
        return MICRODB_ERR_INVALID;
    }

    impl = (const microdb_schema_impl_t *)&schema->_opaque[0];
    if (!impl->sealed) {
        return MICRODB_ERR_INVALID;
    }

    name_len = strlen(impl->name);
    if (name_len >= MICRODB_REL_TABLE_NAME_LEN) {
        return MICRODB_ERR_INVALID;
    }
    if (1u + name_len + 2u + 4u + 4u > sizeof(payload)) {
        return MICRODB_ERR_STORAGE;
    }

    payload[off++] = (uint8_t)name_len;
    memcpy(payload + off, impl->name, name_len);
    off = (uint16_t)(off + name_len);
    microdb_put_u16(payload + off, impl->schema_version);
    off = (uint16_t)(off + 2u);
    microdb_put_u32(payload + off, impl->max_rows);
    off = (uint16_t)(off + 4u);
    microdb_put_u32(payload + off, impl->col_count);
    off = (uint16_t)(off + 4u);

    for (i = 0u; i < impl->col_count; ++i) {
        size_t col_name_len = strlen(impl->cols[i].name);
        if (col_name_len >= MICRODB_REL_COL_NAME_LEN) {
            return MICRODB_ERR_SCHEMA;
        }
        if ((size_t)off + 1u + col_name_len + 1u + 1u + 4u > sizeof(payload)) {
            return MICRODB_ERR_STORAGE;
        }
        payload[off++] = (uint8_t)col_name_len;
        memcpy(payload + off, impl->cols[i].name, col_name_len);
        off = (uint16_t)(off + col_name_len);
        payload[off++] = (uint8_t)impl->cols[i].type;
        payload[off++] = (uint8_t)(impl->cols[i].is_index ? 1u : 0u);
        microdb_put_u32(payload + off, (uint32_t)impl->cols[i].size);
        off = (uint16_t)(off + 4u);
    }

    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_REL,
                                    MICRODB_WAL_OP_REL_TABLE_CREATE,
                                    payload,
                                    off);
}

microdb_err_t microdb_persist_rel_clear(microdb_t *db, const microdb_table_t *table) {
    microdb_core_t *core = microdb_core(db);
    uint8_t payload[MICRODB_REL_TABLE_NAME_LEN];
    size_t name_len;

    if (!microdb_storage_ready(core) || core->storage_loading || core->wal_replaying) {
        return MICRODB_OK;
    }
    if (!core->wal_enabled) {
        return microdb_storage_flush(db);
    }
    if (table == NULL) {
        return MICRODB_ERR_INVALID;
    }

    name_len = strlen(table->name);
    if (name_len + 1u > sizeof(payload)) {
        return MICRODB_ERR_INVALID;
    }
    payload[0] = (uint8_t)name_len;
    memcpy(payload + 1u, table->name, name_len);
    return microdb_append_wal_entry(db,
                                    MICRODB_WAL_ENGINE_REL,
                                    MICRODB_WAL_OP_CLEAR,
                                    payload,
                                    (uint16_t)(1u + name_len));
}
