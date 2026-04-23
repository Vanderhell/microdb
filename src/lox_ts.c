// SPDX-License-Identifier: MIT
#include "lox_internal.h"
#include "lox_lock.h"

#include <stdlib.h>
#include <string.h>

static lox_err_t lox_ts_validate_name(const char *name) {
    size_t len;

    if (name == NULL || name[0] == '\0') {
        return LOX_ERR_INVALID;
    }

    len = strlen(name);
    if (len >= LOX_TS_STREAM_NAME_LEN) {
        return LOX_ERR_INVALID;
    }

    return LOX_OK;
}

static lox_ts_stream_t *lox_ts_find(lox_core_t *core, const char *name) {
    uint32_t i;

    for (i = 0; i < LOX_TS_MAX_STREAMS; ++i) {
        lox_ts_stream_t *stream = &core->ts.streams[i];
        if (stream->registered && strcmp(stream->name, name) == 0) {
            return stream;
        }
    }

    return NULL;
}

static uint32_t lox_ts_stream_val_size(const lox_ts_stream_t *stream) {
    return (stream->type == LOX_TS_RAW) ? (uint32_t)stream->raw_size : 4u;
}

static uint8_t *lox_ts_sample_ptr(lox_ts_stream_t *stream, uint32_t idx) {
    return stream->buf + (idx * stream->sample_stride);
}

static const uint8_t *lox_ts_sample_ptr_const(const lox_ts_stream_t *stream, uint32_t idx) {
    return stream->buf + (idx * stream->sample_stride);
}

static void lox_ts_read_sample(const lox_ts_stream_t *stream, uint32_t idx, lox_ts_sample_t *out) {
    const uint8_t *slot = lox_ts_sample_ptr_const(stream, idx);
    uint32_t val_len = lox_ts_stream_val_size(stream);

    memset(out, 0, sizeof(*out));
    memcpy(&out->ts, slot, sizeof(out->ts));
    memcpy(&out->v, slot + sizeof(out->ts), val_len);
}

static void lox_ts_write_sample(const lox_ts_stream_t *stream, uint32_t idx, const lox_ts_sample_t *sample) {
    uint8_t *slot = lox_ts_sample_ptr((lox_ts_stream_t *)stream, idx);
    uint32_t val_len = lox_ts_stream_val_size(stream);

    memcpy(slot, &sample->ts, sizeof(sample->ts));
    memcpy(slot + sizeof(sample->ts), &sample->v, val_len);
}

static void lox_ts_copy_sample_slot(const lox_ts_stream_t *stream, uint32_t dst_idx, uint32_t src_idx) {
    uint8_t *dst = lox_ts_sample_ptr((lox_ts_stream_t *)stream, dst_idx);
    const uint8_t *src = lox_ts_sample_ptr_const(stream, src_idx);
    memcpy(dst, src, stream->sample_stride);
}

typedef struct {
    lox_ts_stream_t *stream;
    uint32_t stride;
    uint32_t min_bytes;
    uint32_t alloc_bytes;
    uint32_t capacity;
    uint32_t rem_num;
    uint8_t *new_buf;
    uint8_t *tmp;
    uint32_t keep_count;
} lox_ts_layout_item_t;

static uint32_t lox_ts_stride_for_type(lox_ts_type_t type, size_t raw_size) {
    uint32_t val_size = (type == LOX_TS_RAW) ? (uint32_t)raw_size : 4u;
    return (uint32_t)sizeof(lox_timestamp_t) + val_size;
}

static lox_err_t lox_ts_repartition(lox_core_t *core) {
    lox_ts_layout_item_t items[LOX_TS_MAX_STREAMS];
    uint32_t count = 0u;
    uint32_t i;
    uint32_t j;
    uint32_t rem_bytes;
    uint32_t total_weight = 0u;
    uint32_t sum_min = 0u;
    uint32_t sum_alloc = 0u;
    uint8_t *cursor = core->ts_arena.base;

    memset(items, 0, sizeof(items));

    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        lox_ts_stream_t *stream = &core->ts.streams[i];
        uint32_t stride;

        if (!stream->registered) {
            continue;
        }
        stride = lox_ts_stride_for_type(stream->type, stream->raw_size);
        if (stride == 0u) {
            return LOX_ERR_INVALID;
        }
        items[count].stream = stream;
        items[count].stride = stride;
        items[count].min_bytes = stride * 4u;
        sum_min += items[count].min_bytes;
        total_weight += stride;
        count++;
    }

    if (count == 0u) {
        return LOX_OK;
    }
    if (sum_min > core->ts_arena.capacity || total_weight == 0u) {
        return LOX_ERR_NO_MEM;
    }

    rem_bytes = (uint32_t)core->ts_arena.capacity - sum_min;
    for (i = 0u; i < count; ++i) {
        uint64_t num = (uint64_t)rem_bytes * (uint64_t)items[i].stride;
        uint32_t extra = (uint32_t)(num / (uint64_t)total_weight);
        items[i].rem_num = (uint32_t)(num % (uint64_t)total_weight);
        items[i].alloc_bytes = items[i].min_bytes + extra;
        sum_alloc += items[i].alloc_bytes;
    }

    while (sum_alloc < (uint32_t)core->ts_arena.capacity) {
        uint32_t best = 0u;
        for (i = 1u; i < count; ++i) {
            if (items[i].rem_num > items[best].rem_num) {
                best = i;
            }
        }
        items[best].alloc_bytes++;
        items[best].rem_num = 0u;
        sum_alloc++;
    }

    for (i = 0u; i < count; ++i) {
        items[i].capacity = items[i].alloc_bytes / items[i].stride;
        if (items[i].capacity < 4u) {
            return LOX_ERR_NO_MEM;
        }
        items[i].new_buf = cursor;
        cursor += items[i].capacity * items[i].stride;
    }

    for (i = 0u; i < count; ++i) {
        lox_ts_stream_t *stream = items[i].stream;
        uint32_t keep = stream->count;
        uint32_t idx;

        if (keep > items[i].capacity) {
            keep = items[i].capacity;
            core->ts_dropped_samples += (stream->count - keep);
        }
        items[i].keep_count = keep;
        if (keep == 0u) {
            continue;
        }

        items[i].tmp = (uint8_t *)malloc((size_t)keep * items[i].stride);
        if (items[i].tmp == NULL) {
            for (j = 0u; j < i; ++j) {
                free(items[j].tmp);
                items[j].tmp = NULL;
            }
            return LOX_ERR_NO_MEM;
        }

        idx = stream->tail;
        if (stream->count > keep) {
            idx = (stream->tail + (stream->count - keep)) % stream->capacity;
        }
        for (j = 0u; j < keep; ++j) {
            memcpy(items[i].tmp + ((size_t)j * items[i].stride),
                   stream->buf + ((size_t)idx * items[i].stride),
                   items[i].stride);
            idx = (idx + 1u) % stream->capacity;
        }
    }

    for (i = 0u; i < count; ++i) {
        lox_ts_stream_t *stream = items[i].stream;
        stream->sample_stride = items[i].stride;
        stream->capacity = items[i].capacity;
        stream->buf = items[i].new_buf;
        stream->tail = 0u;
        stream->count = items[i].keep_count;
        stream->head = (stream->count == stream->capacity) ? 0u : stream->count;
        if (items[i].keep_count != 0u) {
            memcpy(stream->buf, items[i].tmp, (size_t)items[i].keep_count * items[i].stride);
        }
        free(items[i].tmp);
        items[i].tmp = NULL;
    }

    return LOX_OK;
}

static lox_err_t lox_ts_register_apply(lox_core_t *core,
                                               const char *name,
                                               lox_ts_type_t type,
                                               size_t raw_size,
                                               const lox_ts_log_retain_cfg_t *cfg) {
    uint32_t i;

    if (lox_ts_find(core, name) != NULL) {
        return LOX_ERR_EXISTS;
    }
    if (core->ts.registered_streams >= LOX_TS_MAX_STREAMS) {
        return LOX_ERR_FULL;
    }

    for (i = 0; i < LOX_TS_MAX_STREAMS; ++i) {
        lox_ts_stream_t *stream = &core->ts.streams[i];
        if (!stream->registered) {
            memset(stream->name, 0, sizeof(stream->name));
            memcpy(stream->name, name, strlen(name) + 1u);
            stream->type = type;
            stream->raw_size = (type == LOX_TS_RAW) ? raw_size : 0u;
            stream->log_retain_zones = (cfg != NULL) ? cfg->log_retain_zones : 0u;
            stream->log_retain_zone_pct = (cfg != NULL) ? cfg->log_retain_zone_pct : 0u;
            stream->sample_stride = lox_ts_stride_for_type(stream->type, stream->raw_size);
            if (stream->sample_stride == 0u) {
                return LOX_ERR_INVALID;
            }
            stream->head = 0u;
            stream->tail = 0u;
            stream->count = 0u;
            stream->registered = true;
            {
                lox_err_t repartition_rc = lox_ts_repartition(core);
                if (repartition_rc != LOX_OK) {
                    memset(stream, 0, sizeof(*stream));
                    return repartition_rc;
                }
            }
            core->ts.registered_streams++;
            core->ts.mutation_seq++;
            return LOX_OK;
        }
    }

    return LOX_ERR_FULL;
}

static void lox_ts_set_value(lox_ts_stream_t *stream, lox_ts_sample_t *sample, const void *val) {
    if (stream->type == LOX_TS_F32) {
        memcpy(&sample->v.f32, val, sizeof(sample->v.f32));
    } else if (stream->type == LOX_TS_I32) {
        memcpy(&sample->v.i32, val, sizeof(sample->v.i32));
    } else if (stream->type == LOX_TS_U32) {
        memcpy(&sample->v.u32, val, sizeof(sample->v.u32));
    } else {
        memcpy(sample->v.raw, val, stream->raw_size);
    }
}

static void lox_ts_rb_insert(lox_ts_stream_t *stream, const lox_ts_sample_t *sample) {
    if (stream->count == stream->capacity) {
#if LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_DROP_OLDEST
        lox_ts_sample_t dropped;
        lox_ts_read_sample(stream, stream->tail, &dropped);
        LOX_LOG("WARN",
                    "TS stream '%s' full: dropping oldest sample ts=%u",
                    stream->name,
                    (unsigned)dropped.ts);
#endif
    }

    lox_ts_write_sample(stream, stream->head, sample);
    stream->head = (stream->head + 1u) % stream->capacity;

    if (stream->count < stream->capacity) {
        stream->count++;
    } else {
        stream->tail = (stream->tail + 1u) % stream->capacity;
    }
}

static void lox_ts_downsample_oldest(lox_ts_stream_t *stream) {
    uint32_t i0 = stream->tail;
    uint32_t i1 = (stream->tail + 1u) % stream->capacity;
    uint32_t idx;
    uint32_t next;
    lox_ts_sample_t a;
    lox_ts_sample_t b;

    lox_ts_read_sample(stream, i0, &a);
    lox_ts_read_sample(stream, i1, &b);

    LOX_LOG("INFO",
                "TS stream '%s' downsampling oldest two samples",
                stream->name);

    a.ts = (a.ts / 2u) + (b.ts / 2u);

    if (stream->type == LOX_TS_F32) {
        a.v.f32 = (a.v.f32 + b.v.f32) * 0.5f;
    } else if (stream->type == LOX_TS_I32) {
        a.v.i32 = (a.v.i32 / 2) + (b.v.i32 / 2);
    } else if (stream->type == LOX_TS_U32) {
        a.v.u32 = (a.v.u32 / 2u) + (b.v.u32 / 2u);
    } else {
        size_t i;
        for (i = 0u; i < stream->raw_size; ++i) {
            uint16_t merged = (uint16_t)a.v.raw[i] + (uint16_t)b.v.raw[i];
            a.v.raw[i] = (uint8_t)(merged / 2u);
        }
    }
    lox_ts_write_sample(stream, i0, &a);

    idx = i1;
    while (idx != stream->head) {
        next = (idx + 1u) % stream->capacity;
        if (next == stream->head) {
            break;
        }
        lox_ts_copy_sample_slot(stream, idx, next);
        idx = next;
    }

    stream->head = (stream->head + stream->capacity - 1u) % stream->capacity;
    stream->count--;
}

static void lox_ts_merge_pair(lox_ts_stream_t *stream,
                                  uint32_t dst_idx,
                                  uint32_t a_idx,
                                  uint32_t b_idx) {
    lox_ts_sample_t a;
    lox_ts_sample_t b;

    lox_ts_read_sample(stream, a_idx, &a);
    lox_ts_read_sample(stream, b_idx, &b);
    a.ts = (a.ts / 2u) + (b.ts / 2u);

    if (stream->type == LOX_TS_F32) {
        a.v.f32 = (a.v.f32 + b.v.f32) * 0.5f;
    } else if (stream->type == LOX_TS_I32) {
        a.v.i32 = (a.v.i32 / 2) + (b.v.i32 / 2);
    } else if (stream->type == LOX_TS_U32) {
        a.v.u32 = (a.v.u32 / 2u) + (b.v.u32 / 2u);
    } else {
        size_t i;
        for (i = 0u; i < stream->raw_size; ++i) {
            uint16_t merged = (uint16_t)a.v.raw[i] + (uint16_t)b.v.raw[i];
            a.v.raw[i] = (uint8_t)(merged / 2u);
        }
    }
    lox_ts_write_sample(stream, dst_idx, &a);
}

static uint32_t lox_ts_log_retain_apply(lox_ts_stream_t *stream) {
    uint32_t zone_size;
    uint32_t read_pos;
    uint32_t write_pos;
    uint32_t removed;
    uint32_t k;
    uint32_t cap;
    uint32_t count;
    uint32_t tail;

    if (stream->count == 0u || stream->capacity == 0u) {
        return 0u;
    }
    zone_size = ((uint32_t)stream->capacity * (uint32_t)stream->log_retain_zone_pct) / 100u;
    if (zone_size < 2u) {
        zone_size = 2u;
    }
    if (zone_size > stream->count) {
        zone_size = stream->count;
    }

    cap = stream->capacity;
    count = stream->count;
    tail = stream->tail;
    read_pos = 0u;
    write_pos = 0u;

    while (read_pos + 1u < zone_size) {
        uint32_t a_idx = (tail + read_pos) % cap;
        uint32_t b_idx = (tail + read_pos + 1u) % cap;
        uint32_t dst_idx = (tail + write_pos) % cap;
        lox_ts_merge_pair(stream, dst_idx, a_idx, b_idx);
        read_pos += 2u;
        write_pos += 1u;
    }

    if (read_pos < zone_size) {
        uint32_t src_idx = (tail + read_pos) % cap;
        uint32_t dst_idx = (tail + write_pos) % cap;
        if (src_idx != dst_idx) {
            lox_ts_copy_sample_slot(stream, dst_idx, src_idx);
        }
        write_pos++;
    }

    removed = zone_size - write_pos;
    for (k = zone_size; k < count; ++k) {
        uint32_t src_idx = (tail + k) % cap;
        uint32_t dst_idx = (tail + (k - removed)) % cap;
        if (src_idx != dst_idx) {
            lox_ts_copy_sample_slot(stream, dst_idx, src_idx);
        }
    }

    stream->count = count - removed;
    stream->head = (stream->tail + stream->count) % cap;
    return removed;
}

lox_err_t lox_ts_init(lox_t *db) {
    lox_core_t *core = lox_core(db);
    uint32_t i;

    memset(&core->ts, 0, sizeof(core->ts));

    for (i = 0; i < LOX_TS_MAX_STREAMS; ++i) {
        core->ts.streams[i].buf = core->ts_arena.base;
        core->ts.streams[i].sample_stride = 0u;
        core->ts.streams[i].capacity = 0u;
    }

    return LOX_OK;
}

#if LOX_ENABLE_TS
lox_err_t lox_ts_register_ex(lox_t *db,
                                     const char *name,
                                     lox_ts_type_t type,
                                     size_t raw_size,
                                     const lox_ts_log_retain_cfg_t *cfg) {
    lox_core_t *core;
    lox_err_t err;
    lox_err_t rc = LOX_OK;
    uint32_t before_registered = 0u;
    uint32_t restore_index = UINT32_MAX;
    lox_ts_stream_t restore_stream;
    lox_ts_log_retain_cfg_t local_cfg;
    const lox_ts_log_retain_cfg_t *cfg_ptr = NULL;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    err = lox_ts_validate_name(name);
    if (err != LOX_OK) {
        return err;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    if (type == LOX_TS_RAW && (raw_size == 0u || raw_size > LOX_TS_RAW_MAX)) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    memset(&local_cfg, 0, sizeof(local_cfg));
    if (cfg != NULL && cfg->log_retain_zones != 0u) {
        if (cfg->log_retain_zone_pct == 0u ||
            ((uint32_t)cfg->log_retain_zones * (uint32_t)cfg->log_retain_zone_pct) > 100u ||
            cfg->log_retain_zones < 2u) {
            rc = LOX_ERR_INVALID;
            goto unlock;
        }
        local_cfg = *cfg;
        cfg_ptr = &local_cfg;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = lox_persist_ts_register(db, name, type, raw_size);
        if (rc != LOX_OK) {
            goto unlock;
        }
        rc = lox_ts_register_apply(core, name, type, raw_size, cfg_ptr);
        goto unlock;
    }

    before_registered = core->ts.registered_streams;
    memset(&restore_stream, 0, sizeof(restore_stream));
    {
        uint32_t i;
        for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
            if (!core->ts.streams[i].registered) {
                restore_index = i;
                restore_stream = core->ts.streams[i];
                break;
            }
        }
    }
    rc = lox_ts_register_apply(core, name, type, raw_size, cfg_ptr);
    if (rc != LOX_OK) {
        goto unlock;
    }
    rc = lox_storage_flush(db);
    if (rc != LOX_OK) {
        core->ts.registered_streams = before_registered;
        if (restore_index != UINT32_MAX) {
            core->ts.streams[restore_index] = restore_stream;
        }
        if (core->ts.mutation_seq != 0u) {
            core->ts.mutation_seq--;
        }
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_ts_register(lox_t *db, const char *name, lox_ts_type_t type, size_t raw_size) {
    return lox_ts_register_ex(db, name, type, raw_size, NULL);
}

lox_err_t lox_ts_insert(lox_t *db, const char *name, lox_timestamp_t ts, const void *val) {
    lox_core_t *core;
    lox_ts_stream_t *stream;
    lox_ts_sample_t sample;
    lox_err_t rc = LOX_OK;

    if (db == NULL || val == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    stream = lox_ts_find(core, name);
    if (stream == NULL) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    if (stream->capacity == 0u) {
        rc = LOX_ERR_NO_MEM;
        goto unlock;
    }

#if LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_REJECT
    if (stream->count == stream->capacity) {
        LOX_LOG("WARN",
                    "TS stream '%s' full: rejecting new sample (REJECT policy)",
                    stream->name);
        core->ts_dropped_samples++;
        rc = LOX_ERR_FULL;
        goto unlock;
    }
#elif LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_DOWNSAMPLE
#elif LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_LOG_RETAIN
    if (stream->count == stream->capacity &&
        stream->log_retain_zones == 0u) {
        core->ts_dropped_samples++;
    }
#endif

    memset(&sample, 0, sizeof(sample));
    sample.ts = ts;
    lox_ts_set_value(stream, &sample, val);
    rc = lox_persist_ts_insert(db, name, ts, val, (stream->type == LOX_TS_RAW) ? stream->raw_size : 4u);
    if (rc == LOX_OK) {
#if LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_DOWNSAMPLE
        if (stream->count == stream->capacity) {
            lox_ts_downsample_oldest(stream);
            core->ts_dropped_samples++;
        }
#elif LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_LOG_RETAIN
        if (stream->count == stream->capacity && stream->log_retain_zones > 0u) {
            core->ts_dropped_samples += lox_ts_log_retain_apply(stream);
        }
#elif LOX_TS_OVERFLOW_POLICY == LOX_TS_POLICY_DROP_OLDEST
        if (stream->count == stream->capacity) {
            core->ts_dropped_samples++;
        }
#endif
        lox_ts_rb_insert(stream, &sample);
        core->ts.mutation_seq++;
        lox__maybe_compact(db);
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_ts_last(lox_t *db, const char *name, lox_ts_sample_t *out) {
    lox_core_t *core;
    lox_ts_stream_t *stream;
    uint32_t idx;
    lox_err_t rc = LOX_OK;

    if (db == NULL || out == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    stream = lox_ts_find(core, name);
    if (stream == NULL || stream->count == 0u) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    idx = (stream->head + stream->capacity - 1u) % stream->capacity;
    lox_ts_read_sample(stream, idx, out);
    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_ts_query(lox_t *db,
                               const char *name,
                               lox_timestamp_t from,
                               lox_timestamp_t to,
                               lox_ts_query_cb_t cb,
                               void *ctx) {
    lox_core_t *core;
    lox_ts_stream_t *stream;
    uint32_t i;
    uint32_t idx;
    uint32_t snapshot_mutation_seq;
    lox_err_t rc = LOX_OK;

    if (db == NULL || cb == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    stream = lox_ts_find(core, name);
    if (stream == NULL) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    {
        uint32_t snapshot_tail = stream->tail;
        uint32_t snapshot_count = stream->count;
        uint32_t cap = stream->capacity;
        snapshot_mutation_seq = core->ts.mutation_seq;
        idx = snapshot_tail;
        for (i = 0u; i < snapshot_count; ++i) {
            lox_ts_sample_t sample;
            lox_ts_read_sample(stream, idx, &sample);
            bool in_range = (from <= to && sample.ts >= from && sample.ts <= to);
            idx = (idx + 1u) % cap;
            LOX_UNLOCK(db);
            if (in_range && !cb(&sample, ctx)) {
                return LOX_OK;
            }
            LOX_LOCK(db);
            core = lox_core(db);
            if (core->magic != LOX_MAGIC) {
                rc = LOX_ERR_INVALID;
                goto unlock;
            }
            stream = lox_ts_find(core, name);
            if (stream == NULL) {
                rc = LOX_ERR_NOT_FOUND;
                goto unlock;
            }
            if (core->ts.mutation_seq != snapshot_mutation_seq) {
                rc = LOX_ERR_MODIFIED;
                goto unlock;
            }
        }
    }

    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_ts_query_buf(lox_t *db,
                                   const char *name,
                                   lox_timestamp_t from,
                                   lox_timestamp_t to,
                                   lox_ts_sample_t *buf,
                                   size_t max_count,
                                   size_t *out_count) {
    lox_core_t *core;
    lox_ts_stream_t *stream;
    uint32_t i;
    uint32_t idx;
    size_t written = 0u;
    lox_err_t status = LOX_OK;

    if (db == NULL || buf == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        status = LOX_ERR_INVALID;
        goto unlock;
    }

    stream = lox_ts_find(core, name);
    if (stream == NULL) {
        status = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    idx = stream->tail;
    for (i = 0; i < stream->count; ++i) {
        lox_ts_sample_t sample;
        lox_ts_read_sample(stream, idx, &sample);
        if (from <= to && sample.ts >= from && sample.ts <= to) {
            if (written < max_count) {
                buf[written] = sample;
            } else {
                status = LOX_ERR_OVERFLOW;
            }
            written++;
        }
        idx = (idx + 1u) % stream->capacity;
    }

    if (out_count != NULL) {
        *out_count = (written < max_count) ? written : max_count;
    }

unlock:
    LOX_UNLOCK(db);
    return status;
}

lox_err_t lox_ts_count(lox_t *db,
                               const char *name,
                               lox_timestamp_t from,
                               lox_timestamp_t to,
                               size_t *out_count) {
    lox_core_t *core;
    lox_ts_stream_t *stream;
    uint32_t i;
    uint32_t idx;
    size_t count = 0u;
    lox_err_t rc = LOX_OK;

    if (db == NULL || out_count == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    stream = lox_ts_find(core, name);
    if (stream == NULL) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    idx = stream->tail;
    for (i = 0; i < stream->count; ++i) {
        lox_ts_sample_t sample;
        lox_ts_read_sample(stream, idx, &sample);
        if (from <= to && sample.ts >= from && sample.ts <= to) {
            count++;
        }
        idx = (idx + 1u) % stream->capacity;
    }

    *out_count = count;
    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_ts_clear(lox_t *db, const char *name) {
    lox_core_t *core;
    lox_ts_stream_t *stream;
    lox_err_t rc = LOX_OK;
    uint32_t saved_head = 0u;
    uint32_t saved_tail = 0u;
    uint32_t saved_count = 0u;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    stream = lox_ts_find(core, name);
    if (stream == NULL) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = lox_persist_ts_clear(db, name);
        if (rc != LOX_OK) {
            goto unlock;
        }
        stream->head = 0u;
        stream->tail = 0u;
        stream->count = 0u;
        core->ts.mutation_seq++;
        goto unlock;
    }

    saved_head = stream->head;
    saved_tail = stream->tail;
    saved_count = stream->count;
    stream->head = 0u;
    stream->tail = 0u;
    stream->count = 0u;
    core->ts.mutation_seq++;
    rc = lox_storage_flush(db);
    if (rc != LOX_OK) {
        stream->head = saved_head;
        stream->tail = saved_tail;
        stream->count = saved_count;
        core->ts.mutation_seq--;
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}
#else
lox_err_t lox_ts_register(lox_t *db, const char *name, lox_ts_type_t type, size_t raw_size) {
    (void)db;
    (void)name;
    (void)type;
    (void)raw_size;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_register_ex(lox_t *db,
                                     const char *name,
                                     lox_ts_type_t type,
                                     size_t raw_size,
                                     const lox_ts_log_retain_cfg_t *cfg) {
    (void)db;
    (void)name;
    (void)type;
    (void)raw_size;
    (void)cfg;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_insert(lox_t *db, const char *name, lox_timestamp_t ts, const void *val) {
    (void)db;
    (void)name;
    (void)ts;
    (void)val;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_last(lox_t *db, const char *name, lox_ts_sample_t *out) {
    (void)db;
    (void)name;
    (void)out;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_query(lox_t *db,
                               const char *name,
                               lox_timestamp_t from,
                               lox_timestamp_t to,
                               lox_ts_query_cb_t cb,
                               void *ctx) {
    (void)db;
    (void)name;
    (void)from;
    (void)to;
    (void)cb;
    (void)ctx;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_query_buf(lox_t *db,
                                   const char *name,
                                   lox_timestamp_t from,
                                   lox_timestamp_t to,
                                   lox_ts_sample_t *buf,
                                   size_t max_count,
                                   size_t *out_count) {
    (void)db;
    (void)name;
    (void)from;
    (void)to;
    (void)buf;
    (void)max_count;
    (void)out_count;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_count(lox_t *db,
                               const char *name,
                               lox_timestamp_t from,
                               lox_timestamp_t to,
                               size_t *out_count) {
    (void)db;
    (void)name;
    (void)from;
    (void)to;
    (void)out_count;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_ts_clear(lox_t *db, const char *name) {
    (void)db;
    (void)name;
    return LOX_ERR_DISABLED;
}
#endif
