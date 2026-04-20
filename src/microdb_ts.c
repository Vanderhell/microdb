// SPDX-License-Identifier: MIT
#include "microdb_internal.h"
#include "microdb_lock.h"

#include <string.h>

static microdb_err_t microdb_ts_validate_name(const char *name) {
    size_t len;

    if (name == NULL || name[0] == '\0') {
        return MICRODB_ERR_INVALID;
    }

    len = strlen(name);
    if (len >= MICRODB_TS_STREAM_NAME_LEN) {
        return MICRODB_ERR_INVALID;
    }

    return MICRODB_OK;
}

static microdb_ts_stream_t *microdb_ts_find(microdb_core_t *core, const char *name) {
    uint32_t i;

    for (i = 0; i < MICRODB_TS_MAX_STREAMS; ++i) {
        microdb_ts_stream_t *stream = &core->ts.streams[i];
        if (stream->registered && strcmp(stream->name, name) == 0) {
            return stream;
        }
    }

    return NULL;
}

static microdb_err_t microdb_ts_register_apply(microdb_core_t *core,
                                               const char *name,
                                               microdb_ts_type_t type,
                                               size_t raw_size) {
    uint32_t i;

    if (microdb_ts_find(core, name) != NULL) {
        return MICRODB_ERR_EXISTS;
    }
    if (core->ts.registered_streams >= MICRODB_TS_MAX_STREAMS) {
        return MICRODB_ERR_FULL;
    }

    for (i = 0; i < MICRODB_TS_MAX_STREAMS; ++i) {
        microdb_ts_stream_t *stream = &core->ts.streams[i];
        if (!stream->registered) {
            memset(stream->name, 0, sizeof(stream->name));
            memcpy(stream->name, name, strlen(name) + 1u);
            stream->type = type;
            stream->raw_size = (type == MICRODB_TS_RAW) ? raw_size : 0u;
            stream->head = 0u;
            stream->tail = 0u;
            stream->count = 0u;
            stream->registered = true;
            core->ts.registered_streams++;
            return MICRODB_OK;
        }
    }

    return MICRODB_ERR_FULL;
}

static microdb_err_t microdb_ts_sample_capacity(const microdb_core_t *core, uint32_t *capacity_out) {
    size_t bytes_per_stream;
    uint32_t capacity;

    bytes_per_stream = core->ts_arena.capacity / MICRODB_TS_MAX_STREAMS;
    capacity = (uint32_t)(bytes_per_stream / sizeof(microdb_ts_sample_t));
    if (capacity < 4u) {
        return MICRODB_ERR_NO_MEM;
    }

    *capacity_out = capacity;
    return MICRODB_OK;
}

static void microdb_ts_set_value(microdb_ts_stream_t *stream, microdb_ts_sample_t *sample, const void *val) {
    if (stream->type == MICRODB_TS_F32) {
        memcpy(&sample->v.f32, val, sizeof(sample->v.f32));
    } else if (stream->type == MICRODB_TS_I32) {
        memcpy(&sample->v.i32, val, sizeof(sample->v.i32));
    } else if (stream->type == MICRODB_TS_U32) {
        memcpy(&sample->v.u32, val, sizeof(sample->v.u32));
    } else {
        memcpy(sample->v.raw, val, stream->raw_size);
    }
}

static void microdb_ts_rb_insert(microdb_ts_stream_t *stream, const microdb_ts_sample_t *sample) {
    if (stream->count == stream->capacity) {
#if MICRODB_TS_OVERFLOW_POLICY == MICRODB_TS_POLICY_DROP_OLDEST
        MICRODB_LOG("WARN",
                    "TS stream '%s' full: dropping oldest sample ts=%u",
                    stream->name,
                    (unsigned)stream->buf[stream->tail].ts);
#endif
    }

    stream->buf[stream->head] = *sample;
    stream->head = (stream->head + 1u) % stream->capacity;

    if (stream->count < stream->capacity) {
        stream->count++;
    } else {
        stream->tail = (stream->tail + 1u) % stream->capacity;
    }
}

static void microdb_ts_downsample_oldest(microdb_ts_stream_t *stream) {
    uint32_t i0 = stream->tail;
    uint32_t i1 = (stream->tail + 1u) % stream->capacity;
    uint32_t idx;
    uint32_t next;
    microdb_ts_sample_t *a = &stream->buf[i0];
    microdb_ts_sample_t *b = &stream->buf[i1];

    MICRODB_LOG("INFO",
                "TS stream '%s' downsampling oldest two samples",
                stream->name);

    a->ts = (a->ts / 2u) + (b->ts / 2u);

    if (stream->type == MICRODB_TS_F32) {
        a->v.f32 = (a->v.f32 + b->v.f32) * 0.5f;
    } else if (stream->type == MICRODB_TS_I32) {
        a->v.i32 = (a->v.i32 / 2) + (b->v.i32 / 2);
    } else if (stream->type == MICRODB_TS_U32) {
        a->v.u32 = (a->v.u32 / 2u) + (b->v.u32 / 2u);
    }

    idx = i1;
    while (idx != stream->head) {
        next = (idx + 1u) % stream->capacity;
        if (next == stream->head) {
            break;
        }
        stream->buf[idx] = stream->buf[next];
        idx = next;
    }

    stream->head = (stream->head + stream->capacity - 1u) % stream->capacity;
    stream->count--;
}

microdb_err_t microdb_ts_init(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
#if MICRODB_ENABLE_TS
    uint32_t capacity;
    uint32_t i;
#endif

    memset(&core->ts, 0, sizeof(core->ts));

#if MICRODB_ENABLE_TS
    if (microdb_ts_sample_capacity(core, &capacity) != MICRODB_OK) {
        return MICRODB_ERR_NO_MEM;
    }

    for (i = 0; i < MICRODB_TS_MAX_STREAMS; ++i) {
        core->ts.streams[i].buf =
            (microdb_ts_sample_t *)(core->ts_arena.base + (i * capacity * sizeof(microdb_ts_sample_t)));
        core->ts.streams[i].capacity = capacity;
    }
#endif

    return MICRODB_OK;
}

#if MICRODB_ENABLE_TS
microdb_err_t microdb_ts_register(microdb_t *db, const char *name, microdb_ts_type_t type, size_t raw_size) {
    microdb_core_t *core;
    microdb_err_t err;
    microdb_err_t rc = MICRODB_OK;
    uint32_t before_registered = 0u;
    uint32_t restore_index = UINT32_MAX;
    microdb_ts_stream_t restore_stream;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_ts_validate_name(name);
    if (err != MICRODB_OK) {
        return err;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    if (type == MICRODB_TS_RAW && (raw_size == 0u || raw_size > MICRODB_TS_RAW_MAX)) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = microdb_persist_ts_register(db, name, type, raw_size);
        if (rc != MICRODB_OK) {
            goto unlock;
        }
        rc = microdb_ts_register_apply(core, name, type, raw_size);
        goto unlock;
    }

    before_registered = core->ts.registered_streams;
    memset(&restore_stream, 0, sizeof(restore_stream));
    {
        uint32_t i;
        for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
            if (!core->ts.streams[i].registered) {
                restore_index = i;
                restore_stream = core->ts.streams[i];
                break;
            }
        }
    }
    rc = microdb_ts_register_apply(core, name, type, raw_size);
    if (rc != MICRODB_OK) {
        goto unlock;
    }
    rc = microdb_storage_flush(db);
    if (rc != MICRODB_OK) {
        core->ts.registered_streams = before_registered;
        if (restore_index != UINT32_MAX) {
            core->ts.streams[restore_index] = restore_stream;
        }
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_ts_insert(microdb_t *db, const char *name, microdb_timestamp_t ts, const void *val) {
    microdb_core_t *core;
    microdb_ts_stream_t *stream;
    microdb_ts_sample_t sample;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL || val == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    stream = microdb_ts_find(core, name);
    if (stream == NULL) {
        rc = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    if (stream->capacity == 0u) {
        rc = MICRODB_ERR_NO_MEM;
        goto unlock;
    }

#if MICRODB_TS_OVERFLOW_POLICY == MICRODB_TS_POLICY_REJECT
    if (stream->count == stream->capacity) {
        MICRODB_LOG("WARN",
                    "TS stream '%s' full: rejecting new sample (REJECT policy)",
                    stream->name);
        core->ts_dropped_samples++;
        rc = MICRODB_ERR_FULL;
        goto unlock;
    }
#elif MICRODB_TS_OVERFLOW_POLICY == MICRODB_TS_POLICY_DOWNSAMPLE
#endif

    memset(&sample, 0, sizeof(sample));
    sample.ts = ts;
    microdb_ts_set_value(stream, &sample, val);
    rc = microdb_persist_ts_insert(db, name, ts, val, (stream->type == MICRODB_TS_RAW) ? stream->raw_size : 4u);
    if (rc == MICRODB_OK) {
#if MICRODB_TS_OVERFLOW_POLICY == MICRODB_TS_POLICY_DOWNSAMPLE
        if (stream->count == stream->capacity) {
            microdb_ts_downsample_oldest(stream);
            core->ts_dropped_samples++;
        }
#elif MICRODB_TS_OVERFLOW_POLICY == MICRODB_TS_POLICY_DROP_OLDEST
        if (stream->count == stream->capacity) {
            core->ts_dropped_samples++;
        }
#endif
        microdb_ts_rb_insert(stream, &sample);
        microdb__maybe_compact(db);
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_ts_last(microdb_t *db, const char *name, microdb_ts_sample_t *out) {
    microdb_core_t *core;
    microdb_ts_stream_t *stream;
    uint32_t idx;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL || out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    stream = microdb_ts_find(core, name);
    if (stream == NULL || stream->count == 0u) {
        rc = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    idx = (stream->head + stream->capacity - 1u) % stream->capacity;
    *out = stream->buf[idx];
    rc = MICRODB_OK;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_ts_query(microdb_t *db,
                               const char *name,
                               microdb_timestamp_t from,
                               microdb_timestamp_t to,
                               microdb_ts_query_cb_t cb,
                               void *ctx) {
    microdb_core_t *core;
    microdb_ts_stream_t *stream;
    uint32_t i;
    uint32_t idx;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL || cb == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    stream = microdb_ts_find(core, name);
    if (stream == NULL) {
        rc = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    {
        uint32_t snapshot_tail = stream->tail;
        uint32_t snapshot_count = stream->count;
        uint32_t cap = stream->capacity;
        idx = snapshot_tail;
        for (i = 0u; i < snapshot_count; ++i) {
            microdb_ts_sample_t sample = stream->buf[idx];
            bool in_range = (from <= to && sample.ts >= from && sample.ts <= to);
            idx = (idx + 1u) % cap;
            MICRODB_UNLOCK(db);
            if (in_range && !cb(&sample, ctx)) {
                return MICRODB_OK;
            }
            MICRODB_LOCK(db);
            core = microdb_core(db);
            if (core->magic != MICRODB_MAGIC) {
                rc = MICRODB_ERR_INVALID;
                goto unlock;
            }
            stream = microdb_ts_find(core, name);
            if (stream == NULL) {
                rc = MICRODB_ERR_NOT_FOUND;
                goto unlock;
            }
        }
    }

    rc = MICRODB_OK;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_ts_query_buf(microdb_t *db,
                                   const char *name,
                                   microdb_timestamp_t from,
                                   microdb_timestamp_t to,
                                   microdb_ts_sample_t *buf,
                                   size_t max_count,
                                   size_t *out_count) {
    microdb_core_t *core;
    microdb_ts_stream_t *stream;
    uint32_t i;
    uint32_t idx;
    size_t written = 0u;
    microdb_err_t status = MICRODB_OK;

    if (db == NULL || buf == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        status = MICRODB_ERR_INVALID;
        goto unlock;
    }

    stream = microdb_ts_find(core, name);
    if (stream == NULL) {
        status = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    idx = stream->tail;
    for (i = 0; i < stream->count; ++i) {
        microdb_ts_sample_t *sample = &stream->buf[idx];
        if (from <= to && sample->ts >= from && sample->ts <= to) {
            if (written < max_count) {
                buf[written] = *sample;
            } else {
                status = MICRODB_ERR_OVERFLOW;
            }
            written++;
        }
        idx = (idx + 1u) % stream->capacity;
    }

    if (out_count != NULL) {
        *out_count = (written < max_count) ? written : max_count;
    }

unlock:
    MICRODB_UNLOCK(db);
    return status;
}

microdb_err_t microdb_ts_count(microdb_t *db,
                               const char *name,
                               microdb_timestamp_t from,
                               microdb_timestamp_t to,
                               size_t *out_count) {
    microdb_core_t *core;
    microdb_ts_stream_t *stream;
    uint32_t i;
    uint32_t idx;
    size_t count = 0u;
    microdb_err_t rc = MICRODB_OK;

    if (db == NULL || out_count == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    stream = microdb_ts_find(core, name);
    if (stream == NULL) {
        rc = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    idx = stream->tail;
    for (i = 0; i < stream->count; ++i) {
        microdb_ts_sample_t *sample = &stream->buf[idx];
        if (from <= to && sample->ts >= from && sample->ts <= to) {
            count++;
        }
        idx = (idx + 1u) % stream->capacity;
    }

    *out_count = count;
    rc = MICRODB_OK;

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}

microdb_err_t microdb_ts_clear(microdb_t *db, const char *name) {
    microdb_core_t *core;
    microdb_ts_stream_t *stream;
    microdb_err_t rc = MICRODB_OK;
    uint32_t saved_head = 0u;
    uint32_t saved_tail = 0u;
    uint32_t saved_count = 0u;

    if (db == NULL) {
        return MICRODB_ERR_INVALID;
    }

    MICRODB_LOCK(db);
    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        rc = MICRODB_ERR_INVALID;
        goto unlock;
    }

    stream = microdb_ts_find(core, name);
    if (stream == NULL) {
        rc = MICRODB_ERR_NOT_FOUND;
        goto unlock;
    }

    if (core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying) {
        rc = microdb_persist_ts_clear(db, name);
        if (rc != MICRODB_OK) {
            goto unlock;
        }
        stream->head = 0u;
        stream->tail = 0u;
        stream->count = 0u;
        goto unlock;
    }

    saved_head = stream->head;
    saved_tail = stream->tail;
    saved_count = stream->count;
    stream->head = 0u;
    stream->tail = 0u;
    stream->count = 0u;
    rc = microdb_storage_flush(db);
    if (rc != MICRODB_OK) {
        stream->head = saved_head;
        stream->tail = saved_tail;
        stream->count = saved_count;
    }

unlock:
    MICRODB_UNLOCK(db);
    return rc;
}
#else
microdb_err_t microdb_ts_register(microdb_t *db, const char *name, microdb_ts_type_t type, size_t raw_size) {
    (void)db;
    (void)name;
    (void)type;
    (void)raw_size;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_ts_insert(microdb_t *db, const char *name, microdb_timestamp_t ts, const void *val) {
    (void)db;
    (void)name;
    (void)ts;
    (void)val;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_ts_last(microdb_t *db, const char *name, microdb_ts_sample_t *out) {
    (void)db;
    (void)name;
    (void)out;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_ts_query(microdb_t *db,
                               const char *name,
                               microdb_timestamp_t from,
                               microdb_timestamp_t to,
                               microdb_ts_query_cb_t cb,
                               void *ctx) {
    (void)db;
    (void)name;
    (void)from;
    (void)to;
    (void)cb;
    (void)ctx;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_ts_query_buf(microdb_t *db,
                                   const char *name,
                                   microdb_timestamp_t from,
                                   microdb_timestamp_t to,
                                   microdb_ts_sample_t *buf,
                                   size_t max_count,
                                   size_t *out_count) {
    (void)db;
    (void)name;
    (void)from;
    (void)to;
    (void)buf;
    (void)max_count;
    (void)out_count;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_ts_count(microdb_t *db,
                               const char *name,
                               microdb_timestamp_t from,
                               microdb_timestamp_t to,
                               size_t *out_count) {
    (void)db;
    (void)name;
    (void)from;
    (void)to;
    (void)out_count;
    return MICRODB_ERR_DISABLED;
}

microdb_err_t microdb_ts_clear(microdb_t *db, const char *name) {
    (void)db;
    (void)name;
    return MICRODB_ERR_DISABLED;
}
#endif
