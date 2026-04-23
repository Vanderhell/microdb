// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"
#include "../src/lox_internal.h"

#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_ram_storage;
static uint32_t g_mock_time = 0u;

typedef struct {
    uint32_t count;
    uint32_t stop_after;
} iter_ctx_t;

static lox_timestamp_t mock_now(void) {
    return (lox_timestamp_t)g_mock_time;
}

static lox_core_t *test_core(void) {
    return lox_core(&g_db);
}

static void test_make_key(char *buf, size_t buf_len, uint32_t index);

static uint32_t test_kv_capacity(void) {
    return LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS;
}

static uint32_t test_kv_hash(const char *key) {
    uint32_t hash = 2166136261u;
    size_t i;

    for (i = 0u; key[i] != '\0'; ++i) {
        hash ^= (uint8_t)key[i];
        hash *= 16777619u;
    }
    return hash;
}

static uint32_t test_kv_manual_live_value_bytes(void) {
    lox_core_t *core = test_core();
    uint32_t i;
    uint32_t total = 0u;

    for (i = 0u; i < core->kv.bucket_count; ++i) {
        const lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == 1u) {
            total += bucket->val_len;
        }
    }
    return total;
}

static bool test_find_probe_pair(char *out_a, size_t out_a_len, char *out_b, size_t out_b_len) {
    lox_core_t *core = test_core();
    uint32_t mask = core->kv.bucket_count - 1u;
    uint32_t i;
    uint32_t j;
    char key_i[16];
    char key_j[16];

    for (i = 0u; i < 512u; ++i) {
        test_make_key(key_i, sizeof(key_i), i);
        for (j = i + 1u; j < 512u; ++j) {
            uint32_t hash_i;
            uint32_t hash_j;
            test_make_key(key_j, sizeof(key_j), j);
            hash_i = test_kv_hash(key_i);
            hash_j = test_kv_hash(key_j);
            if ((hash_i & mask) == (hash_j & mask) && hash_i != hash_j) {
                memcpy(out_a, key_i, out_a_len);
                memcpy(out_b, key_j, out_b_len);
                return true;
            }
        }
    }
    return false;
}

static const lox_kv_bucket_t *test_find_bucket(const char *key) {
    lox_core_t *core = test_core();
    uint32_t i;

    for (i = 0; i < core->kv.bucket_count; ++i) {
        const lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == 1u && strcmp(bucket->key, key) == 0) {
            return bucket;
        }
    }

    return NULL;
}

static void test_make_key(char *buf, size_t buf_len, uint32_t index) {
    (void)snprintf(buf, buf_len, "k%03u", (unsigned)index);
}

static void setup_basic(void) {
    lox_cfg_t cfg;

    ASSERT_EQ(lox_port_ram_init(&g_ram_storage, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void setup_ttl(void) {
    lox_cfg_t cfg;

    g_mock_time = 1000u;
    ASSERT_EQ(lox_port_ram_init(&g_ram_storage, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    lox_deinit(&g_db);
    lox_port_ram_deinit(&g_ram_storage);
}

static bool count_iter_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    iter_ctx_t *iter = (iter_ctx_t *)ctx;

    (void)key;
    (void)val;
    (void)val_len;
    (void)ttl_remaining;
    iter->count++;
    return iter->count < iter->stop_after;
}

MDB_TEST(kv_set_and_get_basic) {
    uint8_t val_in[4] = { 0xDEu, 0xADu, 0xBEu, 0xEFu };
    uint8_t val_out[4] = { 0u, 0u, 0u, 0u };
    size_t out_len = 0u;

    ASSERT_EQ(lox_kv_put(&g_db, "testkey", val_in, sizeof(val_in)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "testkey", val_out, sizeof(val_out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, 4u);
    ASSERT_MEM_EQ(val_out, val_in, sizeof(val_in));
}

MDB_TEST(kv_get_nonexistent) {
    uint8_t buf[4] = { 0u, 0u, 0u, 0u };
    ASSERT_EQ(lox_kv_get(&g_db, "nope", buf, sizeof(buf), NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_delete_existing) {
    uint8_t value = 42u;
    uint8_t buf = 0u;

    ASSERT_EQ(lox_kv_put(&g_db, "gone", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_del(&g_db, "gone"), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "gone", &buf, sizeof(buf), NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_delete_nonexistent) {
    ASSERT_EQ(lox_kv_del(&g_db, "ghost"), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_overwrite_existing_returns_latest_value) {
    uint8_t value_a[3] = { 1u, 2u, 3u };
    uint8_t value_b[5] = { 9u, 8u, 7u, 6u, 5u };
    uint8_t out[5] = { 0u, 0u, 0u, 0u, 0u };
    size_t out_len = 0u;

    ASSERT_EQ(lox_kv_put(&g_db, "dup", value_a, sizeof(value_a)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "dup", value_b, sizeof(value_b)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "dup", out, sizeof(out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, 5u);
    ASSERT_MEM_EQ(out, value_b, sizeof(value_b));
}

MDB_TEST(kv_overwrite_same_len_reuses_slot_without_growth) {
    uint8_t value_a[4] = { 1u, 1u, 1u, 1u };
    uint8_t value_b[4] = { 2u, 2u, 2u, 2u };
    const lox_kv_bucket_t *before;
    const lox_kv_bucket_t *after;
    uint32_t used_before;

    ASSERT_EQ(lox_kv_put(&g_db, "same", value_a, sizeof(value_a)), LOX_OK);
    before = test_find_bucket("same");
    used_before = test_core()->kv.value_used;
    ASSERT_EQ(lox_kv_put(&g_db, "same", value_b, sizeof(value_b)), LOX_OK);
    after = test_find_bucket("same");
    ASSERT_EQ(before->val_offset, after->val_offset);
    ASSERT_EQ(used_before, test_core()->kv.value_used);
}

MDB_TEST(kv_overwrite_larger_reuses_slot_and_grows_by_delta) {
    uint8_t value_a[2] = { 1u, 2u };
    uint8_t value_b[6] = { 9u, 8u, 7u, 6u, 5u, 4u };
    const lox_kv_bucket_t *before;
    const lox_kv_bucket_t *after;
    uint32_t used_before;

    ASSERT_EQ(lox_kv_put(&g_db, "grow", value_a, sizeof(value_a)), LOX_OK);
    before = test_find_bucket("grow");
    used_before = test_core()->kv.value_used;
    ASSERT_EQ(lox_kv_put(&g_db, "grow", value_b, sizeof(value_b)), LOX_OK);
    after = test_find_bucket("grow");
    ASSERT_EQ(before->val_offset, after->val_offset);
    ASSERT_EQ(used_before + 4u, test_core()->kv.value_used);
}

MDB_TEST(kv_key_exact_max_minus_one_is_valid) {
    char key[LOX_KV_KEY_MAX_LEN];
    uint8_t value = 1u;

    memset(key, 'a', sizeof(key));
    key[LOX_KV_KEY_MAX_LEN - 1u] = '\0';
    ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
}

MDB_TEST(kv_key_exact_max_is_invalid) {
    char key[LOX_KV_KEY_MAX_LEN + 1u];
    uint8_t value = 1u;

    memset(key, 'b', sizeof(key));
    key[LOX_KV_KEY_MAX_LEN] = '\0';
    ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_ERR_INVALID);
}

MDB_TEST(kv_set_null_key_invalid) {
    uint8_t value = 1u;
    ASSERT_EQ(lox_kv_put(&g_db, NULL, &value, sizeof(value)), LOX_ERR_INVALID);
}

MDB_TEST(kv_set_empty_key_invalid) {
    uint8_t value = 1u;
    ASSERT_EQ(lox_kv_put(&g_db, "", &value, sizeof(value)), LOX_ERR_INVALID);
}

MDB_TEST(kv_get_null_key_invalid) {
    uint8_t value = 0u;
    ASSERT_EQ(lox_kv_get(&g_db, NULL, &value, sizeof(value), NULL), LOX_ERR_INVALID);
}

MDB_TEST(kv_get_empty_key_invalid) {
    uint8_t value = 0u;
    ASSERT_EQ(lox_kv_get(&g_db, "", &value, sizeof(value), NULL), LOX_ERR_INVALID);
}

MDB_TEST(kv_del_null_key_invalid) {
    ASSERT_EQ(lox_kv_del(&g_db, NULL), LOX_ERR_INVALID);
}

MDB_TEST(kv_del_empty_key_invalid) {
    ASSERT_EQ(lox_kv_del(&g_db, ""), LOX_ERR_INVALID);
}

MDB_TEST(kv_value_exact_max_len_ok) {
    uint8_t in[LOX_KV_VAL_MAX_LEN];
    uint8_t out[LOX_KV_VAL_MAX_LEN];
    size_t out_len = 0u;

    memset(in, 0xAB, sizeof(in));
    ASSERT_EQ(lox_kv_put(&g_db, "maxval", in, sizeof(in)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "maxval", out, sizeof(out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, (long long)sizeof(in));
    ASSERT_MEM_EQ(out, in, sizeof(in));
}

MDB_TEST(kv_value_overflow_is_rejected) {
    uint8_t big[LOX_KV_VAL_MAX_LEN + 1u];
    memset(big, 0xCC, sizeof(big));
    ASSERT_EQ(lox_kv_put(&g_db, "toolarge", big, sizeof(big)), LOX_ERR_OVERFLOW);
}

MDB_TEST(kv_binary_all_zero_roundtrip) {
    uint8_t in[8] = { 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u };
    uint8_t out[8] = { 1u, 1u, 1u, 1u, 1u, 1u, 1u, 1u };
    ASSERT_EQ(lox_kv_put(&g_db, "zeros", in, sizeof(in)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "zeros", out, sizeof(out), NULL), LOX_OK);
    ASSERT_MEM_EQ(out, in, sizeof(in));
}

MDB_TEST(kv_binary_all_ff_roundtrip) {
    uint8_t in[8];
    uint8_t out[8];

    memset(in, 0xFF, sizeof(in));
    memset(out, 0x00, sizeof(out));
    ASSERT_EQ(lox_kv_put(&g_db, "ffs", in, sizeof(in)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "ffs", out, sizeof(out), NULL), LOX_OK);
    ASSERT_MEM_EQ(out, in, sizeof(in));
}

MDB_TEST(kv_ttl_zero_never_expires) {
    uint8_t value = 3u;

    ASSERT_EQ(lox_kv_set(&g_db, "forever", &value, sizeof(value), 0u), LOX_OK);
    g_mock_time = 5000u;
    ASSERT_EQ(lox_kv_exists(&g_db, "forever"), LOX_OK);
}

MDB_TEST(kv_ttl_one_expires_after_one_second) {
    uint8_t value = 7u;

    ASSERT_EQ(lox_kv_set(&g_db, "ttl1", &value, sizeof(value), 1u), LOX_OK);
    g_mock_time = 1000u;
    ASSERT_EQ(lox_kv_exists(&g_db, "ttl1"), LOX_OK);
    g_mock_time = 1001u;
    ASSERT_EQ(lox_kv_exists(&g_db, "ttl1"), LOX_ERR_EXPIRED);
}

MDB_TEST(kv_get_expired_key_is_read_only_until_purge) {
    uint8_t value = 4u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "expire_get", &value, sizeof(value), 5u), LOX_OK);
    g_mock_time = 1006u;
    ASSERT_EQ(lox_kv_get(&g_db, "expire_get", &out, sizeof(out), NULL), LOX_ERR_EXPIRED);
    ASSERT_EQ(lox_kv_get(&g_db, "expire_get", &out, sizeof(out), NULL), LOX_ERR_EXPIRED);
    ASSERT_EQ(lox_kv_purge_expired(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "expire_get", &out, sizeof(out), NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_exists_live_key_ok) {
    uint8_t value = 8u;
    ASSERT_EQ(lox_kv_put(&g_db, "live", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_exists(&g_db, "live"), LOX_OK);
}

MDB_TEST(kv_exists_expired_key_returns_expired) {
    uint8_t value = 5u;

    ASSERT_EQ(lox_kv_set(&g_db, "expire_exists", &value, sizeof(value), 2u), LOX_OK);
    g_mock_time = 1003u;
    ASSERT_EQ(lox_kv_exists(&g_db, "expire_exists"), LOX_ERR_EXPIRED);
}

MDB_TEST(kv_purge_expired_removes_all_expired) {
    uint8_t value = 1u;
    iter_ctx_t ctx;

    ASSERT_EQ(lox_kv_set(&g_db, "a", &value, sizeof(value), 1u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "b", &value, sizeof(value), 1u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "c", &value, sizeof(value), 0u), LOX_OK);
    g_mock_time = 1002u;
    ASSERT_EQ(lox_kv_purge_expired(&g_db), LOX_OK);
    ctx.count = 0u;
    ctx.stop_after = 100u;
    ASSERT_EQ(lox_kv_iter(&g_db, count_iter_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 1u);
}

MDB_TEST(kv_iter_visits_all_live_nonexpired_entries) {
    uint8_t value = 1u;
    iter_ctx_t ctx;

    ASSERT_EQ(lox_kv_set(&g_db, "a", &value, sizeof(value), 0u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "b", &value, sizeof(value), 5u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "c", &value, sizeof(value), 0u), LOX_OK);
    g_mock_time = 1006u;
    ctx.count = 0u;
    ctx.stop_after = 100u;
    ASSERT_EQ(lox_kv_iter(&g_db, count_iter_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 2u);
}

MDB_TEST(kv_iter_stops_early_when_callback_returns_false) {
    uint8_t value = 1u;
    iter_ctx_t ctx;

    ASSERT_EQ(lox_kv_put(&g_db, "a", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "b", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "c", &value, sizeof(value)), LOX_OK);
    ctx.count = 0u;
    ctx.stop_after = 2u;
    ASSERT_EQ(lox_kv_iter(&g_db, count_iter_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 2u);
}

MDB_TEST(kv_clear_resets_live_count) {
    uint8_t value = 1u;

    ASSERT_EQ(lox_kv_put(&g_db, "a", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "b", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_clear(&g_db), LOX_OK);
    ASSERT_EQ(test_core()->kv.entry_count, 0u);
}

MDB_TEST(kv_lru_evicts_oldest_entry_when_full) {
    uint8_t value = 9u;
    uint8_t out = 0u;
    char key[16];
    uint32_t i;

    for (i = 0; i < test_kv_capacity(); ++i) {
        test_make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    ASSERT_EQ(lox_kv_put(&g_db, "newest", &value, sizeof(value)), LOX_OK);
    test_make_key(key, sizeof(key), 0u);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_lru_get_refreshes_recentness) {
    uint8_t value = 9u;
    uint8_t out = 0u;
    char key[16];
    uint32_t i;

    for (i = 0; i < test_kv_capacity(); ++i) {
        test_make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    test_make_key(key, sizeof(key), 0u);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "newest", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_OK);
    test_make_key(key, sizeof(key), 1u);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_lru_exists_refreshes_recentness) {
    uint8_t value = 9u;
    uint8_t out = 0u;
    char key[16];
    uint32_t i;

    for (i = 0; i < test_kv_capacity(); ++i) {
        test_make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    test_make_key(key, sizeof(key), 0u);
    ASSERT_EQ(lox_kv_exists(&g_db, key), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "newest", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_OK);
    test_make_key(key, sizeof(key), 1u);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(kv_overflow_reports_required_size) {
    uint8_t value_in[6] = { 1u, 2u, 3u, 4u, 5u, 6u };
    uint8_t value_out[2] = { 0u, 0u };
    size_t out_len = 0u;

    ASSERT_EQ(lox_kv_put(&g_db, "smallbuf", value_in, sizeof(value_in)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "smallbuf", value_out, sizeof(value_out), &out_len), LOX_ERR_OVERFLOW);
    ASSERT_EQ(out_len, 6u);
}

MDB_TEST(kv_compaction_triggers_when_fragmentation_exceeds_half) {
    uint8_t value[8] = { 1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u };
    uint32_t used_after_puts;

    ASSERT_EQ(lox_kv_put(&g_db, "a", value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "b", value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "c", value, sizeof(value)), LOX_OK);
    used_after_puts = test_core()->kv.value_used;
    ASSERT_EQ(used_after_puts, 24u);
    ASSERT_EQ(lox_kv_del(&g_db, "a"), LOX_OK);
    ASSERT_EQ(test_core()->kv.value_used, 24u);
    ASSERT_EQ(lox_kv_del(&g_db, "b"), LOX_OK);
    ASSERT_EQ(test_core()->kv.value_used, 8u);
}

MDB_TEST(kv_live_value_bytes_counter_matches_manual_sum) {
    uint8_t a[5] = { 1u, 2u, 3u, 4u, 5u };
    uint8_t b[3] = { 7u, 8u, 9u };
    uint8_t c[2] = { 9u, 9u };

    ASSERT_EQ(lox_kv_put(&g_db, "k_a", a, sizeof(a)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "k_b", b, sizeof(b)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "k_a", c, sizeof(c)), LOX_OK);
    ASSERT_EQ(lox_kv_del(&g_db, "k_b"), LOX_OK);
    ASSERT_EQ(test_core()->kv.live_value_bytes, test_kv_manual_live_value_bytes());
}

MDB_TEST(kv_hash_prefilter_still_requires_key_match) {
    char key_a[16] = { 0 };
    char key_b[16] = { 0 };
    uint8_t v_a = 1u;
    uint8_t v_b = 2u;
    uint8_t out = 0u;
    size_t out_len = 0u;
    lox_core_t *core = test_core();
    uint32_t i;

    ASSERT_EQ(test_find_probe_pair(key_a, sizeof(key_a), key_b, sizeof(key_b)), 1);
    ASSERT_EQ(lox_kv_put(&g_db, key_a, &v_a, sizeof(v_a)), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, key_b, &v_b, sizeof(v_b)), LOX_OK);

    for (i = 0u; i < core->kv.bucket_count; ++i) {
        lox_kv_bucket_t *bucket = &core->kv.buckets[i];
        if (bucket->state == 1u && strcmp(bucket->key, key_a) == 0) {
            bucket->key_hash = test_kv_hash(key_b);
            break;
        }
    }
    ASSERT_EQ(i < core->kv.bucket_count, 1);

    ASSERT_EQ(lox_kv_get(&g_db, key_b, &out, sizeof(out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, 1u);
    ASSERT_EQ(out, v_b);
}

MDB_TEST(kv_iter_null_callback_invalid) {
    ASSERT_EQ(lox_kv_iter(&g_db, NULL, NULL), LOX_ERR_INVALID);
}

MDB_TEST(kv_set_null_value_invalid) {
    ASSERT_EQ(lox_kv_set(&g_db, "nullval", NULL, 1u, 0u), LOX_ERR_INVALID);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, kv_set_and_get_basic);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_get_nonexistent);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_delete_existing);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_delete_nonexistent);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_overwrite_existing_returns_latest_value);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_overwrite_same_len_reuses_slot_without_growth);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_overwrite_larger_reuses_slot_and_grows_by_delta);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_key_exact_max_minus_one_is_valid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_key_exact_max_is_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_set_null_key_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_set_empty_key_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_get_null_key_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_get_empty_key_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_del_null_key_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_del_empty_key_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_value_exact_max_len_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_value_overflow_is_rejected);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_binary_all_zero_roundtrip);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_binary_all_ff_roundtrip);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_ttl_zero_never_expires);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_ttl_one_expires_after_one_second);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_get_expired_key_is_read_only_until_purge);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_exists_live_key_ok);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_exists_expired_key_returns_expired);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_purge_expired_removes_all_expired);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_iter_visits_all_live_nonexpired_entries);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_iter_stops_early_when_callback_returns_false);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_clear_resets_live_count);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_lru_evicts_oldest_entry_when_full);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_lru_get_refreshes_recentness);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_lru_exists_refreshes_recentness);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_overflow_reports_required_size);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_compaction_triggers_when_fragmentation_exceeds_half);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_live_value_bytes_counter_matches_manual_sum);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_hash_prefilter_still_requires_key_match);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_iter_null_callback_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_set_null_value_invalid);
    return MDB_RESULT();
}
