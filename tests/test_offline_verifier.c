// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"

#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include <process.h>
#else
#include <sys/wait.h>
#endif

static char g_verify_tool[512];
static unsigned g_seq = 0u;

typedef struct {
    uint32_t wal_size;
    uint32_t super_a_offset;
    uint32_t super_b_offset;
    uint32_t bank_a_offset;
    uint32_t bank_b_offset;
    uint32_t total_size;
} verifier_layout_t;

static int normalize_system_exit(int rc) {
#ifdef _WIN32
    return rc;
#else
    if (rc < 0) {
        return rc;
    }
    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }
    return rc;
#endif
}

static void strip_outer_quotes(char *s) {
    size_t len;
    if (s == NULL || s[0] == '\0') {
        return;
    }
    len = strlen(s);
    if (len == 0u) {
        return;
    }
    if (s[0] == '"' || s[0] == '\'') {
        memmove(s, s + 1, len);
        len = strlen(s);
    }
    if (len > 0u && (s[len - 1u] == '"' || s[len - 1u] == '\'')) {
        s[len - 1u] = '\0';
    }
}

static void set_verify_tool_path(const char *argv0) {
#ifdef MICRODB_VERIFY_TOOL_PATH
    (void)argv0;
    memset(g_verify_tool, 0, sizeof(g_verify_tool));
    (void)snprintf(g_verify_tool, sizeof(g_verify_tool), "%s", MICRODB_VERIFY_TOOL_PATH);
    strip_outer_quotes(g_verify_tool);
    return;
#else
    const char *slash = NULL;
    size_t dir_len = 0u;
    memset(g_verify_tool, 0, sizeof(g_verify_tool));
    if (argv0 == NULL || argv0[0] == '\0') {
#ifdef _WIN32
        (void)snprintf(g_verify_tool, sizeof(g_verify_tool), ".\\microdb_verify.exe");
#else
        (void)snprintf(g_verify_tool, sizeof(g_verify_tool), "./microdb_verify");
#endif
        return;
    }

    slash = strrchr(argv0, '/');
#ifdef _WIN32
    {
        const char *backslash = strrchr(argv0, '\\');
        if (backslash != NULL && (slash == NULL || backslash > slash)) {
            slash = backslash;
        }
    }
#endif
    if (slash != NULL) {
        dir_len = (size_t)(slash - argv0);
    }
    if (dir_len == 0u) {
        dir_len = 1u;
        g_verify_tool[0] = '.';
    } else if (dir_len < sizeof(g_verify_tool)) {
        memcpy(g_verify_tool, argv0, dir_len);
    } else {
        dir_len = sizeof(g_verify_tool) - 1u;
        memcpy(g_verify_tool, argv0, dir_len);
    }
#ifdef _WIN32
    (void)snprintf(g_verify_tool + dir_len, sizeof(g_verify_tool) - dir_len, "\\microdb_verify.exe");
#else
    (void)snprintf(g_verify_tool + dir_len, sizeof(g_verify_tool) - dir_len, "/microdb_verify");
#endif
    strip_outer_quotes(g_verify_tool);
#endif
}

static uint32_t align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static void compute_layout_32kb(verifier_layout_t *out) {
    uint32_t erase_size = 256u;
    uint32_t total_bytes = 32u * 1024u;
    uint32_t kv_bytes = (total_bytes * 40u) / 100u;
    uint32_t ts_bytes = (total_bytes * 40u) / 100u;
    uint32_t rel_bytes = total_bytes - kv_bytes - ts_bytes;
    uint32_t max_key_len = (MICRODB_KV_KEY_MAX_LEN > 0u) ? (MICRODB_KV_KEY_MAX_LEN - 1u) : 0u;
    uint32_t per_entry = 1u + max_key_len + 4u + MICRODB_KV_VAL_MAX_LEN + 4u;
    uint32_t max_entries = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    uint32_t kv_payload_max = max_entries * per_entry;
    uint32_t kv_size = align_u32(kv_payload_max + 32u, erase_size);
    uint32_t ts_size = align_u32(ts_bytes + 32u, erase_size);
    uint32_t rel_size = align_u32(rel_bytes + 32u, erase_size);
    uint32_t bank_size = kv_size + ts_size + rel_size;
    uint32_t wal_size = erase_size * 8u;
    uint32_t super_a = wal_size;
    uint32_t super_b = super_a + erase_size;
    uint32_t bank_a = super_b + erase_size;
    uint32_t bank_b = bank_a + bank_size;

    out->wal_size = wal_size;
    out->super_a_offset = super_a;
    out->super_b_offset = super_b;
    out->bank_a_offset = bank_a;
    out->bank_b_offset = bank_b;
    out->total_size = bank_b + bank_size;
}

static int run_verify(const char *path) {
#ifdef _WIN32
    const char *args[] = {
        g_verify_tool,
        "--image",
        path,
        "--ram-kb",
        "32",
        "--kv-pct",
        "40",
        "--ts-pct",
        "40",
        "--rel-pct",
        "20",
        "--erase-size",
        "256",
        "--json",
        NULL
    };
    intptr_t rc = _spawnv(_P_WAIT, g_verify_tool, args);
    if (rc < 0) {
        return 1;
    }
    return (int)rc;
#else
    char cmd[1200];
    int rc;
    (void)snprintf(cmd,
                   sizeof(cmd),
                   "\"%s\" --image \"%s\" --ram-kb 32 --kv-pct 40 --ts-pct 40 --rel-pct 20 --erase-size 256 --json",
                   g_verify_tool,
                   path);
    rc = system(cmd);
    return normalize_system_exit(rc);
#endif
}

static void create_uninitialized_image(const char *path, uint32_t capacity) {
    microdb_storage_t storage;
    memset(&storage, 0, sizeof(storage));
    microdb_port_posix_remove(path);
    ASSERT_EQ(microdb_port_posix_init(&storage, path, capacity), MICRODB_OK);
    microdb_port_posix_deinit(&storage);
}

static void create_valid_image(const char *path, uint32_t capacity) {
    microdb_storage_t storage;
    microdb_cfg_t cfg;
    microdb_t db;
    uint8_t value = 42u;

    memset(&storage, 0, sizeof(storage));
    memset(&cfg, 0, sizeof(cfg));
    memset(&db, 0, sizeof(db));
    microdb_port_posix_remove(path);
    ASSERT_EQ(microdb_port_posix_init(&storage, path, capacity), MICRODB_OK);
    cfg.storage = &storage;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&db, "k", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
    microdb_port_posix_deinit(&storage);
}

static void corrupt_both_bank_kv_headers(const char *path) {
    verifier_layout_t l;
    FILE *fp;
    uint8_t zero = 0u;
    compute_layout_32kb(&l);

    fp = fopen(path, "rb+");
    ASSERT_EQ(fp != NULL, 1);
    ASSERT_EQ(fseek(fp, (long)l.bank_a_offset, SEEK_SET), 0);
    ASSERT_EQ(fwrite(&zero, 1u, 1u, fp), 1u);
    ASSERT_EQ(fseek(fp, (long)l.bank_b_offset, SEEK_SET), 0);
    ASSERT_EQ(fwrite(&zero, 1u, 1u, fp), 1u);
    ASSERT_EQ(fclose(fp), 0);
}

MDB_TEST(test_verifier_uninitialized_exit_5) {
    char path[128];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_uninit_%u.bin", g_seq++);
    create_uninitialized_image(path, 262144u);
    rc = run_verify(path);
    ASSERT_EQ(rc, 5);
    microdb_port_posix_remove(path);
}

MDB_TEST(test_verifier_valid_exit_0) {
    char path[128];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_valid_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    rc = run_verify(path);
    ASSERT_EQ(rc, 0);
    microdb_port_posix_remove(path);
}

MDB_TEST(test_verifier_corrupt_exit_4) {
    char path[128];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_corrupt_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    corrupt_both_bank_kv_headers(path);
    rc = run_verify(path);
    ASSERT_EQ(rc, 4);
    microdb_port_posix_remove(path);
}

static void setup_noop(void) {}
static void teardown_noop(void) {}

int main(int argc, char **argv) {
    (void)argc;
    set_verify_tool_path((argv != NULL) ? argv[0] : NULL);
    MDB_RUN_TEST(setup_noop, teardown_noop, test_verifier_uninitialized_exit_5);
    MDB_RUN_TEST(setup_noop, teardown_noop, test_verifier_valid_exit_0);
    MDB_RUN_TEST(setup_noop, teardown_noop, test_verifier_corrupt_exit_4);
    return MDB_RESULT();
}
