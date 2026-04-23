// SPDX-License-Identifier: MIT
#include "lox.h"

/* Replace include path with your FatFS integration header location. */
#include "ff.h"

#include <stdint.h>
#include <string.h>

typedef struct {
    FIL file;
    uint32_t capacity;
    uint32_t erase_size;
    uint8_t opened;
} sd_file_ctx_t;

static lox_err_t sd_read(void *ctx, uint32_t off, void *buf, size_t len) {
    sd_file_ctx_t *s = (sd_file_ctx_t *)ctx;
    UINT br = 0u;
    if (s == NULL || !s->opened || buf == NULL) return LOX_ERR_INVALID;
    if ((uint64_t)off + (uint64_t)len > (uint64_t)s->capacity) return LOX_ERR_INVALID;
    if (f_lseek(&s->file, off) != FR_OK) return LOX_ERR_STORAGE;
    if (f_read(&s->file, buf, (UINT)len, &br) != FR_OK) return LOX_ERR_STORAGE;
    return (br == (UINT)len) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t sd_write(void *ctx, uint32_t off, const void *buf, size_t len) {
    sd_file_ctx_t *s = (sd_file_ctx_t *)ctx;
    UINT bw = 0u;
    if (s == NULL || !s->opened || buf == NULL) return LOX_ERR_INVALID;
    if ((uint64_t)off + (uint64_t)len > (uint64_t)s->capacity) return LOX_ERR_INVALID;
    if (f_lseek(&s->file, off) != FR_OK) return LOX_ERR_STORAGE;
    if (f_write(&s->file, buf, (UINT)len, &bw) != FR_OK) return LOX_ERR_STORAGE;
    return (bw == (UINT)len) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t sd_erase(void *ctx, uint32_t off) {
    sd_file_ctx_t *s = (sd_file_ctx_t *)ctx;
    uint8_t ff[256];
    uint32_t remaining;
    if (s == NULL || !s->opened) return LOX_ERR_INVALID;
    if (s->erase_size == 0u || (off % s->erase_size) != 0u) return LOX_ERR_INVALID;
    if ((uint64_t)off + (uint64_t)s->erase_size > (uint64_t)s->capacity) return LOX_ERR_INVALID;

    memset(ff, 0xFF, sizeof(ff));
    if (f_lseek(&s->file, off) != FR_OK) return LOX_ERR_STORAGE;
    remaining = s->erase_size;
    while (remaining > 0u) {
        UINT bw = 0u;
        UINT chunk = (remaining > sizeof(ff)) ? (UINT)sizeof(ff) : (UINT)remaining;
        if (f_write(&s->file, ff, chunk, &bw) != FR_OK || bw != chunk) {
            return LOX_ERR_STORAGE;
        }
        remaining -= chunk;
    }
    return LOX_OK;
}

static lox_err_t sd_sync(void *ctx) {
    sd_file_ctx_t *s = (sd_file_ctx_t *)ctx;
    if (s == NULL || !s->opened) return LOX_ERR_INVALID;
    return (f_sync(&s->file) == FR_OK) ? LOX_OK : LOX_ERR_STORAGE;
}

/* Example bring-up flow:
 * 1) mount FatFS volume
 * 2) open/create fixed-size file (loxdb.bin)
 * 3) zero/expand file to desired capacity
 * 4) wire lox_storage_t and call lox_init
 */
lox_err_t sd_fatfs_lox_init(lox_t *db, FATFS *fs, sd_file_ctx_t *ctx) {
    lox_cfg_t cfg;
    lox_storage_t st;
    FRESULT fr;

    if (db == NULL || fs == NULL || ctx == NULL) return LOX_ERR_INVALID;
    memset(ctx, 0, sizeof(*ctx));
    ctx->capacity = 512u * 1024u;
    ctx->erase_size = 512u;

    fr = f_mount(fs, "", 1);
    if (fr != FR_OK) return LOX_ERR_STORAGE;

    fr = f_open(&ctx->file, "loxdb.bin", FA_READ | FA_WRITE | FA_OPEN_ALWAYS);
    if (fr != FR_OK) return LOX_ERR_STORAGE;
    ctx->opened = 1u;

    memset(&st, 0, sizeof(st));
    st.ctx = ctx;
    st.read = sd_read;
    st.write = sd_write;
    st.erase = sd_erase;
    st.sync = sd_sync;
    st.capacity = ctx->capacity;
    st.erase_size = ctx->erase_size;
    st.write_size = 1u;

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    return lox_init(db, &cfg);
}
