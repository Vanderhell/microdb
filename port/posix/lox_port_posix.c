// SPDX-License-Identifier: MIT
#include "lox_port_posix.h"

#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#include <share.h>
#include <sys/stat.h>
#define LOX_POSIX_CLOSE _close
#define LOX_POSIX_READ _read
#define LOX_POSIX_WRITE _write
#define LOX_POSIX_LSEEK _lseek
#define LOX_POSIX_SYNC _commit
#define LOX_POSIX_FLAGS (_O_BINARY | _O_RDWR | _O_CREAT)
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#define LOX_POSIX_CLOSE close
#define LOX_POSIX_OPEN open
#define LOX_POSIX_READ read
#define LOX_POSIX_WRITE write
#define LOX_POSIX_LSEEK lseek
#define LOX_POSIX_SYNC fsync
#define LOX_POSIX_FLAGS (O_RDWR | O_CREAT)
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int posix_fd(void *ctx) {
    return (int)(intptr_t)((lox_port_posix_ctx_t *)ctx)->file;
}

static lox_err_t lox_port_posix_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    lox_port_posix_ctx_t *posix = (lox_port_posix_ctx_t *)ctx;
    int fd;

    if (posix == NULL || buf == NULL || ((size_t)offset + len) > posix->capacity) {
        return LOX_ERR_STORAGE;
    }

    fd = posix_fd(ctx);
    if (fd < 0 || LOX_POSIX_LSEEK(fd, (long)offset, SEEK_SET) < 0) {
        return LOX_ERR_STORAGE;
    }
    if ((size_t)LOX_POSIX_READ(fd, buf, (unsigned int)len) != len) {
        return LOX_ERR_STORAGE;
    }
    return LOX_OK;
}

static lox_err_t lox_port_posix_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    lox_port_posix_ctx_t *posix = (lox_port_posix_ctx_t *)ctx;
    int fd;

    if (posix == NULL || buf == NULL || ((size_t)offset + len) > posix->capacity) {
        return LOX_ERR_STORAGE;
    }

    fd = posix_fd(ctx);
    if (fd < 0 || LOX_POSIX_LSEEK(fd, (long)offset, SEEK_SET) < 0) {
        return LOX_ERR_STORAGE;
    }
    if ((size_t)LOX_POSIX_WRITE(fd, buf, (unsigned int)len) != len) {
        return LOX_ERR_STORAGE;
    }
    return LOX_OK;
}

static lox_err_t lox_port_posix_erase(void *ctx, uint32_t offset) {
    lox_port_posix_ctx_t *posix = (lox_port_posix_ctx_t *)ctx;
    int fd;
    uint8_t *ff;
    uint32_t block_start;

    if (posix == NULL || offset >= posix->capacity) {
        return LOX_ERR_STORAGE;
    }

    ff = (uint8_t *)malloc(posix->erase_size);
    if (ff == NULL) {
        return LOX_ERR_NO_MEM;
    }
    memset(ff, 0xFF, posix->erase_size);
    block_start = (offset / posix->erase_size) * posix->erase_size;
    fd = posix_fd(ctx);
    if (fd < 0 || LOX_POSIX_LSEEK(fd, (long)block_start, SEEK_SET) < 0 ||
        (size_t)LOX_POSIX_WRITE(fd, ff, posix->erase_size) != posix->erase_size) {
        free(ff);
        return LOX_ERR_STORAGE;
    }
    free(ff);
    return LOX_OK;
}

static lox_err_t lox_port_posix_sync(void *ctx) {
    lox_port_posix_ctx_t *posix = (lox_port_posix_ctx_t *)ctx;
    if (posix == NULL) {
        return LOX_ERR_STORAGE;
    }
    return LOX_POSIX_SYNC(posix_fd(ctx)) == 0 ? LOX_OK : LOX_ERR_STORAGE;
}

lox_err_t lox_port_posix_init(lox_storage_t *storage, const char *path, uint32_t capacity) {
    lox_port_posix_ctx_t *ctx;
    int fd;
    uint8_t ff_block[4096];
    long current_size = 0;
    uint32_t remaining = 0u;
    uint32_t chunk = 0u;

    if (storage == NULL || path == NULL || path[0] == '\0' || capacity == 0u) {
        return LOX_ERR_INVALID;
    }

    memset(storage, 0, sizeof(*storage));
    ctx = (lox_port_posix_ctx_t *)malloc(sizeof(*ctx));
    if (ctx == NULL) {
        return LOX_ERR_NO_MEM;
    }
    memset(ctx, 0, sizeof(*ctx));
    if (strlen(path) >= sizeof(ctx->path)) {
        free(ctx);
        return LOX_ERR_INVALID;
    }
    memcpy(ctx->path, path, strlen(path) + 1u);
    ctx->capacity = capacity;
    ctx->erase_size = 256u;

#if defined(_WIN32)
    {
        errno_t open_err = _sopen_s(&fd, path, LOX_POSIX_FLAGS, _SH_DENYNO, _S_IREAD | _S_IWRITE);
        if (open_err != 0) {
            free(ctx);
            return LOX_ERR_STORAGE;
        }
    }
#else
    fd = LOX_POSIX_OPEN(path, LOX_POSIX_FLAGS, 0666);
    if (fd < 0) {
        free(ctx);
        return LOX_ERR_STORAGE;
    }
#endif

    current_size = LOX_POSIX_LSEEK(fd, 0L, SEEK_END);
    if (current_size < 0) {
        LOX_POSIX_CLOSE(fd);
        free(ctx);
        return LOX_ERR_STORAGE;
    }

    if ((uint32_t)current_size < capacity) {
        memset(ff_block, 0xFF, sizeof(ff_block));
        if (LOX_POSIX_LSEEK(fd, current_size, SEEK_SET) < 0) {
            LOX_POSIX_CLOSE(fd);
            free(ctx);
            return LOX_ERR_STORAGE;
        }
        remaining = capacity - (uint32_t)current_size;
        while (remaining > 0u) {
            chunk = (remaining > sizeof(ff_block)) ? (uint32_t)sizeof(ff_block) : remaining;
            if ((size_t)LOX_POSIX_WRITE(fd, ff_block, chunk) != chunk) {
                LOX_POSIX_CLOSE(fd);
                free(ctx);
                return LOX_ERR_STORAGE;
            }
            remaining -= chunk;
        }
    }
    (void)LOX_POSIX_SYNC(fd);

    ctx->file = (void *)(intptr_t)fd;
    storage->read = lox_port_posix_read;
    storage->write = lox_port_posix_write;
    storage->erase = lox_port_posix_erase;
    storage->sync = lox_port_posix_sync;
    storage->capacity = capacity;
    storage->erase_size = ctx->erase_size;
    storage->write_size = 1u;
    storage->ctx = ctx;
    return LOX_OK;
}

void lox_port_posix_deinit(lox_storage_t *storage) {
    lox_port_posix_ctx_t *ctx;

    if (storage == NULL || storage->ctx == NULL) {
        return;
    }

    ctx = (lox_port_posix_ctx_t *)storage->ctx;
    if (ctx->file != NULL) {
        LOX_POSIX_CLOSE(posix_fd(ctx));
    }
    free(ctx);
    memset(storage, 0, sizeof(*storage));
}

void lox_port_posix_simulate_power_loss(lox_storage_t *storage) {
    lox_port_posix_ctx_t *ctx;
    if (storage == NULL || storage->ctx == NULL) {
        return;
    }

    ctx = (lox_port_posix_ctx_t *)storage->ctx;
    if (ctx->file != NULL) {
        (void)LOX_POSIX_CLOSE(posix_fd(ctx));
    }
    ctx->file = NULL;
}

void lox_port_posix_remove(const char *path) {
    if (path != NULL) {
        (void)remove(path);
    }
}
