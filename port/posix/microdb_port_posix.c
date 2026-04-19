// SPDX-License-Identifier: MIT
#include "microdb_port_posix.h"

#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#include <share.h>
#include <sys/stat.h>
#define MICRODB_POSIX_CLOSE _close
#define MICRODB_POSIX_READ _read
#define MICRODB_POSIX_WRITE _write
#define MICRODB_POSIX_LSEEK _lseek
#define MICRODB_POSIX_SYNC _commit
#define MICRODB_POSIX_FLAGS (_O_BINARY | _O_RDWR | _O_CREAT)
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#define MICRODB_POSIX_CLOSE close
#define MICRODB_POSIX_OPEN open
#define MICRODB_POSIX_READ read
#define MICRODB_POSIX_WRITE write
#define MICRODB_POSIX_LSEEK lseek
#define MICRODB_POSIX_SYNC fsync
#define MICRODB_POSIX_FLAGS (O_RDWR | O_CREAT)
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int posix_fd(void *ctx) {
    return (int)(intptr_t)((microdb_port_posix_ctx_t *)ctx)->file;
}

static microdb_err_t microdb_port_posix_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    microdb_port_posix_ctx_t *posix = (microdb_port_posix_ctx_t *)ctx;
    int fd;

    if (posix == NULL || buf == NULL || ((size_t)offset + len) > posix->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    fd = posix_fd(ctx);
    if (fd < 0 || MICRODB_POSIX_LSEEK(fd, (long)offset, SEEK_SET) < 0) {
        return MICRODB_ERR_STORAGE;
    }
    if ((size_t)MICRODB_POSIX_READ(fd, buf, (unsigned int)len) != len) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

static microdb_err_t microdb_port_posix_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    microdb_port_posix_ctx_t *posix = (microdb_port_posix_ctx_t *)ctx;
    int fd;

    if (posix == NULL || buf == NULL || ((size_t)offset + len) > posix->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    fd = posix_fd(ctx);
    if (fd < 0 || MICRODB_POSIX_LSEEK(fd, (long)offset, SEEK_SET) < 0) {
        return MICRODB_ERR_STORAGE;
    }
    if ((size_t)MICRODB_POSIX_WRITE(fd, buf, (unsigned int)len) != len) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

static microdb_err_t microdb_port_posix_erase(void *ctx, uint32_t offset) {
    microdb_port_posix_ctx_t *posix = (microdb_port_posix_ctx_t *)ctx;
    int fd;
    uint8_t *ff;
    uint32_t block_start;

    if (posix == NULL || offset >= posix->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    ff = (uint8_t *)malloc(posix->erase_size);
    if (ff == NULL) {
        return MICRODB_ERR_NO_MEM;
    }
    memset(ff, 0xFF, posix->erase_size);
    block_start = (offset / posix->erase_size) * posix->erase_size;
    fd = posix_fd(ctx);
    if (fd < 0 || MICRODB_POSIX_LSEEK(fd, (long)block_start, SEEK_SET) < 0 ||
        (size_t)MICRODB_POSIX_WRITE(fd, ff, posix->erase_size) != posix->erase_size) {
        free(ff);
        return MICRODB_ERR_STORAGE;
    }
    free(ff);
    return MICRODB_OK;
}

static microdb_err_t microdb_port_posix_sync(void *ctx) {
    microdb_port_posix_ctx_t *posix = (microdb_port_posix_ctx_t *)ctx;
    if (posix == NULL) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_POSIX_SYNC(posix_fd(ctx)) == 0 ? MICRODB_OK : MICRODB_ERR_STORAGE;
}

microdb_err_t microdb_port_posix_init(microdb_storage_t *storage, const char *path, uint32_t capacity) {
    microdb_port_posix_ctx_t *ctx;
    int fd;
    uint8_t ff_block[4096];
    long current_size = 0;
    uint32_t remaining = 0u;
    uint32_t chunk = 0u;

    if (storage == NULL || path == NULL || path[0] == '\0' || capacity == 0u) {
        return MICRODB_ERR_INVALID;
    }

    memset(storage, 0, sizeof(*storage));
    ctx = (microdb_port_posix_ctx_t *)malloc(sizeof(*ctx));
    if (ctx == NULL) {
        return MICRODB_ERR_NO_MEM;
    }
    memset(ctx, 0, sizeof(*ctx));
    if (strlen(path) >= sizeof(ctx->path)) {
        free(ctx);
        return MICRODB_ERR_INVALID;
    }
    memcpy(ctx->path, path, strlen(path) + 1u);
    ctx->capacity = capacity;
    ctx->erase_size = 256u;

#if defined(_WIN32)
    {
        errno_t open_err = _sopen_s(&fd, path, MICRODB_POSIX_FLAGS, _SH_DENYNO, _S_IREAD | _S_IWRITE);
        if (open_err != 0) {
            free(ctx);
            return MICRODB_ERR_STORAGE;
        }
    }
#else
    fd = MICRODB_POSIX_OPEN(path, MICRODB_POSIX_FLAGS, 0666);
    if (fd < 0) {
        free(ctx);
        return MICRODB_ERR_STORAGE;
    }
#endif

    current_size = MICRODB_POSIX_LSEEK(fd, 0L, SEEK_END);
    if (current_size < 0) {
        MICRODB_POSIX_CLOSE(fd);
        free(ctx);
        return MICRODB_ERR_STORAGE;
    }

    if ((uint32_t)current_size < capacity) {
        memset(ff_block, 0xFF, sizeof(ff_block));
        if (MICRODB_POSIX_LSEEK(fd, current_size, SEEK_SET) < 0) {
            MICRODB_POSIX_CLOSE(fd);
            free(ctx);
            return MICRODB_ERR_STORAGE;
        }
        remaining = capacity - (uint32_t)current_size;
        while (remaining > 0u) {
            chunk = (remaining > sizeof(ff_block)) ? (uint32_t)sizeof(ff_block) : remaining;
            if ((size_t)MICRODB_POSIX_WRITE(fd, ff_block, chunk) != chunk) {
                MICRODB_POSIX_CLOSE(fd);
                free(ctx);
                return MICRODB_ERR_STORAGE;
            }
            remaining -= chunk;
        }
    }
    (void)MICRODB_POSIX_SYNC(fd);

    ctx->file = (void *)(intptr_t)fd;
    storage->read = microdb_port_posix_read;
    storage->write = microdb_port_posix_write;
    storage->erase = microdb_port_posix_erase;
    storage->sync = microdb_port_posix_sync;
    storage->capacity = capacity;
    storage->erase_size = ctx->erase_size;
    storage->write_size = 1u;
    storage->ctx = ctx;
    return MICRODB_OK;
}

void microdb_port_posix_deinit(microdb_storage_t *storage) {
    microdb_port_posix_ctx_t *ctx;

    if (storage == NULL || storage->ctx == NULL) {
        return;
    }

    ctx = (microdb_port_posix_ctx_t *)storage->ctx;
    if (ctx->file != NULL) {
        MICRODB_POSIX_CLOSE(posix_fd(ctx));
    }
    free(ctx);
    memset(storage, 0, sizeof(*storage));
}

void microdb_port_posix_simulate_power_loss(microdb_storage_t *storage) {
    microdb_port_posix_ctx_t *ctx;
    if (storage == NULL || storage->ctx == NULL) {
        return;
    }

    ctx = (microdb_port_posix_ctx_t *)storage->ctx;
    if (ctx->file != NULL) {
        (void)MICRODB_POSIX_CLOSE(posix_fd(ctx));
    }
    ctx->file = NULL;
}

void microdb_port_posix_remove(const char *path) {
    if (path != NULL) {
        (void)remove(path);
    }
}
