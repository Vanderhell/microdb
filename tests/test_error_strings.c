// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"

#include <string.h>

static void no_setup(void) {
}

static void no_teardown(void) {
}

MDB_TEST(test_known_error_names) {
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_OK), "LOX_OK"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_INVALID), "LOX_ERR_INVALID"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_NO_MEM), "LOX_ERR_NO_MEM"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_FULL), "LOX_ERR_FULL"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_NOT_FOUND), "LOX_ERR_NOT_FOUND"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_EXPIRED), "LOX_ERR_EXPIRED"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_STORAGE), "LOX_ERR_STORAGE"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_CORRUPT), "LOX_ERR_CORRUPT"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_SEALED), "LOX_ERR_SEALED"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_EXISTS), "LOX_ERR_EXISTS"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_DISABLED), "LOX_ERR_DISABLED"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_OVERFLOW), "LOX_ERR_OVERFLOW"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_SCHEMA), "LOX_ERR_SCHEMA"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_TXN_ACTIVE), "LOX_ERR_TXN_ACTIVE"), 0);
    ASSERT_EQ(strcmp(lox_err_to_string(LOX_ERR_MODIFIED), "LOX_ERR_MODIFIED"), 0);
}

MDB_TEST(test_unknown_error_name) {
    ASSERT_EQ(strcmp(lox_err_to_string((lox_err_t)-12345), "LOX_ERR_UNKNOWN"), 0);
}

int main(void) {
    MDB_RUN_TEST(no_setup, no_teardown, test_known_error_names);
    MDB_RUN_TEST(no_setup, no_teardown, test_unknown_error_name);
    return MDB_RESULT();
}
