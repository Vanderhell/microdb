#include "microtest.h"
#include "microdb.h"

#include <string.h>

static void no_setup(void) {
}

static void no_teardown(void) {
}

MDB_TEST(test_known_error_names) {
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_OK), "MICRODB_OK"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_INVALID), "MICRODB_ERR_INVALID"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_NO_MEM), "MICRODB_ERR_NO_MEM"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_FULL), "MICRODB_ERR_FULL"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_NOT_FOUND), "MICRODB_ERR_NOT_FOUND"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_EXPIRED), "MICRODB_ERR_EXPIRED"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_STORAGE), "MICRODB_ERR_STORAGE"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_CORRUPT), "MICRODB_ERR_CORRUPT"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_SEALED), "MICRODB_ERR_SEALED"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_EXISTS), "MICRODB_ERR_EXISTS"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_DISABLED), "MICRODB_ERR_DISABLED"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_OVERFLOW), "MICRODB_ERR_OVERFLOW"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_SCHEMA), "MICRODB_ERR_SCHEMA"), 0);
    ASSERT_EQ(strcmp(microdb_err_to_string(MICRODB_ERR_TXN_ACTIVE), "MICRODB_ERR_TXN_ACTIVE"), 0);
}

MDB_TEST(test_unknown_error_name) {
    ASSERT_EQ(strcmp(microdb_err_to_string((microdb_err_t)-12345), "MICRODB_ERR_UNKNOWN"), 0);
}

int main(void) {
    MDB_RUN_TEST(no_setup, no_teardown, test_known_error_names);
    MDB_RUN_TEST(no_setup, no_teardown, test_unknown_error_name);
    return MDB_RESULT();
}
