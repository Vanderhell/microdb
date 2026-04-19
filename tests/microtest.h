// SPDX-License-Identifier: MIT
#ifndef MICROTEST_H
#define MICROTEST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int mdb_test_failures = 0;
static int mdb_test_total = 0;

#define MDB_TEST(name) static void name(void)

#define ASSERT_EQ(actual, expected)                                                         \
    do {                                                                                    \
        long long mdb_actual_value = (long long)(actual);                                   \
        long long mdb_expected_value = (long long)(expected);                               \
        if (mdb_actual_value != mdb_expected_value) {                                       \
            fprintf(stderr, "%s:%d ASSERT_EQ failed: got %lld expected %lld\n",             \
                    __FILE__, __LINE__, mdb_actual_value, mdb_expected_value);              \
            mdb_test_failures++;                                                            \
            return;                                                                         \
        }                                                                                   \
    } while (0)

#define ASSERT_GT(actual, expected)                                                         \
    do {                                                                                    \
        long long mdb_actual_value = (long long)(actual);                                   \
        long long mdb_expected_value = (long long)(expected);                               \
        if (mdb_actual_value <= mdb_expected_value) {                                       \
            fprintf(stderr, "%s:%d ASSERT_GT failed: got %lld expected > %lld\n",           \
                    __FILE__, __LINE__, mdb_actual_value, mdb_expected_value);              \
            mdb_test_failures++;                                                            \
            return;                                                                         \
        }                                                                                   \
    } while (0)

#define ASSERT_GE(actual, expected)                                                         \
    do {                                                                                    \
        long long mdb_actual_value = (long long)(actual);                                   \
        long long mdb_expected_value = (long long)(expected);                               \
        if (mdb_actual_value < mdb_expected_value) {                                        \
            fprintf(stderr, "%s:%d ASSERT_GE failed: got %lld expected >= %lld\n",          \
                    __FILE__, __LINE__, mdb_actual_value, mdb_expected_value);              \
            mdb_test_failures++;                                                            \
            return;                                                                         \
        }                                                                                   \
    } while (0)

#define ASSERT_LE(actual, expected)                                                         \
    do {                                                                                    \
        long long mdb_actual_value = (long long)(actual);                                   \
        long long mdb_expected_value = (long long)(expected);                               \
        if (mdb_actual_value > mdb_expected_value) {                                        \
            fprintf(stderr, "%s:%d ASSERT_LE failed: got %lld expected <= %lld\n",          \
                    __FILE__, __LINE__, mdb_actual_value, mdb_expected_value);              \
            mdb_test_failures++;                                                            \
            return;                                                                         \
        }                                                                                   \
    } while (0)

#define ASSERT_MEM_EQ(actual, expected, len)                                                \
    do {                                                                                    \
        if (memcmp((actual), (expected), (len)) != 0) {                                     \
            fprintf(stderr, "%s:%d ASSERT_MEM_EQ failed\n", __FILE__, __LINE__);            \
            mdb_test_failures++;                                                            \
            return;                                                                         \
        }                                                                                   \
    } while (0)

#define MDB_RUN_TEST(setup_fn, teardown_fn, test_fn)                                        \
    do {                                                                                    \
        int mdb_before_failures = mdb_test_failures;                                        \
        mdb_test_total++;                                                                   \
        setup_fn();                                                                         \
        if (mdb_test_failures == mdb_before_failures) {                                     \
            test_fn();                                                                      \
        }                                                                                   \
        teardown_fn();                                                                      \
        if (mdb_test_failures == mdb_before_failures) {                                     \
            fprintf(stdout, "[PASS] %s\n", #test_fn);                                       \
        } else {                                                                            \
            fprintf(stdout, "[FAIL] %s\n", #test_fn);                                       \
        }                                                                                   \
    } while (0)

#define MDB_RESULT() ((mdb_test_failures == 0) ? EXIT_SUCCESS : EXIT_FAILURE)

#endif
