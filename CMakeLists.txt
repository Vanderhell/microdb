cmake_minimum_required(VERSION 3.16)
project(microdb C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
option(MICRODB_BUILD_TESTS "Build tests" ON)

add_library(microdb
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
)

target_include_directories(microdb PUBLIC include port/ram PRIVATE src)

add_library(microdb_reject STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
)

target_include_directories(microdb_reject PUBLIC include port/ram PRIVATE src)
target_compile_definitions(microdb_reject PRIVATE MICRODB_KV_OVERFLOW_POLICY=MICRODB_KV_POLICY_REJECT)

add_library(microdb_ts_reject STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
)

target_include_directories(microdb_ts_reject PUBLIC include port/ram PRIVATE src)
target_compile_definitions(microdb_ts_reject PRIVATE MICRODB_TS_OVERFLOW_POLICY=MICRODB_TS_POLICY_REJECT)

add_library(microdb_ts_downsample STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
)

target_include_directories(microdb_ts_downsample PUBLIC include port/ram PRIVATE src)
target_compile_definitions(microdb_ts_downsample PRIVATE MICRODB_TS_OVERFLOW_POLICY=MICRODB_TS_POLICY_DOWNSAMPLE)

if(MSVC)
    target_compile_options(microdb PRIVATE /W4)
    target_compile_options(microdb_reject PRIVATE /W4)
    target_compile_options(microdb_ts_reject PRIVATE /W4)
    target_compile_options(microdb_ts_downsample PRIVATE /W4)
else()
    target_compile_options(microdb PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_reject PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_ts_reject PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_ts_downsample PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(MICRODB_BUILD_TESTS)
    enable_testing()
    add_executable(test_kv tests/test_kv.c)
    target_include_directories(test_kv PRIVATE include tests port/ram src)
    target_link_libraries(test_kv PRIVATE microdb)
    add_test(NAME test_kv COMMAND test_kv)

    add_executable(test_kv_reject tests/test_kv_reject.c)
    target_include_directories(test_kv_reject PRIVATE include tests port/ram src)
    target_link_libraries(test_kv_reject PRIVATE microdb_reject)
    add_test(NAME test_kv_reject COMMAND test_kv_reject)

    add_executable(test_ts tests/test_ts.c)
    target_include_directories(test_ts PRIVATE include tests port/ram src)
    target_link_libraries(test_ts PRIVATE microdb)
    add_test(NAME test_ts COMMAND test_ts)

    add_executable(test_ts_reject tests/test_ts_reject.c)
    target_include_directories(test_ts_reject PRIVATE include tests port/ram src)
    target_link_libraries(test_ts_reject PRIVATE microdb_ts_reject)
    add_test(NAME test_ts_reject COMMAND test_ts_reject)

    add_executable(test_ts_downsample tests/test_ts_downsample.c)
    target_include_directories(test_ts_downsample PRIVATE include tests port/ram src)
    target_link_libraries(test_ts_downsample PRIVATE microdb_ts_downsample)
    add_test(NAME test_ts_downsample COMMAND test_ts_downsample)

    add_executable(test_rel tests/test_rel.c)
    target_include_directories(test_rel PRIVATE include tests port/ram src)
    target_link_libraries(test_rel PRIVATE microdb)
    add_test(NAME test_rel COMMAND test_rel)
endif()
