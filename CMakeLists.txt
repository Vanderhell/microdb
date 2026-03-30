cmake_minimum_required(VERSION 3.16)
project(microdb VERSION 1.0.0 LANGUAGES C)

include(GNUInstallDirs)

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
    port/posix/microdb_port_posix.c
)

target_include_directories(
    microdb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/port/ram>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/port/posix>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/microdb/port>
    PRIVATE
        src
)
set_target_properties(microdb PROPERTIES PUBLIC_HEADER include/microdb.h)

add_library(microdb_reject STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_reject PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_reject PRIVATE MICRODB_KV_OVERFLOW_POLICY=MICRODB_KV_POLICY_REJECT)

add_library(microdb_ts_reject STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_ts_reject PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_ts_reject PRIVATE MICRODB_TS_OVERFLOW_POLICY=MICRODB_TS_POLICY_REJECT)

add_library(microdb_ts_downsample STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_ts_downsample PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_ts_downsample PRIVATE MICRODB_TS_OVERFLOW_POLICY=MICRODB_TS_POLICY_DOWNSAMPLE)

add_library(microdb_no_wal STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_no_wal PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_no_wal PRIVATE MICRODB_ENABLE_WAL=0)

add_library(microdb_kv128 STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_kv128 PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_kv128 PRIVATE MICRODB_KV_MAX_KEYS=128)

add_library(microdb_kv_off STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_kv_off PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_kv_off PRIVATE MICRODB_ENABLE_KV=0)

add_library(microdb_ts_off STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_ts_off PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_ts_off PRIVATE MICRODB_ENABLE_TS=0)

add_library(microdb_rel_off STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_rel_off PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_rel_off PRIVATE MICRODB_ENABLE_REL=0)

add_library(microdb_small_limits STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)

target_include_directories(microdb_small_limits PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(
    microdb_small_limits
    PRIVATE
        MICRODB_KV_MAX_KEYS=8
        MICRODB_TS_MAX_STREAMS=2
        MICRODB_REL_MAX_TABLES=1
        MICRODB_REL_MAX_COLS=4
)

function(create_microdb_ram_variant target_name ram_kb)
    add_library(${target_name} STATIC
        src/microdb.c
        src/microdb_kv.c
        src/microdb_ts.c
        src/microdb_rel.c
        src/microdb_wal.c
        src/microdb_crc.c
        port/ram/microdb_port_ram.c
        port/posix/microdb_port_posix.c
    )
    target_include_directories(${target_name} PUBLIC include port/ram port/posix PRIVATE src)
    target_compile_definitions(${target_name} PRIVATE MICRODB_RAM_KB=${ram_kb})
    if(MSVC)
        target_compile_options(${target_name} PRIVATE /W4)
    else()
        target_compile_options(${target_name} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endfunction()

create_microdb_ram_variant(microdb_128kb 128)
create_microdb_ram_variant(microdb_256kb 256)
create_microdb_ram_variant(microdb_512kb 512)
create_microdb_ram_variant(microdb_1024kb 1024)
create_microdb_ram_variant(microdb_64kb 64)

add_library(microdb_kv_only STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)
target_include_directories(microdb_kv_only PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_kv_only PRIVATE MICRODB_ENABLE_TS=0 MICRODB_ENABLE_REL=0)

add_library(microdb_ts_only STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)
target_include_directories(microdb_ts_only PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_ts_only PRIVATE MICRODB_ENABLE_KV=0 MICRODB_ENABLE_REL=0)

add_library(microdb_rel_only STATIC
    src/microdb.c
    src/microdb_kv.c
    src/microdb_ts.c
    src/microdb_rel.c
    src/microdb_wal.c
    src/microdb_crc.c
    port/ram/microdb_port_ram.c
    port/posix/microdb_port_posix.c
)
target_include_directories(microdb_rel_only PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(microdb_rel_only PRIVATE MICRODB_ENABLE_KV=0 MICRODB_ENABLE_TS=0)

if(MSVC)
    target_compile_options(microdb PRIVATE /W4)
    target_compile_options(microdb_reject PRIVATE /W4)
    target_compile_options(microdb_ts_reject PRIVATE /W4)
    target_compile_options(microdb_ts_downsample PRIVATE /W4)
    target_compile_options(microdb_no_wal PRIVATE /W4)
    target_compile_options(microdb_kv128 PRIVATE /W4)
    target_compile_options(microdb_kv_off PRIVATE /W4)
    target_compile_options(microdb_ts_off PRIVATE /W4)
    target_compile_options(microdb_rel_off PRIVATE /W4)
    target_compile_options(microdb_small_limits PRIVATE /W4)
    target_compile_options(microdb_kv_only PRIVATE /W4)
    target_compile_options(microdb_ts_only PRIVATE /W4)
    target_compile_options(microdb_rel_only PRIVATE /W4)
else()
    target_compile_options(microdb PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_reject PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_ts_reject PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_ts_downsample PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_no_wal PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_kv128 PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_kv_off PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_ts_off PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_rel_off PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_small_limits PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_kv_only PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_ts_only PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(microdb_rel_only PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(
    TARGETS microdb
    EXPORT microdbTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(
    FILES
        port/ram/microdb_port_ram.h
        port/posix/microdb_port_posix.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/microdb/port
)

install(
    FILES
        README.md
        LICENSE
    DESTINATION ${CMAKE_INSTALL_DATADIR}/microdb
)

install(
    EXPORT microdbTargets
    FILE microdbTargets.cmake
    NAMESPACE microdb::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/microdb
)

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

    add_executable(test_wal tests/test_wal.c)
    target_include_directories(test_wal PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_wal PRIVATE microdb)
    add_test(NAME test_wal COMMAND test_wal)

    add_executable(test_wal_no_wal tests/test_wal_no_wal.c)
    target_include_directories(test_wal_no_wal PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_wal_no_wal PRIVATE microdb_no_wal)
    add_test(NAME test_wal_no_wal COMMAND test_wal_no_wal)

    add_executable(test_wal_kv128 tests/test_wal_kv128.c)
    target_include_directories(test_wal_kv128 PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_wal_kv128 PRIVATE microdb_kv128)
    add_test(NAME test_wal_kv128 COMMAND test_wal_kv128)

    add_executable(test_integration tests/test_integration.c)
    target_include_directories(test_integration PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_integration PRIVATE MICRODB_TEST_DB_PATH="integration_default.bin")
    target_link_libraries(test_integration PRIVATE microdb)
    add_test(NAME test_integration COMMAND test_integration)

    add_executable(test_integration_8kb tests/test_integration.c)
    target_include_directories(test_integration_8kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_8kb
        PRIVATE
            MICRODB_TEST_WITH_8KB=1
            MICRODB_TEST_DB_PATH="integration_8kb.bin"
            MICRODB_KV_MAX_KEYS=8
            MICRODB_TS_MAX_STREAMS=2
            MICRODB_REL_MAX_TABLES=1
            MICRODB_REL_MAX_COLS=4
    )
    target_link_libraries(test_integration_8kb PRIVATE microdb_small_limits)
    add_test(NAME test_integration_8kb COMMAND test_integration_8kb)

    foreach(RAM_KB 128 256 512 1024)
        add_executable(test_integration_${RAM_KB}kb tests/test_integration.c)
        target_include_directories(test_integration_${RAM_KB}kb PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_integration_${RAM_KB}kb
            PRIVATE
                MICRODB_TEST_LARGE_RAM_VARIANT=1
                MICRODB_RAM_KB=${RAM_KB}
                MICRODB_TEST_DB_PATH="integration_${RAM_KB}kb.bin"
        )
        target_link_libraries(test_integration_${RAM_KB}kb PRIVATE microdb_${RAM_KB}kb)
        add_test(NAME integration_${RAM_KB}kb COMMAND test_integration_${RAM_KB}kb)
    endforeach()

    add_executable(test_integration_kv_off tests/test_integration.c)
    target_include_directories(test_integration_kv_off PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_kv_off
        PRIVATE
            MICRODB_ENABLE_KV=0
            MICRODB_TEST_DB_PATH="integration_kv_off.bin"
    )
    target_link_libraries(test_integration_kv_off PRIVATE microdb_kv_off)
    add_test(NAME test_integration_kv_off COMMAND test_integration_kv_off)

    add_executable(test_integration_ts_off tests/test_integration.c)
    target_include_directories(test_integration_ts_off PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_ts_off
        PRIVATE
            MICRODB_ENABLE_TS=0
            MICRODB_TEST_DB_PATH="integration_ts_off.bin"
    )
    target_link_libraries(test_integration_ts_off PRIVATE microdb_ts_off)
    add_test(NAME test_integration_ts_off COMMAND test_integration_ts_off)

    add_executable(test_integration_rel_off tests/test_integration.c)
    target_include_directories(test_integration_rel_off PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_rel_off
        PRIVATE
            MICRODB_ENABLE_REL=0
            MICRODB_TEST_DB_PATH="integration_rel_off.bin"
    )
    target_link_libraries(test_integration_rel_off PRIVATE microdb_rel_off)
    add_test(NAME test_integration_rel_off COMMAND test_integration_rel_off)

    add_executable(test_limits tests/test_limits.c)
    target_include_directories(test_limits PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_limits PRIVATE microdb)
    add_test(NAME test_limits COMMAND test_limits)

    add_executable(test_limits_32kb tests/test_limits.c)
    target_include_directories(test_limits_32kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_limits_32kb PRIVATE MICRODB_RAM_KB=32)
    target_link_libraries(test_limits_32kb PRIVATE microdb)
    add_test(NAME test_limits_32kb COMMAND test_limits_32kb)

    add_executable(test_limits_64kb tests/test_limits.c)
    target_include_directories(test_limits_64kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_limits_64kb PRIVATE MICRODB_RAM_KB=64)
    target_link_libraries(test_limits_64kb PRIVATE microdb_64kb)
    add_test(NAME test_limits_64kb COMMAND test_limits_64kb)

    add_executable(test_limits_8kb tests/test_limits.c)
    target_include_directories(test_limits_8kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_8kb
        PRIVATE
            MICRODB_TEST_WITH_8KB=1
            MICRODB_KV_MAX_KEYS=8
            MICRODB_TS_MAX_STREAMS=2
            MICRODB_REL_MAX_TABLES=1
            MICRODB_REL_MAX_COLS=4
    )
    target_link_libraries(test_limits_8kb PRIVATE microdb_small_limits)
    add_test(NAME test_limits_8kb COMMAND test_limits_8kb)

    foreach(RAM_KB 128 256 512 1024)
        add_executable(test_limits_${RAM_KB}kb tests/test_limits.c)
        target_include_directories(test_limits_${RAM_KB}kb PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(test_limits_${RAM_KB}kb PRIVATE MICRODB_RAM_KB=${RAM_KB})
        target_link_libraries(test_limits_${RAM_KB}kb PRIVATE microdb_${RAM_KB}kb)
        add_test(NAME limits_${RAM_KB}kb COMMAND test_limits_${RAM_KB}kb)
    endforeach()

    add_executable(test_limits_kv_only tests/test_limits.c)
    target_include_directories(test_limits_kv_only PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_kv_only
        PRIVATE
            MICRODB_ENABLE_TS=0
            MICRODB_ENABLE_REL=0
            MICRODB_TEST_LIMITS_KV_ONLY=1
    )
    target_link_libraries(test_limits_kv_only PRIVATE microdb_kv_only)
    add_test(NAME test_limits_kv_only COMMAND test_limits_kv_only)

    add_executable(test_limits_ts_only tests/test_limits.c)
    target_include_directories(test_limits_ts_only PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_ts_only
        PRIVATE
            MICRODB_ENABLE_KV=0
            MICRODB_ENABLE_REL=0
            MICRODB_TEST_LIMITS_TS_ONLY=1
    )
    target_link_libraries(test_limits_ts_only PRIVATE microdb_ts_only)
    add_test(NAME test_limits_ts_only COMMAND test_limits_ts_only)

    add_executable(test_limits_rel_only tests/test_limits.c)
    target_include_directories(test_limits_rel_only PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_rel_only
        PRIVATE
            MICRODB_ENABLE_KV=0
            MICRODB_ENABLE_TS=0
            MICRODB_TEST_LIMITS_REL_ONLY=1
    )
    target_link_libraries(test_limits_rel_only PRIVATE microdb_rel_only)
    add_test(NAME test_limits_rel_only COMMAND test_limits_rel_only)

    add_test(
        NAME test_limits_pct_sum_invalid
        COMMAND ${CMAKE_COMMAND}
            -DCC=${CMAKE_C_COMPILER}
            -DCOMPILER_ID=${CMAKE_C_COMPILER_ID}
            -DINCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/include
            -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/invalid_pct_sum.c
            -DOBJ=${CMAKE_CURRENT_BINARY_DIR}/invalid_pct_sum.obj
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_invalid_pct_sum.cmake
    )
endif()

if(DEFINED IDF_TARGET)
    idf_component_register(
        SRCS
            "src/microdb.c"
            "src/microdb_kv.c"
            "src/microdb_ts.c"
            "src/microdb_rel.c"
            "src/microdb_wal.c"
            "src/microdb_crc.c"
            "port/esp32/microdb_port_esp32.c"
        INCLUDE_DIRS
            "include"
            "port/esp32"
        REQUIRES
            esp_partition
    )
endif()
