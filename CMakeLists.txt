cmake_minimum_required(VERSION 3.16)
project(loxdb VERSION 1.0.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(LOX_BUILD_TESTS "Build tests" ON)
option(LOX_BUILD_TOOLS "Build offline tools" ON)
option(LOX_BUILD_EXAMPLES "Build runnable examples" ON)
option(LOX_BUILD_JSON_WRAPPER "Build JSON wrapper module (separate from core)" ON)
option(LOX_BUILD_IMPORT_EXPORT "Build import/export module (separate from core)" ON)
option(LOX_BUILD_OPTIONAL_BACKENDS "Build optional backend adapter modules (not linked into core)" ON)
option(LOX_BUILD_BACKEND_ALIGNED_STUB "Build aligned backend stub module" ON)
option(LOX_BUILD_BACKEND_NAND_STUB "Build NAND managed backend stub module" ON)
option(LOX_BUILD_BACKEND_EMMC_STUB "Build eMMC managed backend stub module" ON)
option(LOX_BUILD_BACKEND_SD_STUB "Build SD managed backend stub module" ON)
option(LOX_BUILD_BACKEND_FS_STUB "Build filesystem managed backend stub module" ON)
option(LOX_BUILD_BACKEND_BLOCK_STUB "Build block-device managed backend stub module" ON)
set(LOX_MANAGED_STRESS_SMOKE_MAX_MS "5000" CACHE STRING "Runtime budget (ms) for managed stress smoke lane")
set(LOX_MANAGED_STRESS_LONG_MAX_MS "20000" CACHE STRING "Runtime budget (ms) for managed stress long lane")
set(LOX_FS_MATRIX_SMOKE_MAX_MS "6000" CACHE STRING "Runtime budget (ms) for fs/block matrix smoke lane")
set(LOX_FS_MATRIX_LONG_MAX_MS "25000" CACHE STRING "Runtime budget (ms) for fs/block matrix long lane")
find_program(LOX_LLVM_READOBJ NAMES llvm-readobj llvm-readobj.exe PATHS "C:/Program Files/LLVM/bin")

add_library(loxdb
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(
    loxdb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/port/ram>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/port/posix>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/loxdb/port>
    PRIVATE
        src
)
set_target_properties(loxdb PROPERTIES PUBLIC_HEADER include/lox.h)

add_library(lox_reject STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_reject PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_reject PRIVATE LOX_KV_OVERFLOW_POLICY=LOX_KV_POLICY_REJECT)

add_library(lox_ts_reject STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_ts_reject PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_ts_reject PRIVATE LOX_TS_OVERFLOW_POLICY=LOX_TS_POLICY_REJECT)

add_library(lox_ts_downsample STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_ts_downsample PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_ts_downsample PRIVATE LOX_TS_OVERFLOW_POLICY=LOX_TS_POLICY_DOWNSAMPLE)

add_library(lox_no_wal STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_no_wal PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_no_wal PRIVATE LOX_ENABLE_WAL=0)

add_library(lox_kv128 STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_kv128 PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_kv128 PRIVATE LOX_KV_MAX_KEYS=128)

add_library(lox_kv_off STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_kv_off PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_kv_off PRIVATE LOX_ENABLE_KV=0)

add_library(lox_ts_off STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_ts_off PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_ts_off PRIVATE LOX_ENABLE_TS=0)

add_library(lox_rel_off STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_rel_off PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_rel_off PRIVATE LOX_ENABLE_REL=0)

add_library(lox_small_limits STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)

target_include_directories(lox_small_limits PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(
    lox_small_limits
    PRIVATE
        LOX_KV_MAX_KEYS=8
        LOX_TXN_STAGE_KEYS=2
        LOX_TS_MAX_STREAMS=2
        LOX_REL_MAX_TABLES=1
        LOX_REL_MAX_COLS=4
)

function(create_lox_ram_variant target_name ram_kb)
    add_library(${target_name} STATIC
        src/loxdb.c
        src/lox_kv.c
        src/lox_ts.c
        src/lox_rel.c
        src/lox_wal.c
        src/lox_crc.c
        port/ram/lox_port_ram.c
        port/posix/lox_port_posix.c
    )
    target_include_directories(${target_name} PUBLIC include port/ram port/posix PRIVATE src)
    target_compile_definitions(${target_name} PRIVATE LOX_RAM_KB=${ram_kb})
    if(MSVC)
        target_compile_options(${target_name} PRIVATE /W4)
    else()
        target_compile_options(${target_name} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endfunction()

create_lox_ram_variant(lox_128kb 128)
create_lox_ram_variant(lox_256kb 256)
create_lox_ram_variant(lox_512kb 512)
create_lox_ram_variant(lox_1024kb 1024)
create_lox_ram_variant(lox_64kb 64)

add_library(lox_kv_only STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_kv_only PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_kv_only PRIVATE LOX_ENABLE_TS=0 LOX_ENABLE_REL=0)

add_library(lox_ts_only STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_ts_only PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_ts_only PRIVATE LOX_ENABLE_KV=0 LOX_ENABLE_REL=0)

add_library(lox_rel_only STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_rel_only PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_rel_only PRIVATE LOX_ENABLE_KV=0 LOX_ENABLE_TS=0)

add_library(lox_thread_safe STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_thread_safe PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_thread_safe PRIVATE LOX_THREAD_SAFE=1)

add_library(lox_tiny STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_tiny PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(
    lox_tiny
    PRIVATE
        LOX_ENABLE_TS=0
        LOX_ENABLE_REL=0
        LOX_ENABLE_WAL=0
        LOX_RAM_KB=8
        LOX_KV_MAX_KEYS=8
        LOX_TXN_STAGE_KEYS=2
        LOX_KV_KEY_MAX_LEN=16
        LOX_KV_VAL_MAX_LEN=64
)

add_library(lox_footprint_min STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_footprint_min PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_footprint_min PRIVATE LOX_PROFILE_FOOTPRINT_MIN=1)

function(create_lox_profile_variant target_name profile_macro)
    add_library(${target_name} STATIC
        src/loxdb.c
        src/lox_kv.c
        src/lox_ts.c
        src/lox_rel.c
        src/lox_wal.c
        src/lox_crc.c
        port/ram/lox_port_ram.c
        port/posix/lox_port_posix.c
    )
    target_include_directories(${target_name} PUBLIC include port/ram port/posix PRIVATE src)
    target_compile_definitions(${target_name} PRIVATE ${profile_macro}=1 LOX_THREAD_SAFE=1)
endfunction()

create_lox_profile_variant(lox_profile_core_min LOX_PROFILE_CORE_MIN)
create_lox_profile_variant(lox_profile_core_wal LOX_PROFILE_CORE_WAL)
create_lox_profile_variant(lox_profile_core_perf LOX_PROFILE_CORE_PERF)
create_lox_profile_variant(lox_profile_core_himem LOX_PROFILE_CORE_HIMEM)

add_library(lox_ts_log_retain STATIC
    src/loxdb.c
    src/lox_kv.c
    src/lox_ts.c
    src/lox_rel.c
    src/lox_wal.c
    src/lox_crc.c
    port/ram/lox_port_ram.c
    port/posix/lox_port_posix.c
)
target_include_directories(lox_ts_log_retain PUBLIC include port/ram port/posix PRIVATE src)
target_compile_definitions(lox_ts_log_retain PRIVATE LOX_TS_OVERFLOW_POLICY=LOX_TS_POLICY_LOG_RETAIN)

if(LOX_BUILD_JSON_WRAPPER)
    add_library(lox_json_wrapper STATIC src/modules/lox_json_wrapper.c)
    target_include_directories(lox_json_wrapper PUBLIC include PRIVATE src)
    target_link_libraries(lox_json_wrapper PUBLIC loxdb)
endif()

if(LOX_BUILD_IMPORT_EXPORT)
    add_library(lox_import_export STATIC src/modules/lox_import_export.c)
    target_include_directories(lox_import_export PUBLIC include PRIVATE src)
    if(TARGET lox_json_wrapper)
        target_link_libraries(lox_import_export PUBLIC lox_json_wrapper)
    else()
        target_link_libraries(lox_import_export PUBLIC loxdb)
    endif()
endif()

if(LOX_BUILD_OPTIONAL_BACKENDS)
    set(LOX_OPTIONAL_BACKEND_TARGETS "")

    add_library(lox_backend_registry STATIC src/backends/lox_backend_registry.c)
    target_include_directories(lox_backend_registry PUBLIC include PRIVATE src)
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_registry)

    add_library(lox_backend_compat STATIC src/backends/lox_backend_compat.c)
    target_include_directories(lox_backend_compat PUBLIC include PRIVATE src)
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_compat)

    add_library(lox_backend_decision STATIC src/backends/lox_backend_decision.c)
    target_include_directories(lox_backend_decision PUBLIC include PRIVATE src)
    target_link_libraries(
        lox_backend_decision
        PUBLIC
            lox_backend_registry
            lox_backend_compat
    )
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_decision)

    add_library(lox_backend_aligned_adapter STATIC src/backends/lox_backend_aligned_adapter.c)
    target_include_directories(lox_backend_aligned_adapter PUBLIC include PRIVATE src)
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_aligned_adapter)

    add_library(lox_backend_managed_adapter STATIC src/backends/lox_backend_managed_adapter.c)
    target_include_directories(lox_backend_managed_adapter PUBLIC include PRIVATE src)
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_managed_adapter)

    add_library(lox_backend_fs_adapter STATIC src/backends/lox_backend_fs_adapter.c)
    target_include_directories(lox_backend_fs_adapter PUBLIC include PRIVATE src)
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_fs_adapter)

    add_library(lox_backend_open STATIC src/backends/lox_backend_open.c)
    target_include_directories(lox_backend_open PUBLIC include PRIVATE src)
    target_link_libraries(
        lox_backend_open
        PUBLIC
            lox_backend_decision
            lox_backend_registry
            lox_backend_aligned_adapter
            lox_backend_fs_adapter
            lox_backend_managed_adapter
    )
    list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_open)

    if(LOX_BUILD_BACKEND_ALIGNED_STUB)
        add_library(lox_backend_aligned_stub STATIC src/backends/lox_backend_aligned_stub.c)
        target_include_directories(lox_backend_aligned_stub PUBLIC include PRIVATE src)
        target_link_libraries(lox_backend_aligned_stub PUBLIC lox_backend_registry)
        list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_aligned_stub)
    endif()

    if(LOX_BUILD_BACKEND_NAND_STUB)
        add_library(lox_backend_nand_stub STATIC src/backends/lox_backend_nand_stub.c)
        target_include_directories(lox_backend_nand_stub PUBLIC include PRIVATE src)
        target_link_libraries(lox_backend_nand_stub PUBLIC lox_backend_registry)
        list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_nand_stub)
    endif()

    if(LOX_BUILD_BACKEND_EMMC_STUB)
        add_library(lox_backend_emmc_stub STATIC src/backends/lox_backend_emmc_stub.c)
        target_include_directories(lox_backend_emmc_stub PUBLIC include PRIVATE src)
        target_link_libraries(lox_backend_emmc_stub PUBLIC lox_backend_registry)
        list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_emmc_stub)
    endif()

    if(LOX_BUILD_BACKEND_SD_STUB)
        add_library(lox_backend_sd_stub STATIC src/backends/lox_backend_sd_stub.c)
        target_include_directories(lox_backend_sd_stub PUBLIC include PRIVATE src)
        target_link_libraries(lox_backend_sd_stub PUBLIC lox_backend_registry)
        list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_sd_stub)
    endif()

    if(LOX_BUILD_BACKEND_FS_STUB)
        add_library(lox_backend_fs_stub STATIC src/backends/lox_backend_fs_stub.c)
        target_include_directories(lox_backend_fs_stub PUBLIC include PRIVATE src)
        target_link_libraries(lox_backend_fs_stub PUBLIC lox_backend_registry)
        list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_fs_stub)
    endif()

    if(LOX_BUILD_BACKEND_BLOCK_STUB)
        add_library(lox_backend_block_stub STATIC src/backends/lox_backend_block_stub.c)
        target_include_directories(lox_backend_block_stub PUBLIC include PRIVATE src)
        target_link_libraries(lox_backend_block_stub PUBLIC lox_backend_registry)
        list(APPEND LOX_OPTIONAL_BACKEND_TARGETS lox_backend_block_stub)
    endif()

    add_custom_target(
        lox_optional_backends_all
        DEPENDS ${LOX_OPTIONAL_BACKEND_TARGETS}
    )
endif()

# Hard gate: core must never directly link optional backend modules.
foreach(_optional_backend_target
        lox_backend_aligned_adapter
        lox_backend_managed_adapter
        lox_backend_fs_adapter
        lox_backend_open
        lox_backend_aligned_stub
        lox_backend_nand_stub
        lox_backend_emmc_stub
        lox_backend_sd_stub
        lox_backend_fs_stub
        lox_backend_block_stub
        lox_json_wrapper
        lox_import_export)
    if(TARGET ${_optional_backend_target})
        get_target_property(_lox_links loxdb LINK_LIBRARIES)
        if(_lox_links)
            list(FIND _lox_links ${_optional_backend_target} _linked_idx)
            if(NOT _linked_idx EQUAL -1)
                message(FATAL_ERROR "Core target 'loxdb' must not link optional backend target '${_optional_backend_target}'")
            endif()
        endif()
    endif()
endforeach()

if(MSVC)
    target_compile_options(loxdb PRIVATE /W4)
    target_compile_options(lox_reject PRIVATE /W4)
    target_compile_options(lox_ts_reject PRIVATE /W4)
    target_compile_options(lox_ts_downsample PRIVATE /W4)
    target_compile_options(lox_no_wal PRIVATE /W4)
    target_compile_options(lox_kv128 PRIVATE /W4)
    target_compile_options(lox_kv_off PRIVATE /W4)
    target_compile_options(lox_ts_off PRIVATE /W4)
    target_compile_options(lox_rel_off PRIVATE /W4)
    target_compile_options(lox_small_limits PRIVATE /W4)
    target_compile_options(lox_kv_only PRIVATE /W4)
    target_compile_options(lox_ts_only PRIVATE /W4)
    target_compile_options(lox_rel_only PRIVATE /W4)
    target_compile_options(lox_thread_safe PRIVATE /W4)
    target_compile_options(lox_tiny PRIVATE /W4)
    target_compile_options(lox_footprint_min PRIVATE /W4)
    target_compile_options(lox_profile_core_min PRIVATE /W4)
    target_compile_options(lox_profile_core_wal PRIVATE /W4)
    target_compile_options(lox_profile_core_perf PRIVATE /W4)
    target_compile_options(lox_profile_core_himem PRIVATE /W4)
    if(TARGET lox_json_wrapper)
        target_compile_options(lox_json_wrapper PRIVATE /W4)
    endif()
    if(TARGET lox_import_export)
        target_compile_options(lox_import_export PRIVATE /W4)
    endif()
    foreach(_opt_target
            lox_backend_registry
            lox_backend_compat
            lox_backend_decision
            lox_backend_aligned_adapter
            lox_backend_managed_adapter
            lox_backend_fs_adapter
            lox_backend_open
            lox_backend_aligned_stub
            lox_backend_nand_stub
            lox_backend_emmc_stub
            lox_backend_sd_stub
            lox_backend_fs_stub
            lox_backend_block_stub)
        if(TARGET ${_opt_target})
            target_compile_options(${_opt_target} PRIVATE /W4)
        endif()
    endforeach()
else()
    target_compile_options(loxdb PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_reject PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_ts_reject PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_ts_downsample PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_no_wal PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_kv128 PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_kv_off PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_ts_off PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_rel_off PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_small_limits PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_kv_only PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_ts_only PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_rel_only PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_thread_safe PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_tiny PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_footprint_min PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_profile_core_min PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_profile_core_wal PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_profile_core_perf PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(lox_profile_core_himem PRIVATE -Wall -Wextra -Wpedantic)
    if(TARGET lox_json_wrapper)
        target_compile_options(lox_json_wrapper PRIVATE -Wall -Wextra -Wpedantic)
    endif()
    if(TARGET lox_import_export)
        target_compile_options(lox_import_export PRIVATE -Wall -Wextra -Wpedantic)
    endif()
    foreach(_opt_target
            lox_backend_registry
            lox_backend_compat
            lox_backend_decision
            lox_backend_aligned_adapter
            lox_backend_managed_adapter
            lox_backend_fs_adapter
            lox_backend_open
            lox_backend_aligned_stub
            lox_backend_nand_stub
            lox_backend_emmc_stub
            lox_backend_sd_stub
            lox_backend_fs_stub
            lox_backend_block_stub)
        if(TARGET ${_opt_target})
            target_compile_options(${_opt_target} PRIVATE -Wall -Wextra -Wpedantic)
        endif()
    endforeach()
endif()

install(
    TARGETS loxdb
    EXPORT loxdbTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

if(TARGET lox_json_wrapper)
    install(
        TARGETS lox_json_wrapper
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()

if(TARGET lox_import_export)
    install(
        TARGETS lox_import_export
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()

install(
    FILES
        port/ram/lox_port_ram.h
        port/posix/lox_port_posix.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/loxdb/port
)

install(
    FILES
        include/lox_capacity_profile.h
        include/lox_json_wrapper.h
        include/lox_import_export.h
        include/lox_backend_adapter.h
        include/lox_backend_compat.h
        include/lox_backend_decision.h
        include/lox_backend_aligned_adapter.h
        include/lox_backend_managed_adapter.h
        include/lox_backend_fs_adapter.h
        include/lox_backend_open.h
        include/lox_cpp_storage_profile.hpp
        include/lox_cpp.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(
    FILES
        README.md
        LICENSE
    DESTINATION ${CMAKE_INSTALL_DATADIR}/loxdb
)

install(
    EXPORT loxdbTargets
    FILE loxdbTargets.cmake
    NAMESPACE loxdb::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/loxdb
)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/loxdbConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/loxdbConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/loxdb
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/loxdbConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/loxdbConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/loxdbConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/loxdb
)

if(LOX_BUILD_TOOLS)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tools/lox_verify.c")
        add_executable(lox_verify
            tools/lox_verify.c
            src/lox_crc.c
        )
        target_include_directories(lox_verify PRIVATE include src)
    else()
        message(STATUS "Skipping lox_verify: missing tools/lox_verify.c")
    endif()
endif()

if(LOX_BUILD_EXAMPLES)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/posix_simulator/main.c")
        add_executable(example_posix_simulator examples/posix_simulator/main.c)
        target_include_directories(example_posix_simulator PRIVATE include port/posix)
        target_link_libraries(example_posix_simulator PRIVATE loxdb)
    else()
        message(STATUS "Skipping example_posix_simulator: missing examples/posix_simulator/main.c")
    endif()
endif()

if(LOX_BUILD_TESTS)
    enable_testing()
    add_executable(test_kv tests/test_kv.c)
    target_include_directories(test_kv PRIVATE include tests port/ram src)
    target_link_libraries(test_kv PRIVATE loxdb)
    add_test(NAME test_kv COMMAND test_kv)

    add_executable(test_kv_reject tests/test_kv_reject.c)
    target_include_directories(test_kv_reject PRIVATE include tests port/ram src)
    target_link_libraries(test_kv_reject PRIVATE lox_reject)
    add_test(NAME test_kv_reject COMMAND test_kv_reject)

    add_executable(test_ts tests/test_ts.c)
    target_include_directories(test_ts PRIVATE include tests port/ram src)
    target_link_libraries(test_ts PRIVATE loxdb)
    add_test(NAME test_ts COMMAND test_ts)

    add_executable(test_ts_reject tests/test_ts_reject.c)
    target_include_directories(test_ts_reject PRIVATE include tests port/ram src)
    target_link_libraries(test_ts_reject PRIVATE lox_ts_reject)
    add_test(NAME test_ts_reject COMMAND test_ts_reject)

    add_executable(test_ts_downsample tests/test_ts_downsample.c)
    target_include_directories(test_ts_downsample PRIVATE include tests port/ram src)
    target_link_libraries(test_ts_downsample PRIVATE lox_ts_downsample)
    add_test(NAME test_ts_downsample COMMAND test_ts_downsample)

    add_executable(test_rel tests/test_rel.c)
    target_include_directories(test_rel PRIVATE include tests port/ram src)
    target_link_libraries(test_rel PRIVATE loxdb)
    add_test(NAME test_rel COMMAND test_rel)

    add_executable(test_stats tests/test_stats.c)
    target_include_directories(test_stats PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_stats PRIVATE loxdb)
    add_test(NAME test_stats COMMAND test_stats)

    add_executable(test_error_strings tests/test_error_strings.c)
    target_include_directories(test_error_strings PRIVATE include tests port/ram src)
    target_link_libraries(test_error_strings PRIVATE loxdb)
    add_test(NAME test_error_strings COMMAND test_error_strings)

    add_executable(test_preflight tests/test_preflight.c)
    target_include_directories(test_preflight PRIVATE include tests port/ram src)
    target_link_libraries(test_preflight PRIVATE loxdb)
    add_test(NAME test_preflight COMMAND test_preflight)

    if(TARGET lox_json_wrapper AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_json_wrapper.c")
        add_executable(test_json_wrapper tests/test_json_wrapper.c)
        target_include_directories(test_json_wrapper PRIVATE include tests port/ram src)
        target_link_libraries(test_json_wrapper PRIVATE lox_json_wrapper)
        add_test(NAME test_json_wrapper COMMAND test_json_wrapper)
    else()
        message(STATUS "Skipping test_json_wrapper: missing module target or tests/test_json_wrapper.c")
    endif()

    if(TARGET lox_import_export AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_import_export.c")
        add_executable(test_import_export tests/test_import_export.c)
        target_include_directories(test_import_export PRIVATE include tests port/ram src)
        target_link_libraries(test_import_export PRIVATE lox_import_export)
        add_test(NAME test_import_export COMMAND test_import_export)
    else()
        message(STATUS "Skipping test_import_export: missing module target or tests/test_import_export.c")
    endif()

    if(TARGET lox_import_export AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_import_export_fuzz.c")
        add_executable(test_import_export_fuzz tests/test_import_export_fuzz.c)
        target_include_directories(test_import_export_fuzz PRIVATE include tests port/ram src)
        target_link_libraries(test_import_export_fuzz PRIVATE lox_import_export)
        add_test(NAME test_import_export_fuzz COMMAND test_import_export_fuzz)
    else()
        message(STATUS "Skipping test_import_export_fuzz: missing module target or tests/test_import_export_fuzz.c")
    endif()

    if(TARGET lox_import_export AND TARGET lox_json_wrapper AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_realdata_integration.c")
        add_executable(test_realdata_integration tests/test_realdata_integration.c)
        target_include_directories(test_realdata_integration PRIVATE include tests port/ram src)
        target_link_libraries(test_realdata_integration PRIVATE lox_import_export)
        add_test(NAME test_realdata_integration COMMAND test_realdata_integration)
    else()
        message(STATUS "Skipping test_realdata_integration: missing module target or tests/test_realdata_integration.c")
    endif()

    add_executable(test_thread_safety tests/test_thread_safety.c)
    target_include_directories(test_thread_safety PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_thread_safety PRIVATE LOX_THREAD_SAFE=1)
    target_link_libraries(test_thread_safety PRIVATE lox_thread_safe)
    add_test(NAME test_thread_safety COMMAND test_thread_safety)

    add_executable(test_compact tests/test_compact.c)
    target_include_directories(test_compact PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_compact PRIVATE loxdb)
    add_test(NAME test_compact COMMAND test_compact)

    add_executable(test_migration tests/test_migration.c)
    target_include_directories(test_migration PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_migration PRIVATE loxdb)
    add_test(NAME test_migration COMMAND test_migration)

    add_executable(test_txn tests/test_txn.c)
    target_include_directories(test_txn PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_txn PRIVATE loxdb)
    add_test(NAME test_txn COMMAND test_txn)

    add_executable(test_wal tests/test_wal.c)
    target_include_directories(test_wal PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_wal PRIVATE loxdb)
    add_test(NAME test_wal COMMAND test_wal)

    add_executable(test_wal_no_wal tests/test_wal_no_wal.c)
    target_include_directories(test_wal_no_wal PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_wal_no_wal PRIVATE lox_no_wal)
    add_test(NAME test_wal_no_wal COMMAND test_wal_no_wal)

    add_executable(test_wal_kv128 tests/test_wal_kv128.c)
    target_include_directories(test_wal_kv128 PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_wal_kv128 PRIVATE lox_kv128)
    add_test(NAME test_wal_kv128 COMMAND test_wal_kv128)

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_durability_closure.c")
        add_executable(test_durability_closure tests/test_durability_closure.c)
        target_include_directories(test_durability_closure PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_durability_closure PRIVATE loxdb)
        add_test(NAME test_durability_closure COMMAND test_durability_closure)
    else()
        message(STATUS "Skipping test_durability_closure: missing tests/test_durability_closure.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_resilience.c")
        add_executable(test_resilience tests/test_resilience.c)
        target_include_directories(test_resilience PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_resilience PRIVATE loxdb)
        add_test(NAME test_resilience COMMAND test_resilience)
    else()
        message(STATUS "Skipping test_resilience: missing tests/test_resilience.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_rel_corruption_replay.c")
        add_executable(test_rel_corruption_replay tests/test_rel_corruption_replay.c)
        target_include_directories(test_rel_corruption_replay PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_rel_corruption_replay
            PRIVATE
                LOX_REL_CORPUS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/corpus/rel"
        )
        target_link_libraries(test_rel_corruption_replay PRIVATE loxdb)
        add_test(NAME test_rel_corruption_replay COMMAND test_rel_corruption_replay)
    else()
        message(STATUS "Skipping test_rel_corruption_replay: missing tests/test_rel_corruption_replay.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_api_contract_matrix.c")
        add_executable(test_api_contract_matrix tests/test_api_contract_matrix.c)
        target_include_directories(test_api_contract_matrix PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_api_contract_matrix PRIVATE loxdb)
        add_test(NAME test_api_contract_matrix COMMAND test_api_contract_matrix)
    else()
        message(STATUS "Skipping test_api_contract_matrix: missing tests/test_api_contract_matrix.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_fail_code_contract.c")
        add_executable(test_fail_code_contract tests/test_fail_code_contract.c)
        target_include_directories(test_fail_code_contract PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_fail_code_contract PRIVATE loxdb)
        add_test(NAME test_fail_code_contract COMMAND test_fail_code_contract)
    else()
        message(STATUS "Skipping test_fail_code_contract: missing tests/test_fail_code_contract.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_tiny_footprint.c")
        add_executable(test_tiny_footprint tests/test_tiny_footprint.c)
        target_include_directories(test_tiny_footprint PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_tiny_footprint PRIVATE lox_tiny)
        add_test(NAME test_tiny_footprint COMMAND test_tiny_footprint)
    else()
        message(STATUS "Skipping test_tiny_footprint: missing tests/test_tiny_footprint.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_tiny_size.cmake")
        add_test(
            NAME test_tiny_size_guard
            COMMAND ${CMAKE_COMMAND}
                -DDEFAULT_LIB=$<TARGET_FILE:loxdb>
                -DTINY_LIB=$<TARGET_FILE:lox_tiny>
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_tiny_size.cmake
        )
    else()
        message(STATUS "Skipping test_tiny_size_guard: missing tests/check_tiny_size.cmake")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/footprint_min_baseline.c")
        add_executable(test_footprint_min_baseline tests/footprint_min_baseline.c)
        target_include_directories(test_footprint_min_baseline PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_footprint_min_baseline PRIVATE lox_footprint_min)
        set(_footprint_map "$<TARGET_FILE_DIR:test_footprint_min_baseline>/test_footprint_min_baseline.map")
        if(MSVC)
            target_link_options(
                test_footprint_min_baseline
                PRIVATE
                    "/MAP:${_footprint_map}"
            )
        elseif(APPLE)
            target_link_options(
                test_footprint_min_baseline
                PRIVATE
                    "-Wl,-map,${_footprint_map}"
            )
        else()
            target_link_options(
                test_footprint_min_baseline
                PRIVATE
                    "-Wl,-Map,${_footprint_map}"
            )
        endif()
        add_test(NAME test_footprint_min_baseline COMMAND test_footprint_min_baseline)

        if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_footprint_min_size.cmake" AND CMAKE_BUILD_TYPE STREQUAL "Release")
            add_test(
                NAME test_footprint_min_size_gate_release
                COMMAND ${CMAKE_COMMAND}
                    -DEXE=$<TARGET_FILE:test_footprint_min_baseline>
                    -DMAP=${_footprint_map}
                    -DLLVM_READOBJ=${LOX_LLVM_READOBJ}
                    -DMAX_TEXT=25000
                    -DMAX_RODATA=1600
                    -DMAX_DATA=256
                    -DMAX_BSS=2048
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_footprint_min_size.cmake
            )
            set_tests_properties(test_footprint_min_size_gate_release PROPERTIES CONFIGURATIONS Release)
        else()
            message(STATUS "Skipping test_footprint_min_size_gate_release: requires Release build and tests/check_footprint_min_size.cmake")
        endif()
    else()
        message(STATUS "Skipping test_footprint_min_baseline: missing tests/footprint_min_baseline.c")
    endif()

    if(LOX_BUILD_TOOLS)
        if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_offline_verifier.c")
            add_executable(test_offline_verifier tests/test_offline_verifier.c)
            target_include_directories(test_offline_verifier PRIVATE include tests port/ram port/posix src)
            target_link_libraries(test_offline_verifier PRIVATE loxdb)
            target_compile_definitions(
                test_offline_verifier
                PRIVATE
                    LOX_VERIFY_TOOL_PATH="$<TARGET_FILE:lox_verify>"
            )
            add_test(NAME test_offline_verifier COMMAND test_offline_verifier)
        else()
            message(STATUS "Skipping test_offline_verifier: missing tests/test_offline_verifier.c")
        endif()
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_capacity_estimator_model.c")
        add_executable(test_capacity_estimator_model tests/test_capacity_estimator_model.c)
        target_include_directories(test_capacity_estimator_model PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_capacity_estimator_model PRIVATE loxdb)
        add_test(NAME test_capacity_estimator_model COMMAND test_capacity_estimator_model)
    else()
        message(STATUS "Skipping test_capacity_estimator_model: missing tests/test_capacity_estimator_model.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_safety_invariants.c")
        add_executable(test_safety_invariants tests/test_safety_invariants.c)
        target_include_directories(test_safety_invariants PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_safety_invariants PRIVATE loxdb)
        add_test(NAME test_safety_invariants COMMAND test_safety_invariants)
    else()
        message(STATUS "Skipping test_safety_invariants: missing tests/test_safety_invariants.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_selfcheck.c")
        add_executable(test_selfcheck tests/test_selfcheck.c)
        target_include_directories(test_selfcheck PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_selfcheck PRIVATE loxdb)
        add_test(NAME test_selfcheck COMMAND test_selfcheck)
    else()
        message(STATUS "Skipping test_selfcheck: missing tests/test_selfcheck.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_wcet_bounds.c")
        add_executable(test_wcet_bounds tests/test_wcet_bounds.c)
        target_include_directories(test_wcet_bounds PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_wcet_bounds PRIVATE loxdb)
        add_test(NAME test_wcet_bounds COMMAND test_wcet_bounds)
    else()
        message(STATUS "Skipping test_wcet_bounds: missing tests/test_wcet_bounds.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_ts_log_retain.c")
        add_executable(test_ts_log_retain tests/test_ts_log_retain.c)
        target_include_directories(test_ts_log_retain PRIVATE include tests port/ram port/posix src)
        target_link_libraries(test_ts_log_retain PRIVATE lox_ts_log_retain)
        add_test(NAME test_ts_log_retain COMMAND test_ts_log_retain)
    else()
        message(STATUS "Skipping test_ts_log_retain: missing tests/test_ts_log_retain.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/worstcase_matrix_runner.c")
        add_executable(worstcase_matrix_runner tests/worstcase_matrix_runner.c)
        target_include_directories(worstcase_matrix_runner PRIVATE include tests port/ram port/posix src)
        target_link_libraries(worstcase_matrix_runner PRIVATE loxdb)
    else()
        message(STATUS "Skipping worstcase_matrix_runner: missing tests/worstcase_matrix_runner.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/soak_runner.c")
        add_executable(soak_runner tests/soak_runner.c)
        target_include_directories(soak_runner PRIVATE include tests port/ram port/posix src)
        target_link_libraries(soak_runner PRIVATE loxdb)
    else()
        message(STATUS "Skipping soak_runner: missing tests/soak_runner.c")
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_profile_matrix.c")
        add_executable(test_profile_core_min tests/test_profile_matrix.c)
        target_include_directories(test_profile_core_min PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_profile_core_min
            PRIVATE
                LOX_PROFILE_CORE_MIN=1
                LOX_THREAD_SAFE=1
                LOX_PROFILE_TEST_DB_PATH="profile_core_min.bin"
        )
        target_link_libraries(test_profile_core_min PRIVATE lox_profile_core_min)
        add_test(NAME test_profile_core_min COMMAND test_profile_core_min)

        add_executable(test_profile_core_wal tests/test_profile_matrix.c)
        target_include_directories(test_profile_core_wal PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_profile_core_wal
            PRIVATE
                LOX_PROFILE_CORE_WAL=1
                LOX_THREAD_SAFE=1
                LOX_PROFILE_TEST_DB_PATH="profile_core_wal.bin"
        )
        target_link_libraries(test_profile_core_wal PRIVATE lox_profile_core_wal)
        add_test(NAME test_profile_core_wal COMMAND test_profile_core_wal)

        add_executable(test_profile_core_perf tests/test_profile_matrix.c)
        target_include_directories(test_profile_core_perf PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_profile_core_perf
            PRIVATE
                LOX_PROFILE_CORE_PERF=1
                LOX_THREAD_SAFE=1
                LOX_PROFILE_TEST_DB_PATH="profile_core_perf.bin"
        )
        target_link_libraries(test_profile_core_perf PRIVATE lox_profile_core_perf)
        add_test(NAME test_profile_core_perf COMMAND test_profile_core_perf)

        add_executable(test_profile_core_himem tests/test_profile_matrix.c)
        target_include_directories(test_profile_core_himem PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_profile_core_himem
            PRIVATE
                LOX_PROFILE_CORE_HIMEM=1
                LOX_THREAD_SAFE=1
                LOX_PROFILE_TEST_DB_PATH="profile_core_himem.bin"
        )
        target_link_libraries(test_profile_core_himem PRIVATE lox_profile_core_himem)
        add_test(NAME test_profile_core_himem COMMAND test_profile_core_himem)
    else()
        message(STATUS "Skipping profile matrix tests: missing tests/test_profile_matrix.c")
    endif()

    add_executable(test_integration tests/test_integration.c)
    target_include_directories(test_integration PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_integration PRIVATE LOX_TEST_DB_PATH="integration_default.bin")
    target_link_libraries(test_integration PRIVATE loxdb)
    add_test(NAME test_integration COMMAND test_integration)

    add_executable(test_storage_capacity_profiles tests/test_storage_capacity_profiles.c)
    target_include_directories(test_storage_capacity_profiles PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_storage_capacity_profiles PRIVATE loxdb)
    add_test(NAME test_storage_capacity_profiles COMMAND test_storage_capacity_profiles)

    add_executable(test_cpp_wrapper tests/test_cpp_wrapper.cpp)
    target_include_directories(test_cpp_wrapper PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_cpp_wrapper PRIVATE loxdb)
    add_test(NAME test_cpp_wrapper COMMAND test_cpp_wrapper)

    add_executable(test_integration_8kb tests/test_integration.c)
    target_include_directories(test_integration_8kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_8kb
        PRIVATE
            LOX_TEST_WITH_8KB=1
            LOX_TEST_DB_PATH="integration_8kb.bin"
            LOX_KV_MAX_KEYS=8
            LOX_TXN_STAGE_KEYS=2
            LOX_TS_MAX_STREAMS=2
            LOX_REL_MAX_TABLES=1
            LOX_REL_MAX_COLS=4
    )
    target_link_libraries(test_integration_8kb PRIVATE lox_small_limits)
    add_test(NAME test_integration_8kb COMMAND test_integration_8kb)

    foreach(RAM_KB 128 256 512 1024)
        add_executable(test_integration_${RAM_KB}kb tests/test_integration.c)
        target_include_directories(test_integration_${RAM_KB}kb PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(
            test_integration_${RAM_KB}kb
            PRIVATE
                LOX_TEST_LARGE_RAM_VARIANT=1
                LOX_RAM_KB=${RAM_KB}
                LOX_TEST_DB_PATH="integration_${RAM_KB}kb.bin"
        )
        target_link_libraries(test_integration_${RAM_KB}kb PRIVATE lox_${RAM_KB}kb)
        add_test(NAME integration_${RAM_KB}kb COMMAND test_integration_${RAM_KB}kb)
    endforeach()

    add_executable(test_integration_kv_off tests/test_integration.c)
    target_include_directories(test_integration_kv_off PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_kv_off
        PRIVATE
            LOX_ENABLE_KV=0
            LOX_TEST_DB_PATH="integration_kv_off.bin"
    )
    target_link_libraries(test_integration_kv_off PRIVATE lox_kv_off)
    add_test(NAME test_integration_kv_off COMMAND test_integration_kv_off)

    add_executable(test_integration_ts_off tests/test_integration.c)
    target_include_directories(test_integration_ts_off PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_ts_off
        PRIVATE
            LOX_ENABLE_TS=0
            LOX_TEST_DB_PATH="integration_ts_off.bin"
    )
    target_link_libraries(test_integration_ts_off PRIVATE lox_ts_off)
    add_test(NAME test_integration_ts_off COMMAND test_integration_ts_off)

    add_executable(test_integration_rel_off tests/test_integration.c)
    target_include_directories(test_integration_rel_off PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_integration_rel_off
        PRIVATE
            LOX_ENABLE_REL=0
            LOX_TEST_DB_PATH="integration_rel_off.bin"
    )
    target_link_libraries(test_integration_rel_off PRIVATE lox_rel_off)
    add_test(NAME test_integration_rel_off COMMAND test_integration_rel_off)

    add_executable(test_limits tests/test_limits.c)
    target_include_directories(test_limits PRIVATE include tests port/ram port/posix src)
    target_link_libraries(test_limits PRIVATE loxdb)
    add_test(NAME test_limits COMMAND test_limits)

    add_executable(test_limits_32kb tests/test_limits.c)
    target_include_directories(test_limits_32kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_limits_32kb PRIVATE LOX_RAM_KB=32)
    target_link_libraries(test_limits_32kb PRIVATE loxdb)
    add_test(NAME test_limits_32kb COMMAND test_limits_32kb)

    add_executable(test_limits_64kb tests/test_limits.c)
    target_include_directories(test_limits_64kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(test_limits_64kb PRIVATE LOX_RAM_KB=64)
    target_link_libraries(test_limits_64kb PRIVATE lox_64kb)
    add_test(NAME test_limits_64kb COMMAND test_limits_64kb)

    add_executable(test_limits_8kb tests/test_limits.c)
    target_include_directories(test_limits_8kb PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_8kb
        PRIVATE
            LOX_TEST_WITH_8KB=1
            LOX_KV_MAX_KEYS=8
            LOX_TXN_STAGE_KEYS=2
            LOX_TS_MAX_STREAMS=2
            LOX_REL_MAX_TABLES=1
            LOX_REL_MAX_COLS=4
    )
    target_link_libraries(test_limits_8kb PRIVATE lox_small_limits)
    add_test(NAME test_limits_8kb COMMAND test_limits_8kb)

    foreach(RAM_KB 128 256 512 1024)
        add_executable(test_limits_${RAM_KB}kb tests/test_limits.c)
        target_include_directories(test_limits_${RAM_KB}kb PRIVATE include tests port/ram port/posix src)
        target_compile_definitions(test_limits_${RAM_KB}kb PRIVATE LOX_RAM_KB=${RAM_KB})
        target_link_libraries(test_limits_${RAM_KB}kb PRIVATE lox_${RAM_KB}kb)
        add_test(NAME limits_${RAM_KB}kb COMMAND test_limits_${RAM_KB}kb)
    endforeach()

    add_executable(test_limits_kv_only tests/test_limits.c)
    target_include_directories(test_limits_kv_only PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_kv_only
        PRIVATE
            LOX_ENABLE_TS=0
            LOX_ENABLE_REL=0
            LOX_TEST_LIMITS_KV_ONLY=1
    )
    target_link_libraries(test_limits_kv_only PRIVATE lox_kv_only)
    add_test(NAME test_limits_kv_only COMMAND test_limits_kv_only)

    add_executable(test_limits_ts_only tests/test_limits.c)
    target_include_directories(test_limits_ts_only PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_ts_only
        PRIVATE
            LOX_ENABLE_KV=0
            LOX_ENABLE_REL=0
            LOX_TEST_LIMITS_TS_ONLY=1
    )
    target_link_libraries(test_limits_ts_only PRIVATE lox_ts_only)
    add_test(NAME test_limits_ts_only COMMAND test_limits_ts_only)

    add_executable(test_limits_rel_only tests/test_limits.c)
    target_include_directories(test_limits_rel_only PRIVATE include tests port/ram port/posix src)
    target_compile_definitions(
        test_limits_rel_only
        PRIVATE
            LOX_ENABLE_KV=0
            LOX_ENABLE_TS=0
            LOX_TEST_LIMITS_REL_ONLY=1
    )
    target_link_libraries(test_limits_rel_only PRIVATE lox_rel_only)
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

    if(LOX_BUILD_OPTIONAL_BACKENDS)
        add_executable(test_backend_compat tests/test_backend_compat.c)
        target_include_directories(test_backend_compat PRIVATE include tests src)
        target_link_libraries(test_backend_compat PRIVATE lox_backend_compat)
        add_test(NAME test_backend_compat COMMAND test_backend_compat)

        add_executable(test_backend_decision tests/test_backend_decision.c)
        target_include_directories(test_backend_decision PRIVATE include tests src)
        if(TARGET lox_backend_nand_stub AND TARGET lox_backend_emmc_stub AND TARGET lox_backend_sd_stub AND TARGET lox_backend_fs_stub AND TARGET lox_backend_block_stub)
            target_link_libraries(
                test_backend_decision
                PRIVATE
                    lox_backend_decision
                    lox_backend_nand_stub
                    lox_backend_emmc_stub
                    lox_backend_sd_stub
                    lox_backend_fs_stub
                    lox_backend_block_stub
            )
            add_test(NAME test_backend_decision COMMAND test_backend_decision)
        else()
            message(STATUS "Skipping test_backend_decision: requires nand/emmc/sd/fs/block stub targets")
        endif()

        add_executable(test_backend_aligned_adapter tests/test_backend_aligned_adapter.c)
        target_include_directories(test_backend_aligned_adapter PRIVATE include tests src)
        target_link_libraries(test_backend_aligned_adapter PRIVATE lox_backend_aligned_adapter)
        add_test(NAME test_backend_aligned_adapter COMMAND test_backend_aligned_adapter)

        add_executable(test_backend_managed_adapter tests/test_backend_managed_adapter.c)
        target_include_directories(test_backend_managed_adapter PRIVATE include tests src)
        target_link_libraries(test_backend_managed_adapter PRIVATE lox_backend_managed_adapter)
        add_test(NAME test_backend_managed_adapter COMMAND test_backend_managed_adapter)

        add_executable(test_backend_fs_adapter tests/test_backend_fs_adapter.c)
        target_include_directories(test_backend_fs_adapter PRIVATE include tests src)
        target_link_libraries(test_backend_fs_adapter PRIVATE lox_backend_fs_adapter)
        add_test(NAME test_backend_fs_adapter COMMAND test_backend_fs_adapter)

        add_executable(test_backend_registry tests/test_backend_registry.c)
        target_include_directories(test_backend_registry PRIVATE include tests src)
        if(TARGET lox_backend_aligned_stub AND TARGET lox_backend_nand_stub AND TARGET lox_backend_emmc_stub AND TARGET lox_backend_sd_stub AND TARGET lox_backend_fs_stub AND TARGET lox_backend_block_stub)
            target_link_libraries(
                test_backend_registry
                PRIVATE
                    lox_backend_aligned_stub
                    lox_backend_registry
                    lox_backend_nand_stub
                    lox_backend_emmc_stub
                    lox_backend_sd_stub
                    lox_backend_fs_stub
                    lox_backend_block_stub
            )
            add_test(NAME test_backend_registry COMMAND test_backend_registry)
        else()
            message(STATUS "Skipping test_backend_registry: requires aligned/nand/emmc/sd/fs/block stub targets")
        endif()

        add_executable(test_backend_open tests/test_backend_open.c)
        target_include_directories(test_backend_open PRIVATE include tests src)
        if(TARGET lox_backend_aligned_stub AND TARGET lox_backend_nand_stub AND TARGET lox_backend_emmc_stub AND TARGET lox_backend_sd_stub AND TARGET lox_backend_fs_stub AND TARGET lox_backend_block_stub)
            target_link_libraries(
                test_backend_open
                PRIVATE
                    lox_backend_open
                    lox_backend_aligned_stub
                    lox_backend_nand_stub
                    lox_backend_emmc_stub
                    lox_backend_sd_stub
                    lox_backend_fs_stub
                    lox_backend_block_stub
            )
            add_test(NAME test_backend_open COMMAND test_backend_open)
        else()
            message(STATUS "Skipping test_backend_open: requires aligned/nand/emmc/sd/fs/block stub targets")
        endif()

        add_executable(test_backend_aligned_recovery tests/test_backend_aligned_recovery.c)
        target_include_directories(test_backend_aligned_recovery PRIVATE include tests src)
        if(TARGET lox_backend_aligned_stub)
            target_link_libraries(
                test_backend_aligned_recovery
                PRIVATE
                    loxdb
                    lox_backend_open
                    lox_backend_aligned_stub
            )
            add_test(NAME test_backend_aligned_recovery COMMAND test_backend_aligned_recovery)
        else()
            message(STATUS "Skipping test_backend_aligned_recovery: requires aligned stub target")
        endif()

        add_executable(test_backend_managed_recovery tests/test_backend_managed_recovery.c)
        target_include_directories(test_backend_managed_recovery PRIVATE include tests src)
        if(TARGET lox_backend_nand_stub)
            target_link_libraries(
                test_backend_managed_recovery
                PRIVATE
                    loxdb
                    lox_backend_open
                    lox_backend_nand_stub
            )
            add_test(NAME test_backend_managed_recovery COMMAND test_backend_managed_recovery)
        else()
            message(STATUS "Skipping test_backend_managed_recovery: requires nand stub target")
        endif()

        add_executable(test_backend_fs_recovery tests/test_backend_fs_recovery.c)
        target_include_directories(test_backend_fs_recovery PRIVATE include tests src)
        if(TARGET lox_backend_fs_stub AND TARGET lox_backend_block_stub)
            target_link_libraries(
                test_backend_fs_recovery
                PRIVATE
                    loxdb
                    lox_backend_open
                    lox_backend_fs_stub
                    lox_backend_block_stub
            )
            add_test(NAME test_backend_fs_recovery COMMAND test_backend_fs_recovery)
        else()
            message(STATUS "Skipping test_backend_fs_recovery: requires fs/block stub targets")
        endif()

        add_executable(test_backend_fs_matrix tests/test_backend_fs_matrix.c)
        target_include_directories(test_backend_fs_matrix PRIVATE include tests src)
        if(TARGET lox_backend_fs_stub AND TARGET lox_backend_block_stub)
            target_link_libraries(
                test_backend_fs_matrix
                PRIVATE
                    loxdb
                    lox_backend_open
                    lox_backend_fs_stub
                    lox_backend_block_stub
            )
            add_test(
                NAME test_backend_fs_matrix_smoke
                COMMAND test_backend_fs_matrix --max-ms ${LOX_FS_MATRIX_SMOKE_MAX_MS}
            )
            add_test(
                NAME test_backend_fs_matrix_long
                COMMAND test_backend_fs_matrix --long --max-ms ${LOX_FS_MATRIX_LONG_MAX_MS}
            )
            set_tests_properties(test_backend_fs_matrix_smoke PROPERTIES LABELS "fs;matrix;smoke")
            set_tests_properties(test_backend_fs_matrix_long PROPERTIES LABELS "fs;matrix;long" TIMEOUT 180)
        else()
            message(STATUS "Skipping test_backend_fs_matrix*: requires fs/block stub targets")
        endif()

        add_executable(test_backend_managed_stress tests/test_backend_managed_stress.c)
        target_include_directories(test_backend_managed_stress PRIVATE include tests src)
        if(TARGET lox_backend_nand_stub)
            target_link_libraries(
                test_backend_managed_stress
                PRIVATE
                    loxdb
                    lox_backend_open
                    lox_backend_nand_stub
            )
            add_test(
                NAME test_backend_managed_stress_smoke
                COMMAND test_backend_managed_stress --max-ms ${LOX_MANAGED_STRESS_SMOKE_MAX_MS}
            )
            add_test(
                NAME test_backend_managed_stress_long
                COMMAND test_backend_managed_stress --long --max-ms ${LOX_MANAGED_STRESS_LONG_MAX_MS}
            )
            set_tests_properties(test_backend_managed_stress_smoke PROPERTIES LABELS "managed;stress;smoke")
            set_tests_properties(test_backend_managed_stress_long PROPERTIES LABELS "managed;stress;long" TIMEOUT 180)
        else()
            message(STATUS "Skipping test_backend_managed_stress*: requires nand stub target")
        endif()

        add_test(
            NAME test_optional_backend_strip_gate
            COMMAND ${CMAKE_COMMAND}
                -DDEFAULT_LIB=$<TARGET_FILE:loxdb>
                -DLLVM_READOBJ=${LOX_LLVM_READOBJ}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_optional_backends_unlinked.cmake
        )

        add_test(
            NAME test_optional_module_strip_gate
            COMMAND ${CMAKE_COMMAND}
                -DDEFAULT_LIB=$<TARGET_FILE:loxdb>
                -DLLVM_READOBJ=${LOX_LLVM_READOBJ}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_optional_modules_unlinked.cmake
        )
    endif()
endif()

if(DEFINED IDF_TARGET)
    idf_component_register(
        SRCS
            "src/loxdb.c"
            "src/lox_kv.c"
            "src/lox_ts.c"
            "src/lox_rel.c"
            "src/lox_wal.c"
            "src/lox_crc.c"
            "port/esp32/lox_port_esp32.c"
        INCLUDE_DIRS
            "include"
            "port/esp32"
        REQUIRES
            esp_partition
    )
endif()
