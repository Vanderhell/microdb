# SPDX-License-Identifier: MIT
if(NOT DEFINED DEFAULT_LIB OR DEFAULT_LIB STREQUAL "")
    message(FATAL_ERROR "DEFAULT_LIB is required")
endif()

if(NOT EXISTS "${DEFAULT_LIB}")
    message(FATAL_ERROR "DEFAULT_LIB does not exist: ${DEFAULT_LIB}")
endif()

if(NOT DEFINED LLVM_READOBJ OR LLVM_READOBJ STREQUAL "" OR LLVM_READOBJ MATCHES "NOTFOUND")
    message(STATUS "Skipping optional backend strip gate: llvm-readobj not available")
    return()
endif()

execute_process(
    COMMAND "${LLVM_READOBJ}" --symbols "${DEFAULT_LIB}"
    RESULT_VARIABLE _rc
    OUTPUT_VARIABLE _out
    ERROR_VARIABLE _err
)

if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "Failed to inspect symbols with llvm-readobj: ${_err}")
endif()

foreach(_sym
        microdb_backend_nand_stub_marker
        microdb_backend_emmc_stub_marker
        microdb_backend_sd_stub_marker
        microdb_backend_fs_stub_marker
        microdb_backend_block_stub_marker)
    string(FIND "${_out}" "${_sym}" _pos)
    if(NOT _pos EQUAL -1)
        message(FATAL_ERROR "Optional backend symbol leaked into core library: ${_sym}")
    endif()
endforeach()
