# SPDX-License-Identifier: MIT
if(NOT DEFINED DEFAULT_LIB OR DEFAULT_LIB STREQUAL "")
    message(FATAL_ERROR "DEFAULT_LIB is required")
endif()

if(NOT EXISTS "${DEFAULT_LIB}")
    message(FATAL_ERROR "DEFAULT_LIB does not exist: ${DEFAULT_LIB}")
endif()

if(NOT DEFINED LLVM_READOBJ OR LLVM_READOBJ STREQUAL "" OR LLVM_READOBJ MATCHES "NOTFOUND")
    message(STATUS "Skipping optional module strip gate: llvm-readobj not available")
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
        lox_json_kv_set_u32
        lox_json_encode_kv_record
        lox_ie_export_kv_json
        lox_ie_import_kv_json
        lox_ie_export_ts_json
        lox_ie_export_rel_json)
    string(FIND "${_out}" "${_sym}" _pos)
    if(NOT _pos EQUAL -1)
        message(FATAL_ERROR "Optional module symbol leaked into core library: ${_sym}")
    endif()
endforeach()
