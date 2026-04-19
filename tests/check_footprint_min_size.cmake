# SPDX-License-Identifier: MIT
if(NOT DEFINED EXE OR NOT EXISTS "${EXE}")
  message(FATAL_ERROR "EXE missing: ${EXE}")
endif()

if(NOT DEFINED MAP OR NOT EXISTS "${MAP}")
  message(FATAL_ERROR "MAP missing: ${MAP}")
endif()

if(NOT DEFINED MAX_TEXT OR NOT DEFINED MAX_RODATA OR NOT DEFINED MAX_DATA OR NOT DEFINED MAX_BSS)
  message(FATAL_ERROR "MAX_TEXT/MAX_RODATA/MAX_DATA/MAX_BSS are required")
endif()

set(TEXT_VS 0)
set(RDATA_VS 0)
set(DATA_INIT 0)
set(BSS_VS 0)
file(READ "${MAP}" MAP_CONTENT)
string(REPLACE "\r\n" "\n" MAP_CONTENT "${MAP_CONTENT}")
string(REPLACE "\n" ";" MAP_LINES "${MAP_CONTENT}")

foreach(LINE IN LISTS MAP_LINES)
  string(STRIP "${LINE}" LINE)
  if(LINE MATCHES "^[0-9A-Fa-f]+:[0-9A-Fa-f]+[ ]+([0-9A-Fa-f]+)H[ ]+([^ ]+)[ ]+(CODE|DATA)$")
    math(EXPR LEN "0x${CMAKE_MATCH_1}")
    set(SEC "${CMAKE_MATCH_2}")
    set(CLS "${CMAKE_MATCH_3}")
    if(CLS STREQUAL "CODE" AND SEC MATCHES "^\\.text")
      math(EXPR TEXT_VS "${TEXT_VS} + ${LEN}")
    elseif(CLS STREQUAL "DATA" AND SEC MATCHES "^\\.rdata")
      math(EXPR RDATA_VS "${RDATA_VS} + ${LEN}")
    elseif(CLS STREQUAL "DATA" AND SEC STREQUAL ".data")
      math(EXPR DATA_INIT "${DATA_INIT} + ${LEN}")
    elseif(CLS STREQUAL "DATA" AND SEC STREQUAL ".bss")
      math(EXPR BSS_VS "${BSS_VS} + ${LEN}")
    endif()
  endif()
endforeach()

set(BSS_TOTAL ${BSS_VS})

message(STATUS "footprint_min sections: .text=${TEXT_VS} .rdata=${RDATA_VS} .data=${DATA_INIT} .bss=${BSS_TOTAL}")
message(STATUS "footprint_min limits: .text<=${MAX_TEXT} .rdata<=${MAX_RODATA} .data<=${MAX_DATA} .bss<=${MAX_BSS}")

if(TEXT_VS GREATER MAX_TEXT)
  message(FATAL_ERROR ".text over limit: ${TEXT_VS} > ${MAX_TEXT}")
endif()
if(RDATA_VS GREATER MAX_RODATA)
  message(FATAL_ERROR ".rdata over limit: ${RDATA_VS} > ${MAX_RODATA}")
endif()
if(DATA_INIT GREATER MAX_DATA)
  message(FATAL_ERROR ".data over limit: ${DATA_INIT} > ${MAX_DATA}")
endif()
if(BSS_TOTAL GREATER MAX_BSS)
  message(FATAL_ERROR ".bss over limit: ${BSS_TOTAL} > ${MAX_BSS}")
endif()

string(TOLOWER "${MAP_CONTENT}" MAP_CONTENT_LOWER)

set(FORBIDDEN_OBJS
  "microdb_ts.obj"
  "microdb_rel.obj"
  "microdb_verify.obj"
  "soak_runner.obj"
  "worstcase_matrix_runner.obj"
)
foreach(OBJ IN LISTS FORBIDDEN_OBJS)
  string(FIND "${MAP_CONTENT_LOWER}" "${OBJ}" POS)
  if(NOT POS EQUAL -1)
    message(FATAL_ERROR "linkage audit failed: forbidden object linked: ${OBJ}")
  endif()
endforeach()

set(REQUIRED_OBJS
  "microdb.obj"
  "microdb_kv.obj"
  "microdb_wal.obj"
  "microdb_crc.obj"
)
foreach(OBJ IN LISTS REQUIRED_OBJS)
  string(FIND "${MAP_CONTENT_LOWER}" "${OBJ}" POS)
  if(POS EQUAL -1)
    message(FATAL_ERROR "linkage audit failed: required object missing: ${OBJ}")
  endif()
endforeach()
