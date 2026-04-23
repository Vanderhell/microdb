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

macro(_accumulate_size VAR RAW)
  if("${RAW}" MATCHES "^0[xX][0-9A-Fa-f]+$")
    math(EXPR ${VAR} "${${VAR}} + ${RAW}")
  elseif("${RAW}" MATCHES "^[0-9]+$")
    math(EXPR ${VAR} "${${VAR}} + ${RAW}")
  elseif("${RAW}" MATCHES "^[0-9A-Fa-f]+$")
    math(EXPR ${VAR} "${${VAR}} + 0x${RAW}")
  endif()
endmacro()

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

if(TEXT_VS EQUAL 0 AND RDATA_VS EQUAL 0 AND DATA_INIT EQUAL 0 AND BSS_VS EQUAL 0)
  execute_process(
    COMMAND size -A "${EXE}"
    OUTPUT_VARIABLE SIZE_OUT
    ERROR_VARIABLE SIZE_ERR
    RESULT_VARIABLE SIZE_RC
  )
  if(SIZE_RC EQUAL 0)
    string(REPLACE "\r\n" "\n" SIZE_OUT "${SIZE_OUT}")
    string(REPLACE "\n" ";" SIZE_LINES "${SIZE_OUT}")
    foreach(SLINE IN LISTS SIZE_LINES)
      string(STRIP "${SLINE}" SLINE)
      if(SLINE MATCHES "^\\.text[ ]+([0-9]+)$")
        math(EXPR TEXT_VS "${TEXT_VS} + ${CMAKE_MATCH_1}")
      elseif(SLINE MATCHES "^\\.rodata[ ]+([0-9]+)$")
        math(EXPR RDATA_VS "${RDATA_VS} + ${CMAKE_MATCH_1}")
      elseif(SLINE MATCHES "^\\.data[ ]+([0-9]+)$")
        math(EXPR DATA_INIT "${DATA_INIT} + ${CMAKE_MATCH_1}")
      elseif(SLINE MATCHES "^\\.bss[ ]+([0-9]+)$")
        math(EXPR BSS_VS "${BSS_VS} + ${CMAKE_MATCH_1}")
      endif()
    endforeach()
  else()
    execute_process(
      COMMAND size -m "${EXE}"
      OUTPUT_VARIABLE SIZE_M_OUT
      ERROR_VARIABLE SIZE_M_ERR
      RESULT_VARIABLE SIZE_M_RC
    )
    if(SIZE_M_RC EQUAL 0)
      string(REPLACE "\r\n" "\n" SIZE_M_OUT "${SIZE_M_OUT}")
      string(REPLACE "\n" ";" SIZE_M_LINES "${SIZE_M_OUT}")
      foreach(SLINE IN LISTS SIZE_M_LINES)
        string(STRIP "${SLINE}" SLINE)
        if(SLINE MATCHES "^__TEXT[ ]+__text[ ]+([0-9A-Fa-fx]+)$")
          _accumulate_size(TEXT_VS "${CMAKE_MATCH_1}")
        elseif(SLINE MATCHES "^__TEXT[ ]+__(const|cstring|literal4|literal8|literal16)[ ]+([0-9A-Fa-fx]+)$")
          _accumulate_size(RDATA_VS "${CMAKE_MATCH_2}")
        elseif(SLINE MATCHES "^__DATA[ ]+__data[ ]+([0-9A-Fa-fx]+)$")
          _accumulate_size(DATA_INIT "${CMAKE_MATCH_1}")
        elseif(SLINE MATCHES "^__DATA[ ]+__bss[ ]+([0-9A-Fa-fx]+)$")
          _accumulate_size(BSS_VS "${CMAKE_MATCH_1}")
        elseif(SLINE MATCHES "^__DATA_DIRTY[ ]+__bss[ ]+([0-9A-Fa-fx]+)$")
          _accumulate_size(BSS_VS "${CMAKE_MATCH_1}")
        endif()
      endforeach()
    else()
      message(FATAL_ERROR "Unable to parse sections from MAP, size -A, or size -m. size -A error: ${SIZE_ERR} size -m error: ${SIZE_M_ERR}")
    endif()
  endif()
endif()

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

set(FORBIDDEN_PATTERNS
  "lox_ts(\\.c)?\\.(obj|o)"
  "lox_rel(\\.c)?\\.(obj|o)"
  "lox_verify(\\.c)?\\.(obj|o)"
  "soak_runner(\\.c)?\\.(obj|o)"
  "worstcase_matrix_runner(\\.c)?\\.(obj|o)"
)
foreach(PAT IN LISTS FORBIDDEN_PATTERNS)
  if(MAP_CONTENT_LOWER MATCHES "${PAT}")
    message(FATAL_ERROR "linkage audit failed: forbidden object linked (pattern): ${PAT}")
  endif()
endforeach()

set(REQUIRED_PATTERNS
  "loxdb(\\.c)?\\.(obj|o)"
  "lox_kv(\\.c)?\\.(obj|o)"
  "lox_wal(\\.c)?\\.(obj|o)"
  "lox_crc(\\.c)?\\.(obj|o)"
)
foreach(PAT IN LISTS REQUIRED_PATTERNS)
  if(NOT MAP_CONTENT_LOWER MATCHES "${PAT}")
    message(FATAL_ERROR "linkage audit failed: required object missing (pattern): ${PAT}")
  endif()
endforeach()
