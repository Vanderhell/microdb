# SPDX-License-Identifier: MIT
if(NOT DEFINED DEFAULT_LIB OR NOT DEFINED TINY_LIB)
  message(FATAL_ERROR "DEFAULT_LIB and TINY_LIB must be provided")
endif()

if(NOT EXISTS "${DEFAULT_LIB}")
  message(FATAL_ERROR "Default library not found: ${DEFAULT_LIB}")
endif()

if(NOT EXISTS "${TINY_LIB}")
  message(FATAL_ERROR "Tiny library not found: ${TINY_LIB}")
endif()

file(SIZE "${DEFAULT_LIB}" DEFAULT_SIZE)
file(SIZE "${TINY_LIB}" TINY_SIZE)

message(STATUS "microdb default size: ${DEFAULT_SIZE} bytes")
message(STATUS "microdb tiny size: ${TINY_SIZE} bytes")

if(TINY_SIZE GREATER DEFAULT_SIZE)
  message(FATAL_ERROR "Tiny library is larger than default (${TINY_SIZE} > ${DEFAULT_SIZE})")
endif()
