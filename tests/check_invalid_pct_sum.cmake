# SPDX-License-Identifier: MIT
if(COMPILER_ID STREQUAL "MSVC")
    execute_process(
        COMMAND "${CC}" /nologo /I "${INCLUDE_DIR}" /c "${SRC}" /Fo"${OBJ}"
        RESULT_VARIABLE compile_result
        OUTPUT_VARIABLE compile_stdout
        ERROR_VARIABLE compile_stderr
    )
else()
    execute_process(
        COMMAND "${CC}" -I "${INCLUDE_DIR}" -c "${SRC}" -o "${OBJ}"
        RESULT_VARIABLE compile_result
        OUTPUT_VARIABLE compile_stdout
        ERROR_VARIABLE compile_stderr
    )
endif()

if(compile_result EQUAL 0)
    message(FATAL_ERROR "invalid PCT sum unexpectedly compiled")
endif()
