
cmake_minimum_required(VERSION 3.10)
project(processNewFiles C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "-Wall -Wextra")

include_directories(.)

add_executable( processNewFiles
        processNewFiles.c
         )

target_link_libraries( processNewFiles argtable3 m )

install( TARGETS processNewFiles
        RUNTIME DESTINATION /usr/bin )