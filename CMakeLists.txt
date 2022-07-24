
cmake_minimum_required(VERSION 3.10)
project(processNewFiles C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "-Wall -Wextra")
set(CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")

include_directories(.)

add_executable( processNewFiles
                processNewFiles.c
                logStuff.c logStuff.h
              )

target_link_libraries( processNewFiles dl config argtable3 m )

install( TARGETS processNewFiles
         RUNTIME DESTINATION /usr/bin )