
cmake_minimum_required(VERSION 3.10)
set(CMAKE_C_STANDARD 17)

project(processNewFiles C)

set(CMAKE_C_FLAGS "-Wall -Wextra")
set(CMAKE_C_FLAGS_DEBUG "-DDEBUG=1 -g -O1 -fsanitize=address ${CMAKE_C_FLAGS_DEBUG}")
set(CMAKE_C_LINK_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG=1 -g -O1 -fsanitize=address -fno_omit_frame_pointer")

include_directories(.)

add_executable( processNewFiles
                processNewFiles.c processNewFiles.h
                logStuff.c logStuff.h
                events.c events.h
                rescan.c rescan.h inotify.c inotify.h )

target_link_libraries( processNewFiles dl config argtable3 m )
target_link_libraries( processNewFiles debug asan )

install( TARGETS processNewFiles
         RUNTIME DESTINATION /usr/bin )