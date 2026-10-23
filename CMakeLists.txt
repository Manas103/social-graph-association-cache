cmake_minimum_required(VERSION 3.16)
project(social_graph_association_cache CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

find_path(MYSQL_INCLUDE_DIR mysql.h PATH_SUFFIXES mysql)
find_library(MYSQL_LIB mysqlclient)

add_library(assoc_core src/store.cpp)
target_include_directories(assoc_core PUBLIC ${MYSQL_INCLUDE_DIR})
target_link_libraries(assoc_core PUBLIC ${MYSQL_LIB} pthread)

if(SANITIZER)
    target_compile_options(assoc_core PUBLIC -fsanitize=${SANITIZER} -g -O1 -fno-omit-frame-pointer)
    target_link_options(assoc_core PUBLIC -fsanitize=${SANITIZER})
endif()

add_executable(run_throughput_bench bench/run_throughput_bench.cpp)
target_link_libraries(run_throughput_bench assoc_core)

add_executable(run_raw_checker bench/run_raw_checker.cpp)
target_link_libraries(run_raw_checker assoc_core)

add_executable(crash_worker bench/crash_worker.cpp)
target_link_libraries(crash_worker assoc_core)

add_executable(run_crash_harness bench/run_crash_harness.cpp)
target_link_libraries(run_crash_harness assoc_core)

add_executable(run_tests tests/test_main.cpp)
target_link_libraries(run_tests assoc_core)
