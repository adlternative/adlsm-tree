cmake_minimum_required(VERSION 3.10)
project(adlsm-tree)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS_DEBUG_INIT "-std=c++2a -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE_INIT "-std=c++2a -g -O2")

include_directories(${PROJECT_SOURCE_DIR}/src)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

enable_testing()

add_executable(mem_table_test src/mem_table.cpp test/mem_table_test.cpp)
target_link_libraries(mem_table_test gtest gtest_main)

add_executable(db_test src/db.cpp test/db_test.cpp src/file_util.cpp src/mem_table.cpp)
target_link_libraries(db_test gtest gtest_main)

add_executable(block_test src/block.cpp src/sstable.cpp test/block_test.cpp)
target_link_libraries(block_test gtest gtest_main)

add_executable(filter_block_test src/encode.cpp src/filter_block.cpp  src/murmur3_hash.cpp test/filter_block_test.cpp)
target_link_libraries(filter_block_test gtest gtest_main)

add_test(NAME mem_table_test COMMAND mem_table_test)
add_test(NAME db_test COMMAND db_test)
add_test(NAME block_test COMMAND block_test)
add_test(NAME filter_block_test COMMAND filter_block_test)
