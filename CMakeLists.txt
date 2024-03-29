cmake_minimum_required(VERSION 3.10)
project(adlsm-tree)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++2a -g -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++2a -O2")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#include_directories(${PROJECT_SOURCE_DIR}/src)


# debug 模式
SET(CMAKE_BUILD_TYPE "Debug")


SET(libadlsmtree_src
    src/block.cpp
    src/mem_table.cpp
    src/sstable.cpp
    src/file_util.cpp
    src/block.cpp
    src/filter_block.cpp
    src/footer_block.cpp
    src/murmur3_hash.cpp
    src/hash_util.cpp
    src/revision.cpp
    src/db.cpp
    src/keys.cpp
    src/encode.cpp
    src/back_ground_worker.cpp
    src/wal.cpp
    src/hash_util.cpp
    src/monitor_logger.cpp)

add_library(adlsmtree SHARED ${libadlsmtree_src})

add_library(adlsmtree_static ${libadlsmtree_src})

set_target_properties(adlsmtree_static PROPERTIES OUTPUT_NAME "adlsmtree")

enable_testing()

add_executable(mem_table_test
               src/block.cpp
               src/mem_table.cpp
               src/sstable.cpp
               src/file_util.cpp
               src/block.cpp
               src/filter_block.cpp
               src/footer_block.cpp
               src/murmur3_hash.cpp
               src/hash_util.cpp
               src/encode.cpp
               src/keys.cpp
               src/wal.cpp
               src/back_ground_worker.cpp
               src/monitor_logger.cpp
               test/mem_table_test.cpp)

target_link_libraries(mem_table_test crypto gtest gtest_main pthread fmt spdlog crc32c)

# 数据库整体测试
add_executable(db_test
               ${libadlsmtree_src}
               test/db_test.cpp)
target_link_libraries(db_test crypto gtest gtest_main pthread fmt spdlog crc32c)

# 块测试
add_executable(block_test
               src/encode.cpp
               src/block.cpp
               src/monitor_logger.cpp
               src/mem_table.cpp
               src/murmur3_hash.cpp
               src/filter_block.cpp
               src/footer_block.cpp
               src/sstable.cpp
               src/hash_util.cpp
               src/keys.cpp
               src/wal.cpp
               src/file_util.cpp
               test/block_test.cpp)
target_link_libraries(block_test crypto gtest gtest_main pthread fmt spdlog crc32c)

# 过滤器块测试
add_executable(filter_block_test
               src/monitor_logger.cpp
               src/encode.cpp
               src/filter_block.cpp
               src/murmur3_hash.cpp
               test/filter_block_test.cpp)
target_link_libraries(filter_block_test crypto gtest gtest_main pthread fmt spdlog)

# sstable 测试
add_executable(sstable_test
               src/block.cpp
               src/mem_table.cpp
               src/sstable.cpp
               src/file_util.cpp
               src/filter_block.cpp
               src/footer_block.cpp
               src/murmur3_hash.cpp
               src/hash_util.cpp
               src/encode.cpp
               src/keys.cpp
               src/wal.cpp
               src/back_ground_worker.cpp
               src/monitor_logger.cpp
               test/sstable_test.cpp)
target_link_libraries(sstable_test crypto gtest gtest_main pthread fmt spdlog crc32c)

# 文件工具测试
add_executable(file_util_test
               src/file_util.cpp
               src/monitor_logger.cpp
               src/wal.cpp
               src/keys.cpp
               src/hash_util.cpp
               src/encode.cpp
               test/file_util_test.cpp)
target_link_libraries(file_util_test gtest gtest_main pthread spdlog fmt crc32c)

# 缓存测试
add_executable(cache_test
               test/cache_test.cpp)
target_link_libraries(cache_test gtest gtest_main pthread spdlog fmt)


add_test(NAME mem_table_test COMMAND mem_table_test)
add_test(NAME db_test COMMAND db_test)
add_test(NAME block_test COMMAND block_test)
add_test(NAME filter_block_test COMMAND filter_block_test)
add_test(NAME sstable_test COMMAND sstable_test)
add_test(NAME file_util_test COMMAND file_util_test)
add_test(NAME cache_test COMMAND cache_test)