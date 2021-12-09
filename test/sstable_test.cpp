#include "../src/sstable.hpp"
#include <gtest/gtest.h>
#include "../src/file_util.hpp"
#include "../src/mem_table.hpp"
#include "../src/options.hpp"

void BuildSSTable(string_view dbname, string &sstable_path) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  for (int i = 0; i < 100; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    MemKey key1(key, i, OP_PUT);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }
  for (int i = 0; i < 100; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(table.Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }
  if (FileManager::Exists(dbname)) {
    ASSERT_EQ(FileManager::Destroy(dbname), OK);
    ASSERT_EQ(FileManager::Create(dbname, DIR_), OK);
  }
  ASSERT_EQ(table.BuildSSTable(dbname, sstable_path), OK);
}

TEST(sstable, memtable_to_sstable) {
  string unused;
  BuildSSTable("/tmp/feiwudb", unused);
}

TEST(sstable, sstable_reader) {
  using namespace adl;
  auto dbname = "/tmp/feiwudb2";
  string sstable_path;
  SSTableReader *table;
  MmapReadAbleFile *file;

  if (FileManager::Exists(dbname)) {
    ASSERT_EQ(FileManager::Destroy(dbname), OK);
    ASSERT_EQ(FileManager::Create(dbname, DIR_), OK);
  }
  BuildSSTable("/tmp/feiwudb2", sstable_path);
  auto rc = FileManager::OpenMmapReadAbleFile(sstable_path, &file);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  rc = SSTableReader::Open(file, &table);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  delete file;
  delete table;
}