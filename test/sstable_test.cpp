#include "../src/sstable.hpp"
#include <gtest/gtest.h>
#include "../src/file_util.hpp"
#include "../src/mem_table.hpp"
#include "../src/options.hpp"

void BuildSSTable(string_view dbname, adl::FileMetaData **meta_data_pointer) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    MemKey key1(key, i, OP_PUT);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }
  if (FileManager::Exists(dbname)) ASSERT_EQ(FileManager::Destroy(dbname), OK);
  ASSERT_EQ(FileManager::Create(dbname, DIR_), OK);
  ASSERT_EQ(FileManager::Create(SstDir(dbname), DIR_), OK);

  ASSERT_EQ(table.BuildSSTable(dbname, meta_data_pointer), OK);
}

void BuildSSTable2(string_view dbname, adl::FileMetaData **meta_data_pointer) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  int seq_count = 0;

  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i / 2);
    string val = "value" + to_string(i / 2);
    MemKey key1(key, seq_count++, !(i % 2) ? OP_PUT : OP_DELETE);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }
  if (FileManager::Exists(dbname)) ASSERT_EQ(FileManager::Destroy(dbname), OK);
  ASSERT_EQ(FileManager::Create(dbname, DIR_), OK);
  ASSERT_EQ(FileManager::Create(SstDir(dbname), DIR_), OK);
  ASSERT_EQ(table.BuildSSTable(dbname, meta_data_pointer), OK);
}

void BuildSSTable3(string_view dbname, adl::FileMetaData **meta_data_pointer) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  int seq_count = 0;

  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i / 2);
    string val = "value" + to_string(i / 2);
    MemKey key1(key, seq_count++, !(i % 2) ? OP_PUT : OP_DELETE);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }

  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i / 2);
    string val = "value" + to_string(i);
    MemKey key1(key, seq_count++, OP_PUT);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }

  if (FileManager::Exists(dbname)) ASSERT_EQ(FileManager::Destroy(dbname), OK);
  ASSERT_EQ(FileManager::Create(dbname, DIR_), OK);
  ASSERT_EQ(FileManager::Create(SstDir(dbname), DIR_), OK);
  ASSERT_EQ(table.BuildSSTable(dbname, meta_data_pointer), OK);
}

void BuildSSTable4(string_view dbname, adl::FileMetaData **meta_data_pointer) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  int seq_count = 0;

  for (int i = 0; i <= 10000; i++) {
    string key = "key";
    string val = "value" + to_string(i / 2);
    MemKey key1(key, seq_count++, !(i % 2) ? OP_PUT : OP_DELETE);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }

  if (FileManager::Exists(dbname)) ASSERT_EQ(FileManager::Destroy(dbname), OK);
  ASSERT_EQ(FileManager::Create(dbname, DIR_), OK);
  ASSERT_EQ(FileManager::Create(SstDir(dbname), DIR_), OK);
  ASSERT_EQ(table.BuildSSTable(dbname, meta_data_pointer), OK);
}

TEST(sstable, memtable_to_sstable) {
  adl::FileMetaData *unused = nullptr;
  BuildSSTable("/tmp/feiwudb", &unused);
  delete unused;
}

TEST(sstable, sstable_reader) {
  using namespace adl;
  auto dbname = "/tmp/feiwudb2";
  adl::FileMetaData *sstable_meta = nullptr;
  SSTableReader *sstable;
  MmapReadAbleFile *file;

  BuildSSTable(dbname, &sstable_meta);
  string sstable_path = sstable_meta->GetSSTablePath(dbname);
  auto rc = FileManager::OpenMmapReadAbleFile(sstable_path, &file);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  rc = SSTableReader::Open(file, &sstable);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val;
    string inner_key = NewMinInnerKey(key);
    auto rc = sstable->Get(inner_key, val);
    EXPECT_EQ(rc, OK) << "Get " << key << " error";
    if (rc == OK) {
      std::cout << "KEY " << key << "VAL " << val << std::endl;
      EXPECT_EQ(val, "value" + to_string(i));
    }
  }
  for (int i = 1; i < 10000; i++) {
    string key = "key" + to_string(10000 + i);
    string val;
    string inner_key = NewMinInnerKey(key);
    auto rc = sstable->Get(inner_key, val);
    EXPECT_EQ(rc, NOT_FOUND)
        << "Get error: KEY " << key << "VAL " << val << " should not be found";
    if (rc == OK) {
      EXPECT_EQ(val, "value" + to_string(i));
    }
  }
  delete sstable_meta;
  delete file;
  delete sstable;
}

TEST(sstable, sstable_reader2) {
  using namespace adl;
  auto dbname = "/tmp/feiwudb2";
  adl::FileMetaData *sstable_meta = nullptr;
  SSTableReader *sstable;
  MmapReadAbleFile *file;

  BuildSSTable2(dbname, &sstable_meta);
  string sstable_path = sstable_meta->GetSSTablePath(dbname);
  auto rc = FileManager::OpenMmapReadAbleFile(sstable_path, &file);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  rc = SSTableReader::Open(file, &sstable);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  for (int i = 1; i < 10000 / 2; i++) {
    string key = "key" + to_string(i);
    string val;
    string inner_key = NewMinInnerKey(key);
    auto rc = sstable->Get(inner_key, val);
    EXPECT_EQ(rc, NOT_FOUND)
        << "Get error: KEY " << key << "VAL " << val << " should not be found";
    if (rc == OK)
      EXPECT_EQ(val, "value" + to_string(i));
    else if (rc == NOT_FOUND) {
      std::cout << key << " NOT_FOUND" << std::endl;
    }
  }

  delete sstable_meta;
  delete file;
  delete sstable;
}

TEST(sstable, sstable_reader3) {
  using namespace adl;
  auto dbname = "/tmp/feiwudb2";
  adl::FileMetaData *sstable_meta = nullptr;
  SSTableReader *sstable;
  MmapReadAbleFile *file;

  BuildSSTable3(dbname, &sstable_meta);
  string sstable_path = sstable_meta->GetSSTablePath(dbname);
  auto rc = FileManager::OpenMmapReadAbleFile(sstable_path, &file);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  rc = SSTableReader::Open(file, &sstable);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  for (int i = 0; i < 10000 / 2; i++) {
    string key = "key" + to_string(i);
    string val;
    string inner_key = NewMinInnerKey(key);
    auto rc = sstable->Get(inner_key, val);
    EXPECT_EQ(rc, OK) << "Get " << key << " error";
    if (rc == OK) EXPECT_EQ(val, "value" + to_string(i * 2 + 1));
  }

  delete sstable_meta;
  delete file;
  delete sstable;
}

TEST(sstable, sstable_reader4) {
  using namespace adl;
  auto dbname = "/tmp/feiwudb2";
  adl::FileMetaData *sstable_meta = nullptr;
  SSTableReader *sstable;
  MmapReadAbleFile *file;

  BuildSSTable4(dbname, &sstable_meta);
  string sstable_path = sstable_meta->GetSSTablePath(dbname);
  auto rc = FileManager::OpenMmapReadAbleFile(sstable_path, &file);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  rc = SSTableReader::Open(file, &sstable);
  ASSERT_EQ(rc, OK) << "error: " << strrc(rc);
  string key = "key";
  string val;
  string inner_key = NewMinInnerKey(key);
  rc = sstable->Get(inner_key, val);
  EXPECT_EQ(rc, OK) << "Get " << key << " error";
  if (rc == OK) EXPECT_EQ(val, "value5000");

  delete sstable_meta;
  delete file;
  delete sstable;
}
