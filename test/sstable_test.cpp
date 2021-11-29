#include "../src/sstable.hpp"
#include <gtest/gtest.h>
#include "../src/mem_table.hpp"
#include "../src/options.hpp"

TEST(sstable, test1) {
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
  string dbname = "/tmp";
  ASSERT_EQ(table.BuildSSTable(dbname), OK);
}
