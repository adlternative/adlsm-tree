#include "../src/mem_table.hpp"
#include <gtest/gtest.h>
#include "../src/options.hpp"

TEST(mem_table_test, PutAndGet1) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  string user_key("key");
  MemKey key(user_key, 1, OP_PUT);
  string val;
  ASSERT_EQ(table.Put(key, "value"), OK) << "put error";
  ASSERT_EQ(table.Get(user_key, val), OK) << "get error";
  ASSERT_EQ(val, "value");
}

TEST(mem_table_test, PutAndGet) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  string val;
  MemKey key1("key", 1, OP_PUT);
  ASSERT_EQ(table.Put(key1, "value1"), OK) << "put error";
  MemKey key2("key", 2, OP_PUT);
  ASSERT_EQ(table.Put(key2, "value2"), OK) << "put error";
  ASSERT_EQ(table.Get("key", val), OK) << "get error";
  ASSERT_EQ(val, "value2");
}

TEST(mem_table_test, PutAndGet3) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  string val;
  MemKey key1("key", 1, OP_PUT);
  ASSERT_EQ(table.Put(key1, "value1"), OK) << "put error";
  MemKey key2("key", 2, OP_PUT);
  ASSERT_EQ(table.Put(key2, "value2"), OK) << "put error";
  MemKey key3("key", 3, OP_DELETE);
  ASSERT_EQ(table.Put(key3, ""), OK) << "put error";
  ASSERT_EQ(table.Get("key", val), NOT_FOUND) << "get ?";
}

TEST(mem_table_test, PutAndGet4) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  string val;
  MemKey key1("key", 1, OP_PUT);
  ASSERT_EQ(table.Put(key1, "value1"), OK) << "put error";
  MemKey key2("key", 2, OP_PUT);
  ASSERT_EQ(table.Put(key2, "value2"), OK) << "put error";
  MemKey key3("key", 3, OP_DELETE);
  ASSERT_EQ(table.Put(key3, ""), OK) << "put error";
  MemKey key4("key", 4, OP_PUT);
  ASSERT_EQ(table.Put(key4, "a silly apple\n"), OK) << "put error";
  ASSERT_EQ(table.Get("key", val), OK) << "get error";
  ASSERT_EQ(val, "a silly apple\n");
}

TEST(mem_table_test, PutAndGet5) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  string val;
  MemKey key1("key1", 1, OP_PUT);
  ASSERT_EQ(table.Put(key1, "value1"), OK) << "put error";
  MemKey key2("key2", 2, OP_PUT);
  ASSERT_EQ(table.Put(key2, "value2"), OK) << "put error";
  key2.seq_ = 3;
  ASSERT_EQ(table.Put(key2, "value3"), OK) << "put error";
  MemKey key3("key3", 4, OP_PUT);
  ASSERT_EQ(table.Put(key3, "value4"), OK) << "put error";
  key3.seq_ = 5;
  key3.op_type_ = OP_DELETE;
  ASSERT_EQ(table.Put(key3, "unused value"), OK) << "put error";
  ASSERT_EQ(table.Get("key1", val), OK) << "get error";
  ASSERT_EQ(val, "value1");
  ASSERT_EQ(table.Get("key2", val), OK) << "get error";
  ASSERT_EQ(val, "value3");
  ASSERT_EQ(table.Get("key3", val), NOT_FOUND) << "get error";
}

TEST(mem_table_test, PutAndGetBig) {
  using namespace adl;
  DBOptions opts;
  MemTable table(opts);
  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    MemKey key1(key, i, OP_PUT);
    ASSERT_EQ(table.Put(key1, val), OK) << "put error";
  }
  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(table.Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }
}