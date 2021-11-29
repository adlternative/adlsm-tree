#include "../src/db.hpp"
#include <gtest/gtest.h>

TEST(db, test_db_create) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb";
  opts.create_if_not_exists = true;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  ASSERT_EQ(db->Close(), OK);
  ASSERT_EQ(DB::Destroy(dbname), OK);
  delete db;
}

TEST(db, test_db_put_get) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    ASSERT_EQ(db->Delete(key), OK) << "delete error";
  }
  for (int i = 0; i < 10000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), NOT_FOUND) << "get ?";
  }

  ASSERT_EQ(db->Close(), OK);
  delete db;
}