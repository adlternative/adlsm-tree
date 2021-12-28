#include "../src/db.hpp"
#include <gtest/gtest.h>
#include "../src/defer.hpp"
using namespace adl;

TEST(db, test_db_create) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb";
  opts.create_if_not_exists = true;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  ASSERT_EQ(db->Close(), OK);
  ASSERT_EQ(DB::Destroy(dbname), OK);
  delete db;
}

TEST(db, test_db_write1) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "~/adldb";
  opts.create_if_not_exists = true;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    ASSERT_EQ(db->Delete(key), OK) << "delete error";
  }
  ASSERT_EQ(db->Close(), OK);
  delete db;
}

TEST(db, test_db_write2) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    ASSERT_EQ(db->Delete(key), OK) << "delete error";
  }
  ASSERT_EQ(db->Close(), OK);
  delete db;
}

TEST(db, test_db_put_get1) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });
  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    ASSERT_EQ(db->Delete(key), OK) << "delete error";
  }
  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), NOT_FOUND)
        << "get " << val << "is bug (batch " << i << ")";
  }

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_db_put_get_and_reopen_get) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });
  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  ASSERT_EQ(db->Close(), OK);
  delete db;

  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 10; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_db_put_get2) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });
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
    ASSERT_EQ(db->Get(key, val), NOT_FOUND)
        << "get " << val << "is bug (batch " << i << ")";
  }

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_db_put_get_and_reopen_get2) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });
  for (int i = 0; i < 100000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  for (int i = 0; i < 100000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  ASSERT_EQ(db->Close(), OK);
  delete db;

  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 100000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_db_put_get_and_reopen_get3) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  ASSERT_EQ(db->Close(), OK);
  delete db;

  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_current_create) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  auto rc = DB::Open(dbname, opts, &db);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  ASSERT_EQ(FileManager::Exists(CurrentFile(dbname)), true);
  ASSERT_EQ(db->Close(), OK);
  delete db;
}

TEST(db, test_db_reopen) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
    ASSERT_EQ(db->Close(), OK);
    delete db;
  }
  /* for (int i = 0; i < 5000; i++) {
      string key = "key" + to_string(i);
      string val = "value" + to_string(i);
      ASSERT_EQ(db->Put(key, val), OK) << "put error";
    }
    for (int i = 0; i < 5000; i++) {
      string key = "key" + to_string(i);
      ASSERT_EQ(db->Delete(key), OK) << "delete error";
    } */
}

TEST(db, test_db_reopen2) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 200000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }
  for (int i = 0; i < 200000; i++) {
    string key = "key" + to_string(i);
    ASSERT_EQ(db->Delete(key), OK) << "delete error";
  }
  ASSERT_EQ(db->Close(), OK);
  delete db;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  ASSERT_EQ(db->Close(), OK);
  delete db;
}

TEST(db, test_db_reopen3) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 200000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }
  for (int i = 0; i < 200000; i++) {
    string key = "key" + to_string(i);
    ASSERT_EQ(db->Delete(key), OK) << "delete error";
  }
  ASSERT_EQ(db->Close(), OK);
  delete db;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  for (int i = 0; i < 200000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }
  ASSERT_EQ(db->Close(), OK);
  delete db;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  ASSERT_EQ(db->Close(), OK);
  delete db;
}

/* used for test if can reopen db after crash it !!! */
TEST(db, test_db_open) {
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb1";
  auto rc = DB::Open(dbname, opts, &db);
  ASSERT_EQ(rc, OK) << strrc(rc);
  ASSERT_EQ(db->Close(), OK);
  delete db;
}
