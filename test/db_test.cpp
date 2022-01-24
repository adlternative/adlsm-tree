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

TEST(db, test_db_put_get_and_reopen_get4) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "~/adldb";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });

  std::chrono::high_resolution_clock::time_point beginTime =
      std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  ASSERT_EQ(db->Close(), OK);
  delete db;

  std::chrono::high_resolution_clock::time_point endTime =
      std::chrono::high_resolution_clock::now();
  std::cout << "write time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  beginTime = std::chrono::high_resolution_clock::now();
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  endTime = std::chrono::high_resolution_clock::now();

  std::cout << "open time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  beginTime = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << "get error";
    ASSERT_EQ(val, "value" + to_string(i));
  }
  endTime = std::chrono::high_resolution_clock::now();
  std::cout << "get time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_compaction) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  opts.mem_table_max_size = 1UL << 20; /* 1MB */

  string dbname = "~/adldb";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });

  std::chrono::high_resolution_clock::time_point beginTime =
      std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  std::chrono::high_resolution_clock::time_point endTime =
      std::chrono::high_resolution_clock::now();
  std::cout << "write time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  ASSERT_EQ(db->Close(), OK);
  delete db;

  beginTime = std::chrono::high_resolution_clock::now();
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);
  endTime = std::chrono::high_resolution_clock::now();

  std::cout << "open time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  beginTime = std::chrono::high_resolution_clock::now();
  db->Debug();
  for (int i = 0; i < 300000; i++) {
    string key = "key" + to_string(i);
    string val;
    ASSERT_EQ(db->Get(key, val), OK) << fmt::format("bach {} get error", i);
    ASSERT_EQ(val, "value" + to_string(i));
  }
  endTime = std::chrono::high_resolution_clock::now();
  std::cout << "get time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  // db->Debug();

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_compaction2) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  opts.mem_table_max_size = 1UL << 20; /* 1MB */

  string dbname = "~/adldb";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });

  std::chrono::high_resolution_clock::time_point beginTime =
      std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 300000; i++) {
    string key = "key";
    string val = "value" + to_string(i);
    ASSERT_EQ(db->Put(key, val), OK) << "put error";
  }

  std::chrono::high_resolution_clock::time_point endTime =
      std::chrono::high_resolution_clock::now();
  std::cout << "write time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  ASSERT_EQ(db->Close(), OK);
  delete db;

  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  string key = "key";
  string val;
  ASSERT_EQ(db->Get(key, val), OK) << "get error";
  ASSERT_EQ(val, "value299999");
  // db->Debug();
  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_compaction3) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  opts.mem_table_max_size = 1UL << 14; /* 64KB */

  string dbname = "~/adldb";
  opts.create_if_not_exists = true;
  if (FileManager::Exists(dbname)) FileManager::Destroy(dbname);
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });

  auto beginTime = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 300000; i++) {
    string key = fmt::format("key{}", i);
    string val = fmt::format("value{}", i);
    ASSERT_EQ(db->Put(key, val), OK) << fmt::format("batch {} put error", i);
    ASSERT_EQ(db->Delete(key), OK) << fmt::format("batch {} delete error", i);
    val = fmt::format("value{}", i * 2);
    ASSERT_EQ(db->Put(key, val), OK) << fmt::format("batch {} put 2* error", i);
    ;
  }

  auto endTime = std::chrono::high_resolution_clock::now();
  std::cout << "write time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  ASSERT_EQ(db->Close(), OK);
  delete db;

  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  beginTime = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 300000; i++) {
    string key = fmt::format("key{}", i);
    string val;
    auto rc = db->Get(key, val);
    ASSERT_EQ(rc, OK) << fmt::format("batch {} get error with {}", i,
                                     strrc(rc));
    ASSERT_EQ(val, fmt::format("value{}", i * 2));
  }

  endTime = std::chrono::high_resolution_clock::now();
  std::cout << "read time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                     beginTime)
                   .count()
            << "ms\n";

  ASSERT_EQ(db->Close(), OK);
}

TEST(db, test_debug_sstable) {
  using namespace adl;
  DB *db = nullptr;
  DBOptions opts;
  opts.mem_table_max_size = 1UL << 20; /* 1MB */
  string dbname = "~/adldb";
  opts.create_if_not_exists = true;
  ASSERT_EQ(DB::Open(dbname, opts, &db), OK);

  defer _([&]() {
    if (db) delete db;
  });
  // db->DebugSSTable(
  // "776d6fe1b4b1f6a9b745a17ee5123fba23606f6a525d7743c3040a981ce761b6");
  // db->DebugSSTable(
  //     "dbd0619eca24cf8cd978dfbae93ce4407a7cbbec1dd7162ab7e4a6ceb56f61c0");
}