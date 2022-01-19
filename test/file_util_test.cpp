#include "../src/file_util.hpp"
#include <crc32c/crc32c.h>
#include <gtest/gtest.h>
#include "../src/monitor_logger.hpp"
#include "../src/wal.hpp"

using namespace adl;

TEST(tempfile, temp_file) {
  TempFile *file = nullptr;
  auto rc = TempFile::Open("~", "tmp_sst_", &file);
  ASSERT_EQ(rc, OK) << rc << std::endl;

  auto s = "~/work.sst";
  rc = file->ReName(s);
  EXPECT_EQ(rc, OK) << rc << std::endl;

  EXPECT_TRUE(FileManager::Exists(s)) << s << " is not exist?" << std::endl;
  delete file;
}

TEST(tempfile, mmap) {
  MmapReadAbleFile *file = nullptr;
  auto rc = FileManager::OpenMmapReadAbleFile("/etc/passwd", &file);
  EXPECT_EQ(rc, OK) << strrc(rc) << std::endl;
  auto file_size = file->Size();
  string_view buffer;
  rc = file->Read(0, 4, buffer);
  EXPECT_EQ(rc, OK) << strrc(rc) << std::endl;
  EXPECT_EQ("root", buffer);
  delete file;
}

TEST(tempfile, ReadFileToString) {
  WritAbleFile *file = nullptr;
  RC rc = OK;
  string result_string;
  string input_message("adl die today");

  rc = FileManager::OpenWritAbleFile("./a.txt", &file);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  rc = file->Append(input_message);
  delete file;
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  rc = FileManager::ReadFileToString("./a.txt", result_string);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  EXPECT_EQ(input_message, result_string);
}

TEST(wal, add) {
  WAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = adl::FileManager::OpenWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  vector<string> v{"adl", "is", "god"};
  for (int i = 0; i < v.size(); i++) wal->AddRecord(v[i]);
  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  for (int i = 0; i < v.size(); i++) {
    rc = reader->ReadRecord(record);
    ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
    EXPECT_EQ(v[i], record);
  }
  delete reader;
}

TEST(wal, add_with_eof) {
  WAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = adl::FileManager::OpenWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  vector<string> v{"adl", "is", "god"};
  for (int i = 0; i < v.size(); i++) wal->AddRecord(v[i]);
  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  vector<string> result;

  while (true) {
    rc = reader->ReadRecord(record);
    if (!rc)
      result.push_back(record);
    else
      break;
  }
  EXPECT_EQ(rc, FILE_EOF);
  ASSERT_EQ(result.size(), v.size());
  for (int i = 0; i < v.size(); i++) EXPECT_EQ(result[i], v[i]);
  delete reader;
}

TEST(wal, drop) {
  WAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = adl::FileManager::OpenWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  vector<string> v{"adl", "is", "god"};
  for (int i = 0; i < v.size(); i++) wal->AddRecord(v[i]);
  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  vector<string> result;

  while (true) {
    rc = reader->ReadRecord(record);
    if (!rc)
      result.push_back(record);
    else
      break;
  }
  EXPECT_EQ(rc, FILE_EOF);
  ASSERT_EQ(result.size(), v.size());
  for (int i = 0; i < v.size(); i++) EXPECT_EQ(result[i], v[i]);
  reader->Drop();
  delete reader;
  EXPECT_EQ(FileManager::Exists(WalFile(WalDir(db), log_number)), false);
}

class BadWAL : public WAL {
 public:
  BadWAL(WritAbleFile *wal_file) : WAL(wal_file) {}

  RC AddRecord(string_view data) override {
    RC rc = OK;
    /* checksum */
    uint32_t check_sum = crc32c::Crc32c(data);
    if (rc = wal_file_->Append({(char *)&check_sum, sizeof(uint32_t)}); rc)
      return rc;
    /* type */
    int type = wal_kv_;
    if (change_type_one_byte_) type = wal_kv_ + 1;
    if (rc = wal_file_->Append({(char *)&type, sizeof(int)}); rc) return rc;
    /* len */
    int lens = (int)data.length();
    if (add_len_) lens += 2;
    if (rc = wal_file_->Append({(char *)&lens, sizeof(int)}); rc) return rc;
    /* data */
    if (truncate_data_) {
      data = data.substr(0, data.length() / 2);
    }

    if (change_data_one_byte_) {
      string data_copy(data);
      data_copy[data_copy.length() / 2] = (char)0xfa;
      if (rc = wal_file_->Append(data_copy); rc) return rc;
    } else {
      if (rc = wal_file_->Append(data); rc) return rc;
    }

    return rc;
  }

  RC AddTuncateRecord(string_view data) {
    RC rc = OK;
    /* checksum */
    uint32_t check_sum = crc32c::Crc32c(data);
    if (rc = wal_file_->Append({(char *)&check_sum, sizeof(uint32_t)}); rc)
      return rc;
    /* type */
    int type = wal_kv_;
    if (rc = wal_file_->Append({(char *)&type, sizeof(int)}); rc) return rc;
    /* len */
    int lens = (int)data.length();
    if (rc = wal_file_->Append({(char *)&lens, sizeof(int)}); rc) return rc;
    /* data */
    if (rc = wal_file_->Append(data.substr(0, data.size() / 2)); rc) return rc;
    return rc;
  }
  ~BadWAL() = default;
  bool change_data_one_byte_ = false;
  bool change_type_one_byte_ = false;
  bool add_len_ = false;
  bool truncate_data_ = false;
};

RC OpenBadWAL(string_view dbname, int64_t log_number, BadWAL **result) {
  /* open append only file for wal */
  string wal_file_name = WalFile(WalDir(dbname), log_number);
  MLog->info("wal_file_name: {}", wal_file_name);

  WritAbleFile *wal_file = nullptr;
  if (auto rc = adl::FileManager::OpenAppendOnlyFile(wal_file_name, &wal_file);
      rc)
    return rc;
  *result = new BadWAL(wal_file);
  return OK;
}

TEST(wal, add_with_bad_record) {
  BadWAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = OpenBadWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  wal->change_data_one_byte_ = true;
  vector<string> v{"adl", "is", "god"};
  for (int i = 0; i < v.size(); i++) wal->AddRecord(v[i]);
  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  vector<string> result;

  rc = reader->ReadRecord(record);
  EXPECT_EQ(rc, CHECK_SUM_ERROR) << strrc(rc) << std::endl;
  ASSERT_EQ(result.size(), 0);
  delete reader;
}

TEST(wal, add_with_bad_record2) {
  BadWAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = OpenBadWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  wal->change_type_one_byte_ = true;

  vector<string> v(100, "asd");

  for (int i = 0; i < v.size(); i++) wal->AddRecord(v[i]);
  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  vector<string> result;

  rc = reader->ReadRecord(record);
  EXPECT_EQ(rc, BAD_RECORD) << strrc(rc) << std::endl;
  ASSERT_EQ(result.size(), 0);
  delete reader;
}

TEST(wal, add_with_bad_record3) {
  BadWAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = OpenBadWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  wal->add_len_ = true;

  vector<string> v(100, "asd");

  for (int i = 0; i < v.size(); i++) wal->AddRecord(v[i]);
  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  vector<string> result;

  rc = reader->ReadRecord(record);
  EXPECT_EQ(rc, CHECK_SUM_ERROR) << strrc(rc) << std::endl;
  ASSERT_EQ(result.size(), 0);
  delete reader;
}

TEST(wal, add_with_bad_record4) {
  BadWAL *wal = nullptr;
  string db(FileManager::FixDirName("./test-db/"));
  string waldir = WalDir(db);
  string oid(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  int64_t log_number = 0;
  if (FileManager::Exists(db)) FileManager::Destroy(db);
  EXPECT_EQ(OK, FileManager::Create(db, DIR_));
  EXPECT_EQ(OK, FileManager::Create(waldir, DIR_));

  auto rc = OpenBadWAL(db, log_number, &wal);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;

  vector<string> v(100, "asdfghjkl;ertyuiop");
  int i = 0;
  for (; i < v.size() - 1; i++) wal->AddRecord(v[i]);
  wal->AddTuncateRecord(v[i]);

  /* THE IMPORTANT SYNC */
  wal->Sync();
  wal->Close();
  delete wal;
  MLog->info("write ok");
  WALReader *reader = nullptr;
  rc = adl::FileManager::OpenWALReader(db, log_number, &reader);
  ASSERT_EQ(rc, OK) << strrc(rc) << std::endl;
  string record;
  vector<string> result;

  ASSERT_EQ(result.size(), 0);

  while (true) {
    rc = reader->ReadRecord(record);
    if (!rc)
      result.push_back(record);
    else
      break;
  }

  EXPECT_EQ(rc, FILE_EOF) << strrc(rc) << std::endl;
  /* 最后一条记录 */
  ASSERT_EQ(result.size(), v.size() - 1);
  for (int i = 0; i < v.size() - 1; i++) EXPECT_EQ(result[i], v[i]);

  delete reader;
}