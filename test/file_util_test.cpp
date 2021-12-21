#include "../src/file_util.hpp"
#include <gtest/gtest.h>

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
  RC rc;
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