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