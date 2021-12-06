#include "../src/file_util.hpp"
#include <gtest/gtest.h>

TEST(tempfile, test1) {
  using namespace adl;
  TempFile *file = nullptr;
  auto rc = TempFile::Open("/home/adl", "tmp_sst_", &file);
  ASSERT_EQ(rc, OK) << rc << std::endl;

  auto s = "/home/adl/work.sst";
  rc = file->ReName(s);
  EXPECT_EQ(rc, OK) << rc << std::endl;

  EXPECT_TRUE(FileManager::Exists(s)) << s << " is not exist?" << std::endl;
  delete file;
}