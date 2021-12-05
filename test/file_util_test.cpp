#include "../src/file_util.hpp"
#include <gtest/gtest.h>

TEST(tempfile, test1) {
  using namespace adl;
  TempFile *file = nullptr;
  auto rc = TempFile::Open(&file);
  ASSERT_EQ(rc, OK) << rc << std::endl;
  delete file;
}