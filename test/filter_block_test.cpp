#include "../src/filter_block.hpp"
#include <gtest/gtest.h>

TEST(filter_block, bloom_filter) {
  using namespace adl;
  string result;
  FilterBlock filter(make_unique<BloomFilter>(10));

  /* block0 */
  filter.Update("hello");
  filter.Update("world");
  filter.Update("hello-yly");
  filter.Update("hello-ddl");
  for (int i = 0; i < 10000; i++) {
    string s = "hello-ddl" + to_string(i);
    filter.Update(s);
  }
  //  if (current data more than 2k) {
  filter.Keys2Block();
  // }

  /* block1 */
  filter.Update("adl");
  filter.Update("dont");
  filter.Update("like-apple");

  //  if (current data more than 2k) {
  filter.Keys2Block();
  // }

  filter.Final(result);
  /* persist result */
  /* fetch result */
  FilterBlockReader fbr;
  ASSERT_EQ(fbr.Init(result), OK);

  EXPECT_FALSE(fbr.IsKeyExists(0, "adl"));
  EXPECT_FALSE(fbr.IsKeyExists(0, "zackboge"));
  EXPECT_TRUE(fbr.IsKeyExists(0, "hello-yly"));
  EXPECT_TRUE(fbr.IsKeyExists(0, "hello-ddl"));
  EXPECT_TRUE(fbr.IsKeyExists(0, "hello"));
  EXPECT_TRUE(fbr.IsKeyExists(0, "world"));

  for (int i = 0; i < 10000; i++) {
    string s = "hello-ddl" + to_string(i);
    EXPECT_TRUE(fbr.IsKeyExists(0, s));
  }

  EXPECT_TRUE(fbr.IsKeyExists(1, "adl"));
  EXPECT_TRUE(fbr.IsKeyExists(1, "dont"));
  EXPECT_TRUE(fbr.IsKeyExists(1, "like-apple"));
  EXPECT_FALSE(fbr.IsKeyExists(1, "dont like-apple"));
}