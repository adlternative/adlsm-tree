#include "../src/cache.hpp"
#include <gtest/gtest.h>

using namespace adl;
using namespace std;

TEST(cache, cache_miss) {
  LRUCache<string, string> cache;

  for (int i = 0; i < 65; i++) {
    string key("key");
    key += to_string(i);
    string val("val");
    val += to_string(i);
    cache.Put(key, val);
  }

  string result;
  bool hint = cache.Get("key0", result);
  EXPECT_FALSE(hint);
  for (int i = 1; i < 64; i++) {
    string key("key");
    key += to_string(i);
    string expect("val");
    expect += to_string(i);
    string val;
    bool hint = cache.Get(key, val);
    EXPECT_TRUE(hint);
    EXPECT_EQ(expect, val);
  }
}
