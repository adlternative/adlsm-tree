#include "../src/block.hpp"
#include <gtest/gtest.h>

void AppendKV2Expect(std::string &expect, int shared_key_len,
                     int unshared_key_len, int value_len,
                     const std::string &key, const std::string &value) {
  expect.append(reinterpret_cast<char *>(&shared_key_len),
                sizeof(shared_key_len));
  expect.append(reinterpret_cast<char *>(&unshared_key_len),
                sizeof(unshared_key_len));
  expect.append(reinterpret_cast<char *>(&value_len), sizeof(value_len));
  expect.append(key, shared_key_len, unshared_key_len);
  expect.append(value);
}

void AppendRS2Expect(std::string &expect, std::vector<int> &&restarts) {
  for (int i = 0; i < restarts.size(); i++) {
    expect.append(reinterpret_cast<char *>(&restarts[i]), sizeof(int));
  }
  int restarts_len = restarts.size();
  expect.append(reinterpret_cast<char *>(&restarts_len), sizeof(int));
}

TEST(block_test, Add) {
  using namespace adl;
  BlockWriter block;
  block.Add("key0", "value");
  block.Add("key1", "value");
  string result;
  block.Final(result);

  string expect;
  AppendKV2Expect(expect, 0, 4, 5, "key0", "value");
  AppendKV2Expect(expect, 3, 1, 5, "key1", "value");
  AppendRS2Expect(expect, {0});
  EXPECT_EQ(result, expect);
}

TEST(block_test, Add2) {
  using namespace adl;
  BlockWriter block;
  block.Add("key0", "value0");
  block.Add("key1", "value1");
  block.Add("key12", "value12");
  string result;
  block.Final(result);
  string expect;

  AppendKV2Expect(expect, 0, 4, 6, "key0", "value0");
  AppendKV2Expect(expect, 3, 1, 6, "key1", "value1");
  AppendKV2Expect(expect, 4, 1, 7, "key12", "value12");
  AppendRS2Expect(expect, {0});
  EXPECT_EQ(result, expect);
}

TEST(block_test, Add3) {
  using namespace adl;
  BlockWriter block;
  string k;
  string v;
  for (int i = 0; i < 16; i++) {
    k = "key" + std::to_string(i);
    v = "value" + std::to_string(i);
    block.Add(k, v);
  }

  string result;

  EXPECT_EQ(block.EstimatedSize(), 330);

  block.Final(result);
  string expect;

  AppendKV2Expect(expect, 0, 4, 6, "key0", "value0");   /* 12 + 4 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key1", "value1");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key2", "value2");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key3", "value3");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key4", "value4");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key5", "value5");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key6", "value6");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key7", "value7");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key8", "value8");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 1, 6, "key9", "value9");   /* 12 + 1 + 6 */
  AppendKV2Expect(expect, 3, 2, 7, "key10", "value10"); /* 12 + 2 + 7 */
  AppendKV2Expect(expect, 4, 1, 7, "key11", "value11"); /* 12 + 1 + 7 */
  AppendKV2Expect(expect, 0, 5, 7, "key12", "value12"); /* 12 + 5 + 7 */
  AppendKV2Expect(expect, 4, 1, 7, "key13", "value13"); /* 12 + 1 + 7 */
  AppendKV2Expect(expect, 4, 1, 7, "key14", "value14"); /* 12 + 1 + 7 */
  AppendKV2Expect(expect, 4, 1, 7, "key15", "value15"); /* 12 + 1 + 7 */
  AppendRS2Expect(expect, {0, 234});
  EXPECT_EQ(result, expect);
}

TEST(block_test, Add4) {
  using namespace adl;
  BlockWriter block;
  block.Add("key0", "value");
  block.Add("key1", "value");
  string result;
  block.Final(result);
  string result2;
  block.Reset();
  block.Add("key0", "value");
  block.Add("key1", "value");
  block.Final(result2);

  string expect;
  AppendKV2Expect(expect, 0, 4, 5, "key0", "value");
  AppendKV2Expect(expect, 3, 1, 5, "key1", "value");
  AppendRS2Expect(expect, {0});
  EXPECT_EQ(result, expect);
  EXPECT_EQ(result, result2);
}