#include "../src/block.hpp"
#include <fmt/core.h>
#include <gtest/gtest.h>
#include "../src/keys.hpp"
#include "../src/mem_table.hpp"
#include "../src/options.hpp"

using namespace std;

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

TEST(block_test, Get1) {
  using namespace adl;
  BlockWriter block;
  BlockReader block_reader;
  block.Add("key0", "v0");
  block.Add("key1", "v1");
  block.Add("key2", "v2");
  string result;
  block.Final(result);
  block.Reset();
  string value;
  string _;
  RC rc = OK;

  ASSERT_EQ(block_reader.Init(result, EasyCmp, EasySaveValue), OK);
  rc = block_reader.Get("key0", _, value);
  ASSERT_EQ(rc, OK) << strrc(rc);
  EXPECT_EQ(value, "v0");
  rc = block_reader.Get("key1", _, value);
  ASSERT_EQ(rc, OK) << strrc(rc);
  EXPECT_EQ(value, "v1");
  rc = block_reader.Get("key2", _, value);
  ASSERT_EQ(rc, OK) << strrc(rc);
  EXPECT_EQ(value, "v2");
}

TEST(block_test, Get2) {
  using namespace adl;
  BlockWriter block;
  BlockReader block_reader;
  map<string, string> m;
  string result;
  string _;
  RC rc = OK;
  for (int i = 0; i < 20; i++) {
    string key("key");
    key += std::to_string(i);
    string value("value");
    value += std::to_string(i);
    m.insert({key, value});
  }
  for (auto &p : m) block.Add(p.first, p.second);

  block.Final(result);
  block.Reset();

  ASSERT_EQ(block_reader.Init(result, EasyCmp, EasySaveValue), OK);

  for (int i = 0; i < 20; i++) {
    string key("key");
    key += std::to_string(i);
    string value;
    string expect("value");
    expect += std::to_string(i);
    rc = block_reader.Get(key, _, value);
    ASSERT_EQ(rc, OK) << "batch " << i << ": " << strrc(rc);
    EXPECT_EQ(value, expect);
  }
}

TEST(block_test, Get3) {
  using namespace adl;
  BlockWriter block;
  BlockReader block_reader;
  map<string, string> m;
  string result;
  RC rc = OK;
  for (int i = 0; i < 2000; i++) {
    string key("key");
    key += std::to_string(i);
    string value("value");
    value += std::to_string(i);
    m.insert({key, value});
  }
  for (auto &p : m) block.Add(p.first, p.second);

  block.Final(result);
  block.Reset();

  string _;
  ASSERT_EQ(block_reader.Init(result, EasyCmp, EasySaveValue), OK);

  for (int i = 0; i < 2000; i++) {
    string key("key");
    key += std::to_string(i);
    string value;
    string expect("value");
    expect += std::to_string(i);
    rc = block_reader.Get(key, _, value);
    ASSERT_EQ(rc, OK) << "batch " << i << ": " << strrc(rc);
    EXPECT_EQ(value, expect);
  }
}

TEST(block_test, Get4) {
  using namespace adl;
  BlockWriter block;
  BlockReader block_reader;
  DBOptions opt;
  MemTable mem_table(opt);
  string result;
  RC rc = OK;
  for (int i = 0; i < 2000; i++) {
    string key("key");
    key += std::to_string(i);
    string value("value");
    value += std::to_string(i);
    MemKey mem_key(key, i, OP_PUT);
    mem_table.Put(mem_key, value);
  }
  rc = mem_table.ForEachNoLock(
      [&](const MemKey &memkey, string_view value) -> RC {
        string key = memkey.ToKey();
        return block.Add(key, value);
      });
  ASSERT_EQ(rc, OK) << strrc(rc);

  block.Final(result);
  block.Reset();

  ASSERT_EQ(block_reader.Init(result, CmpInnerKey, SaveResultIfUserKeyMatch),
            OK);
  string _;

  for (int i = 0; i < 2000; i++) {
    string key("key");
    key += std::to_string(i);
    auto inner_key = NewMinInnerKey(key);
    string value;
    string expect("value");
    expect += std::to_string(i);
    rc = block_reader.Get(inner_key, _, value);
    ASSERT_EQ(rc, OK) << "batch " << i << ": " << strrc(rc);
    EXPECT_EQ(value, expect);
  }

  for (int i = 0; i < 2000; i++) {
    string key("key");
    key += std::to_string(2000 + i);
    auto inner_key = NewMinInnerKey(key);
    string value;
    rc = block_reader.Get(inner_key, _, value);
    ASSERT_EQ(rc, NOT_FOUND) << "batch " << i << ": " << strrc(rc);
  }

  for (int i = 0; i < 2000; i++) {
    string key("key");
    key += std::to_string(-1 - i);
    auto inner_key = NewMinInnerKey(key);
    string value;
    rc = block_reader.Get(inner_key, _, value);
    ASSERT_EQ(rc, NOT_FOUND) << "batch " << i << ": " << strrc(rc);
  }

  for (int i = 2000 - 1; i >= 0; --i) {
    string key("key");
    key += std::to_string(i);
    auto inner_key = NewMinInnerKey(key);
    string value;
    string expect("value");
    expect += std::to_string(i);
    rc = block_reader.Get(inner_key, _, value);
    ASSERT_EQ(rc, OK) << "batch " << i << ": " << strrc(rc);
    EXPECT_EQ(value, expect);
  }
}

TEST(block_test, Iterator) {
  using namespace adl;
  BlockWriter block;
  auto block_reader = make_shared<BlockReader>();
  map<string, string> m;
  string result;
  RC rc = OK;
  vector<pair<string, string>> vk;
  for (int i = 0; i < 10000; i++) {
    string key("key");
    key += std::to_string(i);
    string value("value");
    value += std::to_string(i);
    m.insert({key, value});
    vk.push_back({key, value});
  }
  sort(vk.begin(), vk.end(),
       [](pair<string, string> &l, pair<string, string> &r) -> bool {
         return l.first < r.first;
       });

  for (auto &p : m) block.Add(p.first, p.second);

  block.Final(result);
  block.Reset();

  ASSERT_EQ(block_reader->Init(result, EasyCmp, EasySaveValue), OK);
  int i = 0;
  for (auto iter = block_reader->begin(); iter != block_reader->end();
       iter++, i++) {
    if (!iter.Valid()) iter.Fetch();
    ASSERT_TRUE(iter.Valid()) << fmt::format("batch {}", i);
    ;
    // fmt::print("{} {}\n",iter.Key(),iter.Value());
    EXPECT_EQ(iter.Key(), vk[i].first) << fmt::format("batch {}", i);
    EXPECT_EQ(iter.Value(), vk[i].second) << fmt::format("batch {}", i);
  }
}