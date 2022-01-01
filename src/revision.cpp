#include "revision.hpp"
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fstream>
#include <sstream>
#include "defer.hpp"
#include "encode.hpp"
#include "file_util.hpp"
#include "hash_util.hpp"
#include "monitor_logger.hpp"
#include "rc.hpp"

namespace adl {

string Object::GetOid() const {
  string sha256_hex = sha256_digit_to_hex(sha256_digit_);
  return sha256_hex;
}

Level::Level(int level, const string_view &oid) : level_(level) {
  hex_to_sha256_digit(oid, sha256_digit_);
}

Level::Level() : level_(-1) {
  memset(sha256_digit_, 0, SHA256_DIGEST_LENGTH * sizeof(unsigned char));
}

Level::Level(const Level &rhs)
    : level_(rhs.level_), files_meta_(rhs.files_meta_) {
  memcpy(&sha256_digit_, &rhs.sha256_digit_, SHA256_DIGEST_LENGTH);
  memcpy(&sha256_, &rhs.sha256_, sizeof(SHA256_CTX));
}

Level &Level::operator=(Level &rhs) {
  if (this != &rhs) {
    level_ = rhs.level_;
    files_meta_ = rhs.files_meta_;
    sha256_ = rhs.sha256_;
    memcpy(&sha256_digit_, &rhs.sha256_digit_, SHA256_DIGEST_LENGTH);
  }
  return *this;
}

void Level::Insert(FileMetaData *file_meta) {
  files_meta_.insert(shared_ptr<FileMetaData>(file_meta));
}

void Level::Erase(FileMetaData *file_meta) {
  if (auto iter = files_meta_.find(file_meta); iter != files_meta_.end())
    files_meta_.erase(iter);
}
bool Level::Empty() const { return files_meta_.empty(); }

bool Level::HaveCheckSum() const {
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    if (sha256_digit_[i] != 0) return true;
  }
  return false;
}

int Level::GetLevel() const { return level_; }

RC Level::BuildFile(string_view dbname) {
  /* seq write file */
  TempFile *temp_level_file = nullptr;
  auto level_dir = LevelDir(dbname, level_);
  int file_num = (int)files_meta_.size();

  auto rc = FileManager::OpenTempFile(level_dir, "tmp_lvl", &temp_level_file);
  if (rc) return rc;

  SHA256_Init(&sha256_);
  /* level 4B */
  temp_level_file->Append({(char *)&level_, sizeof(int)});
  SHA256_Update(&sha256_, &level_, sizeof(int));
  /* file num 4B */
  temp_level_file->Append({(char *)&file_num, sizeof(int)});
  SHA256_Update(&sha256_, &file_num, sizeof(int));
  /* file meta [num_keys, min-key-size, min-key, max-key-size, max-key] */
  for (auto &file : files_meta_) {
    int num_keys = file->num_keys;
    string min_inner_key = file->min_inner_key.ToKey();
    int min_inner_key_size = (int)min_inner_key.size();
    string max_inner_key = file->max_inner_key.ToKey();
    int max_inner_key_size = (int)max_inner_key.size();
    string file_oid = sha256_digit_to_hex(file->sha256);

    MLog->debug("num_keys: {}", num_keys);
    MLog->debug("min_inner_key: {}", file->min_inner_key);
    MLog->debug("max_inner_key: {}", file->max_inner_key);

    temp_level_file->Append({(char *)&num_keys, sizeof(int)});
    SHA256_Update(&sha256_, (char *)&num_keys, sizeof(int));
    temp_level_file->Append({(char *)&min_inner_key_size, sizeof(int)});
    SHA256_Update(&sha256_, (char *)&min_inner_key_size, sizeof(int));
    temp_level_file->Append(min_inner_key);
    SHA256_Update(&sha256_, min_inner_key.data(), min_inner_key_size);
    temp_level_file->Append({(char *)&max_inner_key_size, sizeof(int)});
    SHA256_Update(&sha256_, (char *)&max_inner_key_size, sizeof(int));
    temp_level_file->Append(max_inner_key);
    SHA256_Update(&sha256_, max_inner_key.data(), max_inner_key_size);
    temp_level_file->Append(file_oid);
    SHA256_Update(&sha256_, file_oid.data(), file_oid.size());
  }
  SHA256_Final(sha256_digit_, &sha256_);
  string level_file_path =
      LevelFile(level_dir, sha256_digit_to_hex(sha256_digit_));

  if (rc = temp_level_file->ReName(level_file_path); rc) return rc;
  if (rc = temp_level_file->Close(); rc) return rc;
  MLog->info("level file {} created", level_file_path);
  delete temp_level_file;
  return OK;
}

RC Level::LoadFromFile(string_view dbname, string_view lvl_sha_hex) {
  RC rc = OK;
  hex_to_sha256_digit(lvl_sha_hex, sha256_digit_);
  /* 层级对象的地址 */
  string level_file_path = LevelFile(LevelDir(dbname, level_), lvl_sha_hex);
  /* 读取层级文件 */
  MmapReadAbleFile *level_file = nullptr;
  if (rc = FileManager::OpenMmapReadAbleFile(level_file_path, &level_file);
      rc) {
    MLog->error("open level file {} failed", level_file_path);
    return rc;
  }

  defer def([&]() {
    if (rc) {
      MLog->error("load level file {} failed", level_file_path);
      delete level_file;
    }
  });

  string_view buffer;
  int file_size = (int)level_file->Size();

  /* 1. HEAD level + file_num */
  int level;
  int file_num;
  if (file_size <= sizeof(int) * 2) {
    rc = BAD_LEVEL;
    return rc;
  }
  level_file->Read(0, sizeof(int) * 2, buffer);
  Decode32(buffer.data(), &level);
  Decode32(buffer.data() + sizeof(int), &file_num);
  if (level > 5 || level < 0 || file_num <= 0) {
    rc = BAD_LEVEL;
    return rc;
  }
  MLog->info("level file {} is loading, size {}", level_file_path, file_size);
  MLog->info("level {} file num {}", level, file_num);

  /* 2. file meta
  [num_keys, min-key-size, min-key, max-key-size, max-key, sstable-sha ] */
  level_file->Read(sizeof(int) * 2, file_size - sizeof(int) * 2, buffer);
  const char *cur_pointer = buffer.data();
  const char *end_pointer = buffer.data() + buffer.size();
  for (int i = 0; i < file_num; i++) {
    int num_keys;
    int min_key_len;
    int max_key_len;
    string min_key;
    string max_key;
    string sha256_hex;
    size_t sha256_hex_len = (size_t)(SHA256_DIGEST_LENGTH * 2);

    if (cur_pointer + sizeof(int) > end_pointer) {
      rc = BAD_LEVEL;
      return rc;
    }
    Decode32(cur_pointer, &num_keys);
    cur_pointer += sizeof(int);
    MLog->debug("num_keys {}", num_keys);

    if (cur_pointer + sizeof(int) > end_pointer) {
      rc = BAD_LEVEL;
      return rc;
    }
    Decode32(cur_pointer, &min_key_len);
    cur_pointer += sizeof(int);
    MLog->debug("min_key_len {}", min_key_len);

    if (cur_pointer + min_key_len > end_pointer) {
      rc = BAD_LEVEL;
      return rc;
    }
    min_key.assign(cur_pointer, min_key_len);
    cur_pointer += min_key_len;
    MLog->debug("min_key {}", min_key);

    if (cur_pointer + sizeof(int) > end_pointer) {
      rc = BAD_LEVEL;
      return rc;
    }
    Decode32(cur_pointer, &max_key_len);
    cur_pointer += sizeof(int);
    MLog->debug("max_key_len {}", max_key_len);

    if (cur_pointer + max_key_len > end_pointer) {
      rc = BAD_LEVEL;
      return rc;
    }
    max_key.assign(cur_pointer, max_key_len);
    cur_pointer += max_key_len;
    MLog->debug("max_key {}", max_key);

    if (cur_pointer + sha256_hex_len > end_pointer) {
      rc = BAD_LEVEL;
      return rc;
    }

    sha256_hex.assign(cur_pointer, sha256_hex_len);
    MLog->debug("sha256_hex {}", sha256_hex);

    cur_pointer += sha256_hex_len;
    FileMetaData *file_meta = new FileMetaData;
    file_meta->belong_to_level = level_;
    file_meta->num_keys = num_keys;
    file_meta->min_inner_key.FromKey(min_key);
    file_meta->max_inner_key.FromKey(max_key);
    file_meta->sstable_path = SstFile(SstDir(dbname), sha256_hex);
    if (rc = FileManager::GetFileSize(file_meta->sstable_path,
                                      &file_meta->file_size);
        rc)
      return rc;
    hex_to_sha256_digit(sha256_hex, file_meta->sha256);

    Insert(file_meta);
  }
  delete level_file;
  return rc;
}

RC Level::Get(string_view key, string &value) {
  RC rc = NOT_FOUND;
  if (level_) return UN_IMPLEMENTED;
  MemKey mk = MemKey::NewMinMemKey(key);
  if (!files_meta_.size()) {
    MLog->debug("not file in level {}?", GetOid());
    return rc;
  }
  MLog->debug("{} file in level {}", files_meta_.size(), GetOid());

  for (auto &file_meta : files_meta_) {
    if (mk < file_meta->min_inner_key &&
        mk.user_key_ != file_meta->min_inner_key.user_key_)
      continue;
    if (file_meta->max_inner_key < mk) continue;
    rc = file_meta->Get(key, value);
    if (rc == NOT_FOUND) {
      MLog->debug("Get key {} from file {} miss", key, file_meta->sstable_path);
      continue;
    }
    return rc;
  }
  return rc;
}

int64_t Level::GetMaxSeq() {
  if (files_meta_.empty()) return 0;
  auto back = files_meta_.end();
  back--;
  return back->get()->max_inner_key.seq_;
}

Revision::Revision(vector<Level> &&levels, deque<int64_t> &&log_nums) noexcept
    : levels_(std::move(levels)), log_nums_(std::move(log_nums)) {}

const vector<Level> &Revision::GetLevels() { return levels_; }

RC Revision::BuildFile(string_view dbname) {
  /* seq write file */
  TempFile *temp_rev_file = nullptr;
  auto rev_dir = RevDir(dbname);
  auto rc = FileManager::OpenTempFile(rev_dir, "tmp_rev", &temp_rev_file);
  if (rc) return rc;

  SHA256_Init(&sha256_);
  /* levels */
  for (const auto &level : levels_) {
    if (level.Empty()) continue;
    stringstream ss;
    ss << level.GetLevel() << " " << level.GetOid() << '\n';
    auto write_buffer = ss.str();
    SHA256_Update(&sha256_, write_buffer.c_str(), write_buffer.size());
    temp_rev_file->Append(write_buffer);
  }
  SHA256_Final(sha256_digit_, &sha256_);
  string rev_file_path = RevFile(rev_dir, sha256_digit_to_hex(sha256_digit_));

  if (rc = temp_rev_file->ReName(rev_file_path); rc) return rc;
  if (rc = temp_rev_file->Close(); rc) return rc;
  MLog->info("rev file {} created", rev_file_path);
  delete temp_rev_file;

  return OK;
}

RC Revision::LoadFromFile(string_view dbname, string_view rev_sha_hex) {
  MLog->info("load rev file {}", RevFile(RevDir(dbname), rev_sha_hex));
  hex_to_sha256_digit(rev_sha_hex, sha256_digit_);
  /* 版本对象的地址 */
  string rev_file_path = RevFile(RevDir(dbname), rev_sha_hex);
  /* 读取版本文件 */
  ifstream rev_file(rev_file_path);
  if (!rev_file.is_open()) {
    MLog->error("open rev file {} failed", rev_file_path);
    return NOT_FOUND;
  }

  /* level + oid */
  while (rev_file.good()) {
    string line;
    getline(rev_file, line);
    if (line.empty()) continue;
    stringstream ss(line);
    int level;
    string oid;
    /* 将 level 和 oid 解析出来，并构建版本对象 */
    ss >> level >> oid;
    /* 对于拥有的层进行加载 */
    levels_[level].SetLevel(level);
    if (auto rc = levels_[level].LoadFromFile(dbname, oid); rc) {
      MLog->error("load level {} {} from file {} failed", level, oid,
                  rev_file_path);
      rev_file.close();
      return rc;
    }
  }
  /* CHECK ? */
  return OK;
}

Revision::Revision() : /* seq_(0), */ levels_(5) {
  for (int i = 0; i < 5; ++i) levels_[i].SetLevel(i);
}

RC Revision::Get(string_view key, std::string &value) {
  RC rc = NOT_FOUND;
  for (int i = 0; i < 5 && rc == NOT_FOUND; ++i) {
    if (levels_[i].Empty()) continue;
    rc = levels_[i].Get(key, value);
    if (rc == NOT_FOUND) MLog->debug("Get key {} level {} miss", key, i);
  }
  return rc;
}

}  // namespace adl