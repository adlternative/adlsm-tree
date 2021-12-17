#include "revision.hpp"
#include <sstream>
#include "hash_util.hpp"
#include "monitor_logger.hpp"

namespace adl {

Level::Level(const Level &rhs)
    : level_(rhs.level_), files_meta_(rhs.files_meta_), sha256_(rhs.sha256_) {
  memcpy(&sha256_digit_, &rhs.sha256_digit_, SHA256_DIGEST_LENGTH);
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

string Level::GetOid() const {
  string sha256_hex = sha256_digit_to_hex(sha256_digit_);
  return sha256_hex;
}

int Level::GetLevel() const { return level_; }

RC Level::BuildFile(string_view dbname) {
  /* seq write file */
  TempFile *temp_level_file = nullptr;
  auto level_dir = LevelDir(dbname, level_);
  auto rc = FileManager::OpenTempFile(level_dir, "tmp_lvl", &temp_level_file);
  if (rc) return rc;

  SHA256_Init(&sha256_);
  /* level */
  SHA256_Update(&sha256_, &level_, sizeof(int));
  temp_level_file->Append({(char *)&level_, sizeof(int)});
  /* file num */
  int file_num = (int)files_meta_.size();
  SHA256_Update(&sha256_, &file_num, sizeof(int));
  temp_level_file->Append({(char *)&file_num, sizeof(int)});
  /* file meta [min-key-size, min-key, max-key-size, max-key] */
  for (auto &file : files_meta_) {
    stringstream ss;
    string min_inner_key = file->min_inner_key.ToKey();
    string max_inner_key = file->max_inner_key.ToKey();

    ss << (int)min_inner_key.size() << min_inner_key
       << (int)max_inner_key.size() << max_inner_key
       << sha256_digit_to_hex(file->sha256);

    auto write_buffer = ss.str();
    SHA256_Update(&sha256_, write_buffer.c_str(), write_buffer.size());
    temp_level_file->Append(write_buffer);
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

Revision::Revision(vector<Level> &&levels) noexcept
    : levels_(std::move(levels)) {}

const vector<Level> &Revision::GetLevels() { return levels_; }

RC Revision::BuildFile(string_view dbname) {
  /* seq write file */
  TempFile *temp_rev_file = nullptr;
  auto rev_dir = RevDir(dbname);
  auto rc = FileManager::OpenTempFile(rev_dir, "tmp_rev", &temp_rev_file);
  if (rc) return rc;

  SHA256_Init(&sha256_);
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

string Revision::GetOid() const {
  string sha256_hex = sha256_digit_to_hex(sha256_digit_);
  return sha256_hex;
}

Revision::Revision() : levels_(5) {
  for (int i = 0; i < 5; ++i) levels_[i].SetLevel(i);
}

}  // namespace adl