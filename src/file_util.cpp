#include "file_util.hpp"
#include <stdlib.h>
#include <string.h>
#include <sys/dir.h>
#include <sys/stat.h>
#include <unistd.h>
#include "rc.hpp"

namespace adl {

bool FileManager::Exists(const std::string &path) {
  struct stat sb;
  return stat(path.c_str(), &sb) == 0;
}

bool FileManager::IsDirectory(const std::string &path) {
  struct stat sb;
  return stat(path.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode);
}

RC FileManager::Create(const std::string &path, FileOptions options) {
  if (options == DIR_) {
    int err = mkdir(path.c_str(), 0755);
    if (err) {
      return CREATE_DIRECTORY_FAILED;
    }
  } else if (options == FILE_) {
    return UN_IMPLEMENTED;
  }
  return OK;
}

/* Dangerous rm -rf */
int remove_directory(const char *path) {
  DIR *d = opendir(path);
  size_t path_len = strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;
    while (!r && (p = readdir(d))) {
      int r2 = -1;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." and "/" as we
       * don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..") ||
          !strcmp(p->d_name, "/"))
        continue;

      len = path_len + strlen(p->d_name) + 2;
      buf = new char[len];

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path, p->d_name);
        if (!stat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode))
            r2 = remove_directory(buf);
          else
            r2 = unlink(buf);
        }
        delete[] buf;
      }
      r = r2;
    }
    closedir(d);
  }

  if (!r) r = rmdir(path);

  return r;
}

RC FileManager::Destroy(const std::string &path) {
  if (IsDirectory(path)) {
    int err = remove_directory(path.c_str());
    if (err) {
      return DESTROY_DIRECTORY_FAILED;
    }
  } else {
    int err = unlink(path.c_str());
    if (err) {
      return DESTROY_FILE_FAILED;
    }
  }
  return OK;
}

}  // namespace adl