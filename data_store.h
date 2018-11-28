// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_SIMPLE_DATA_STORE_H_
#define ENGINE_SIMPLE_DATA_STORE_H_
#include <string.h>
#include <unistd.h>
#include <string>
#include <map>
#include "include/engine.h"

namespace polar_race {

struct Location {
  Location() : file_no(0), offset(0), len(0) {
  }
  uint32_t file_no;
  uint32_t offset;
  uint32_t len;
};

class DataStore  {
 public:
  explicit DataStore(const std::string dir)
    : fd_(-1), dir_(dir) {}

  ~DataStore() {
    if (fd_ > 0) {
      close(fd_);
    }
    for(auto it = fds_.begin(); it != fds_.end(); it++) {
      close(it->second);
    }
  }

  RetCode Init();
  RetCode Read(const Location& l, std::string* value);
  RetCode Append(const std::string& value, Location* location);

 private:
  int fd_;
  std::string dir_;
  Location next_location_;
  std::map<uint32_t, int> fds_;
  RetCode OpenCurFile();
};

}  // namespace polar_race
#endif  // ENGINE_SIMPLE_DATA_STORE_H_
