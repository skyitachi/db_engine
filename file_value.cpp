//
// Created by skyitachi on 2018/12/22.
//

#include "file_value.h"

std::chrono::duration<double> read_total;

namespace polar_race {

  RetCode FileValue::Write(const polar_race::PolarString &value, int64_t offset) {
    ssize_t nwrite = pwrite(fd_, value.data(), kValueLength, offset);
    fprintf(log_, "offset %lld\n", offset);
    if (nwrite != kValueLength) {
      fprintf(log_, "write value error: just write %zd\n", nwrite);
      return kIOError;
    }
    return kSucc;
  }

  RetCode FileValue::Read(int64_t offset, std::string *value) {
    char buf[kValueLength];
    auto start = std::chrono::system_clock::now();
    ssize_t nread = pread(fd_, buf, kValueLength, offset);
    auto end = std::chrono::system_clock::now();
    read_total += (end - start);
    if (nread != kValueLength) {
      fprintf(log_, "read value error: just write %zd\n", nread);
      return kIOError;
    }
    *value = std::string(buf, kValueLength);
    return kSucc;
  }
}
