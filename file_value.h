//
// Created by skyitachi on 2018/12/22.
//

#ifndef NAIVE_DB_ENGINE_FILE_VALUE_H
#define NAIVE_DB_ENGINE_FILE_VALUE_H

#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <mutex>
#include "include/engine.h"
#include "file_index.h"

namespace polar_race {

  class FileValue {
  public:
    FileValue(const std::string &filename, bool append) {
      if (append) {
        fd_ = open(filename.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
      } else {
        fd_ = open(filename.c_str(), O_CREAT | O_RDWR, 0644);
      }
      log_ = stdout;
    }

    RetCode Append(const PolarString &value) {
      ssize_t nwrite = write(fd_, value.data(), kValueLength);
      if (nwrite != kValueLength) {
        return kIOError;
      }
      return kSucc;
    }

    RetCode Write(const PolarString &value, int64_t offset);

    RetCode Read(int64_t offset, std::string *value);

    void setLog(FILE *log) {
      log_ = log;
    }


  private:
    int fd_;
    FILE *log_;
  };
}


#endif //NAIVE_DB_ENGINE_FILE_VALUE_H
