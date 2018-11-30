//
// Created by skyitachi on 2018/11/29.
//

#ifndef NAIVE_DB_ENGINE_LOG_H
#define NAIVE_DB_ENGINE_LOG_H

#include <string>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include "include/engine.h"

namespace polar_race {
  class Log {
  public:
    Log(std::string file) : file_(file) {}

    RetCode Init() {
      fd_ = open(file_.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
      if (fd_ < 0) {
        fprintf(stderr, "open file %s error %s", file_.data(), strerror(errno));
        return kNotFound;
      }
      return kSucc;
    }
    ~Log() {
      close(fd_);
    }
    polar_race::RetCode AddRecord(const PolarString &key, const PolarString &v);

  private:
    std::string file_;
    int fd_;
  };


}
#endif //NAIVE_DB_ENGINE_LOG_H
