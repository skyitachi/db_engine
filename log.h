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
    Log(std::string file) : file_(file) {
      fd_ = open(file.c_str(), O_WRONLY | O_APPEND | O_CREAT);
      if (fd_ < 0) {
        fprintf(stderr, "open file error %s", strerror(errno));
        exit(1);
      }
    }

    polar_race::RetCode AddRecord(const std::string &key, const std::string &v);

  private:
    std::string file_;
    int fd_;
  };


}
#endif //NAIVE_DB_ENGINE_LOG_H
