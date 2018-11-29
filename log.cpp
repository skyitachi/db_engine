//
// Created by skyitachi on 2018/11/29.
//

#include "log.h"
#include "util.h"

namespace polar_race {
  RetCode Log::AddRecord(const std::string &key, const std::string &v) {
    char buf[32];
    auto keyLen = key.length();
    auto valueLen = v.length();
    // TODO: 尽量一次调用
    WriteUnsignedLong(keyLen, buf);
    write(fd_, buf, sizeof(unsigned long));
    FileAppend(fd_, key);
    WriteUnsignedLong(valueLen, buf);
    FileAppend(fd_, v);
  }
}