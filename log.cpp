//
// Created by skyitachi on 2018/11/29.
//

#include "log.h"
#include "util.h"

namespace polar_race {
  RetCode Log::AddRecord(const PolarString &key, const PolarString &v) {
    char buf[32];
    auto keyLen = key.size();
    auto valueLen = v.size();
    // TODO: 尽量一次调用
    WriteUnsignedLong(keyLen, buf);
    write(fd_, buf, sizeof(size_t));
    FileAppend(fd_, key.ToString());
    WriteUnsignedLong(valueLen, buf);
    write(fd_, buf, sizeof(size_t));
    FileAppend(fd_, v.ToString());
    return kSucc;
  }
}