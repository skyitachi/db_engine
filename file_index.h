#ifndef INCLUDE_POLAR_FILE_INDEX_H_
#define INCLUDE_POLAR_FILE_INDEX_H_

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <map>
#include <unistd.h>
#include <fcntl.h>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include "include/engine.h"
namespace polar_race {

  static const int64_t kValueLength = 4096;
  static const int kKeyLength = 8;
  static const int kIndexItemLength = 16;

  class FileValue;

  class FileIndex {
  public:
    struct IndexItem {
      char keyBytes[8];
      int64_t offset;
    };
    typedef RetCode RangeCallback(int64_t, const std::string&, Visitor &visitor, FileValue*);
    FileIndex(const std::string &filenameString, FILE *log);
    
    ~FileIndex() {
      fprintf(log_, "%s destoryed\n", fileName_.c_str());
    }
    // 顺序写
    RetCode Append(const std::string &key, int64_t *offset);

    RetCode Lookup(const std::string &key, int64_t *offset);

    RetCode Range(const std::string&, const std::string&, Visitor&, FileValue*, RangeCallback rcb);

  private:
    void load();

    int fd_;
    int64_t keysCount_;
    bool loaded_;
    int64_t indexFileSize_;
    std::map<const std::string, int64_t> memoryIndex;
    IndexItem *indexItemList;
    FILE *log_;
    std::string fileName_;
    

  };
}

#endif