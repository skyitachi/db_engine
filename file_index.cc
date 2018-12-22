#include "file_index.h"

namespace polar_race {
  FileIndex::FileIndex(const std::string &filenameString, FILE *log) {
    log_ = log;
    const char *filename = filenameString.c_str();
    fd_ = open(filename, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
      fprintf(log_, "Error FileIndex open %s failed %s\n", filename, strerror(errno));
      exit(1);
    }
    loaded_ = false;
    keysCount_ = 0;
    struct stat st;
    if (fstat(fd_, &st) == 0) {
      indexFileSize_ = st.st_size;
    } else {
      fprintf(log_, "Error FileIndex open %s failed %s\n", filename, strerror(errno));
      exit(1);
    }
    indexItemList = (IndexItem *) malloc(indexFileSize_);
    fprintf(log_, "FileIndex open %s sucessfully\n", filename);
    keysCount_ = indexFileSize_ / kIndexItemLength;
  }

  RetCode FileIndex::Append(const std::string &key, int64_t *offset) {
    if (!loaded_) {
      load();
    }
    if (memoryIndex.find(key) == memoryIndex.end()) {
      struct IndexItem item;
      memcpy(item.keyBytes, key.data(), kKeyLength);
      item.offset = keysCount_;
      memoryIndex[key] = keysCount_;
      *offset = keysCount_;

      keysCount_ += 1;
      int n = write(fd_, &item, kIndexItemLength);
      if (n != kIndexItemLength) {
        // fprintf(log_, "FileIndex::Append write error just write %d byte\n", n);
        return kIOError;
      }
      // fprintf(log_, "FileIndex::Append successfully\n");
      return kSucc;
    }
    *offset = memoryIndex[key];
    // fprintf(log_, "FileIndex::Append no need for update index\n");
    return kSucc;
  }

  RetCode FileIndex::Lookup(const std::string &key, int64_t *offset) {
    if (!loaded_) {
      load();
    }
    if (memoryIndex.find(key) == memoryIndex.end()) {
      return kNotFound;
    }
    *offset = memoryIndex[key] * kValueLength;
    return kSucc;
  }

  void FileIndex::load() {
    if (loaded_)
      return;
    read(fd_, indexItemList, indexFileSize_);
    for (int i = 0; i < keysCount_; i++) {
      const std::string key(indexItemList[i].keyBytes, kKeyLength);
      memoryIndex[key] = indexItemList[i].offset;
    }
    loaded_ = true;
  }

} // namespace polar_race