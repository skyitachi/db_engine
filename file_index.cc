#include "file_index.h"

namespace polar_race {
  FileIndex::FileIndex(const std::string &filenameString, FILE *log) {
    indexItemList = nullptr;
    indexFileSize_ = 0;
    fileName_ = filenameString;
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
    if (indexFileSize_ > 0) {
      indexItemList = (IndexItem *) malloc(indexFileSize_);
      if (indexItemList == nullptr) {
        fprintf(log_, "Error FileIndex malloc %s failed %s\n", filename, strerror(errno));
        exit(1);
      }
    }
    fprintf(log_, "FileIndex open %s sucessfully\n", filename);
    keysCount_ = indexFileSize_ / kIndexItemLength;
    load();
  }

  RetCode FileIndex::Append(const std::string &key, int64_t *offset) {
    std::lock_guard<std::mutex> lock(mu_);
    if (memoryIndex.find(key) == memoryIndex.end()) {
      IndexItem item;
      memcpy(item.keyBytes, key.data(), kKeyLength);
      item.offset = keysCount_;
      memoryIndex[key] = keysCount_;
      *offset = keysCount_;

      keysCount_ += 1;
      ssize_t n = write(fd_, &item, kIndexItemLength);
      if (n != kIndexItemLength) {
         fprintf(log_, "FileIndex::Append write error just write %zd byte\n", n);
        return kIOError;
      }
      return kSucc;
    }
    *offset = memoryIndex[key];
    return kSucc;
  }

  RetCode FileIndex::Lookup(const std::string &key, int64_t *offset) {
    std::lock_guard<std::mutex> lock(mu_);
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
    if (indexFileSize_ == 0 || indexItemList == nullptr) {
      loaded_ = true;
    }
    if (loaded_)
      return;
    ssize_t nread = read(fd_, indexItemList, indexFileSize_);
    if (nread < indexFileSize_) {
      fprintf(log_,
          "Error FileIndex::load::read unexpected %zd size data, expected %lld when read %s in thread %x\n",
          nread, indexFileSize_, fileName_.c_str(), std::this_thread::get_id());
      exit(1);
    }
    for (int i = 0; i < keysCount_; i++) {
      const std::string key(indexItemList[i].keyBytes, kKeyLength);
      memoryIndex[key] = indexItemList[i].offset;
    }
    loaded_ = true;
  }

} // namespace polar_race