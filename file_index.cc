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
    mu_.lock();
//    std::lock_guard<std::mutex> lock(mu_);
    // Note: make sure loaded
//    if (!loaded_) {
//      load();
//    }
    if (memoryIndex.find(key) == memoryIndex.end()) {
      IndexItem item;
      memcpy(item.keyBytes, key.data(), kKeyLength);
      item.offset = keysCount_;
      memoryIndex[key] = keysCount_;
      *offset = keysCount_;

      keysCount_ += 1;
      ssize_t n = write(fd_, &item, kIndexItemLength);
      if (n != kIndexItemLength) {
         fprintf(log_, "FileIndex::Append write error just write %d byte\n", n);
         mu_.unlock();
        return kIOError;
      }
       fprintf(log_, "FileIndex::Append successfully\n");
      mu_.unlock();
      return kSucc;
    }
    mu_.unlock();
    *offset = memoryIndex[key];
    fprintf(log_, "FileIndex::Append no need for update index %s\n", fileName_.c_str());
    return kSucc;
  }

  RetCode FileIndex::Lookup(const std::string &key, int64_t *offset) {
//    std::lock_guard<std::mutex> lock(mu_);
    mu_.lock();
    if (!loaded_) {
      load();
    }
    if (memoryIndex.find(key) == memoryIndex.end()) {
      mu_.unlock();
      return kNotFound;
    }
    *offset = memoryIndex[key] * kValueLength;
    mu_.unlock();
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
          "Error FileIndex::load::read unexpected %zd size data, expected %lld when read %s in thread %lld\n",
          nread, indexFileSize_, fileName_.c_str(), std::this_thread::get_id());
      exit(1);
    } else {
      fprintf(log_, "FileIndex::load::read %zd bytes data for %s, in thread %lld\n", nread, fileName_.c_str(), std::this_thread::get_id());
    }
    fprintf(log_, "%lld thread before load %d keys\n", std::this_thread::get_id(), keysCount_);
    for (int i = 0; i < keysCount_; i++) {
      const std::string key(indexItemList[i].keyBytes, kKeyLength);
      memoryIndex[key] = indexItemList[i].offset;
    }
    fprintf(log_, "%lld thread load successfully\n", std::this_thread::get_id());
    loaded_ = true;
  }

} // namespace polar_race