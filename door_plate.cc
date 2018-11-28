// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include "util.h"
#include "door_plate.h"
#include <chrono>

#ifdef __MACH__
#include "posix_fallocate_mac.h"
#endif

std::chrono::duration<double> calcindex_total;

int index_cnt;
std::chrono::duration<double> itemkeymatch_total;
std::chrono::duration<double> strhash_total;
std::chrono::duration<double> itemtry_total;
std::chrono::duration<double> access_item_total;

namespace polar_race {

static const uint32_t kMaxDoorCnt = 1024 * 1024 * 32;
static const char kMetaFileName[] = "META";
static const int kMaxRangeBufCount = kMaxDoorCnt;

static bool ItemKeyMatch(const Item &item, const std::string& target) {
  auto st = std::chrono::system_clock::now();
  if (target.size() != item.key_size
      || memcmp(item.key, target.data(), item.key_size) != 0) {
    // Conflict
    auto ed = std::chrono::system_clock::now();
    itemkeymatch_total += (ed - st);
    return false;
  }
  auto ed = std::chrono::system_clock::now();
  itemkeymatch_total += (ed - st);
  return true;
}

inline static bool ItemKeyMatch2(const Item &item, const std::string& target) {
  auto st = std::chrono::system_clock::now();
  if (target.size() != item.key_size
      || memcmp(item.key, target.data(), item.key_size) != 0) {
    // Conflict
    auto ed = std::chrono::system_clock::now();
    itemkeymatch_total += (ed - st);
    return false;
  }
  auto ed = std::chrono::system_clock::now();
  itemkeymatch_total += (ed - st);
  return true;
}

static bool ItemTryPlace(const Item &item, const std::string& target) {
  auto st = std::chrono::system_clock::now();
  if (item.in_use == 0) {
    auto ed = std::chrono::system_clock::now();
    itemtry_total += (ed - st);
    access_item_total += (ed - st);
    return true;
  }
  auto ed = std::chrono::system_clock::now();
  bool ret = true;
  if (target.size() != item.key_size) {
    ret = false;
  } else if (memcmp(item.key, target.data(), item.key_size) != 0) {
    ret = false;
  }
  itemtry_total += (ed - st);
  return ret;
}

DoorPlate::DoorPlate(const std::string& path)
  : dir_(path),
  fd_(-1),
  items_(NULL) {
  }

RetCode DoorPlate::Init() {
  bool new_create = false;
  std::cout << "Item size: " << sizeof(Item) << std::endl;
  const int map_size = kMaxDoorCnt * sizeof(Item);

  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    return kIOError;
  }

  std::string path = dir_ + "/" + kMetaFileName;
  int fd = open(path.c_str(), O_RDWR, 0644);
  if (fd < 0 && errno == ENOENT) {
    // not exist, then create
    fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) {
      new_create = true;
      if (posix_fallocate(fd, 0, map_size) != 0) {
        std::cerr << "posix_fallocate failed: " << strerror(errno) << std::endl;
        close(fd);
        return kIOError;
      }
    }
  }
  if (fd < 0) {
    return kIOError;
  }
  fd_ = fd;

  void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
      MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    std::cerr << "MAP_FAILED: " << strerror(errno) << std::endl;
    close(fd);
    return kIOError;
  }
  if (new_create) {
    memset(ptr, 0, map_size);
  }

  items_ = reinterpret_cast<Item*>(ptr);
  return kSucc;
}

DoorPlate::~DoorPlate() {
  if (fd_ > 0) {
    const int map_size = kMaxDoorCnt * sizeof(Item);
    munmap(items_, map_size);
    close(fd_);
  }
}

// Very easy hash table, which deal conflict only by try the next one
int DoorPlate::CalcIndex(const std::string& key) {
  auto st = std::chrono::system_clock::now();
  uint32_t jcnt = 0;
  int index = StrHash(key.data(), key.size()) % kMaxDoorCnt;
  auto strhash_end = std::chrono::system_clock::now();
  strhash_total += (strhash_end - st);
  
  while (!ItemTryPlace(*(items_ + index), key)
      && ++jcnt < kMaxDoorCnt) {
    index = (index + 1) % kMaxDoorCnt;
    index_cnt ++;
  }

  if (jcnt == kMaxDoorCnt) {
    // full
    return -1;
  }
  auto end = std::chrono::system_clock::now();
  calcindex_total += (end - st);
  return index;
}

RetCode DoorPlate::AddOrUpdate(const std::string& key, const Location& l) {
  if (key.size() > kMaxKeyLen) {
    return kInvalidArgument;
  }

  int index = CalcIndex(key);
  if (index < 0) {
    return kFull;
  }

  Item* iptr = items_ + index;
  if (iptr->in_use == 0) {
    // new item
    memcpy(iptr->key, key.data(), key.size());
    iptr->key_size = key.size();
    iptr->in_use = 1;  // Place
  }
  iptr->location = l;
  return kSucc;
}

RetCode DoorPlate::Find(const std::string& key, Location *location) {
  
  int index = CalcIndex(key);
  if (index < 0
      || !ItemKeyMatch(*(items_ + index), key)) {
    return kNotFound;
  }

  *location = (items_ + index)->location;
  return kSucc;
}

RetCode DoorPlate::GetRangeLocation(const std::string& lower,
    const std::string& upper,
    std::map<std::string, Location> *locations) {
  int count = 0;
  for (Item *it = items_ + kMaxDoorCnt - 1; it >= items_; it--) {
    if (!it->in_use) {
      continue;
    }
    std::string key(it->key, it->key_size);
    if ((key >= lower || lower.empty())
        && (key < upper || upper.empty())) {
      locations->insert(std::pair<std::string, Location>(key, it->location));
      if (++count > kMaxRangeBufCount) {
        return kOutOfMemory;
      }
    }
  }
  return kSucc;
}

}  // namespace polar_race
