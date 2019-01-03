// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>
#include <chrono>
#include <ctime>
#include "util.h"
#include "engine_race.h"

namespace polar_race {

  static const char kLockFile[] = "LOCK";


  RetCode Engine::Open(const std::string &name, bool append, Engine **eptr) {
    return EngineRace::Open(name, append, eptr);
  }

  Engine::~Engine() {
  }

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
  RetCode EngineRace::Open(const std::string &name, bool append, Engine **eptr) {
    *eptr = NULL;
    EngineRace *engine_race = new EngineRace(name, append);
    *eptr = engine_race;
    return kSucc;
  }

// 2. Close engine
  EngineRace::~EngineRace() {
    if (db_lock_) {
      UnlockFile(db_lock_);
    }
  }

// 3. Write a key-value pair into engine
  RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
    int slice = partition(key);
    int64_t valueOffset;
    RetCode ret = keyIndexList_[slice]->Append(key.ToString(), &valueOffset);
    if (ret == kSucc) {
      if (append_) {
        ret = valueFileList_[slice]->Append(value);
      } else {
        ret = valueFileList_[slice]->Write(value, valueOffset * kValueLength);
      }
      if (ret != kSucc) {
        return ret;
      }
    }
    return kSucc;
  }

// 4. Read value of a key
  RetCode EngineRace::Read(const PolarString &key, std::string *value) {
    int slice = partition(key);
    int64_t offset;
    RetCode ret = keyIndexList_[slice]->Lookup(key.ToString(), &offset);
    if (ret == kSucc) {
      ret = valueFileList_[slice]->Read(offset, value);
      if (ret != kSucc) {
        return ret;
      }
    }
    return kSucc;
  }

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
  RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                            Visitor &visitor) {
    return kSucc;
  }

  RetCode EngineRace::initFileIndexList() {
    for(int i = 0; i < kSliceCount; i++) {
      keyIndexList_.push_back(new FileIndex(dir_ + kFileIndexPrefix + std::to_string(i), log));
    }
    return kSucc;
  }

  RetCode EngineRace::initValueFileList() {
    for(int i = 0; i < kSliceCount; i++) {
      FileValue* fileValue = new FileValue(dir_ + kValueFilePrefix + std::to_string(i), append_);
      fileValue->setLog(log);
      valueFileList_.push_back(fileValue);
    }
    return kSucc;
  }
}  // namespace polar_race
