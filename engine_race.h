// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include <vector>
#include <memory>
#include "include/engine.h"
#include "util.h"
#include "file_index.h"
#include "file_value.h"

namespace polar_race {

  static const int kSliceCount = 1024;
  static const std::string kFileIndexPrefix = "/KEY_INDEX";
  static const std::string kValueFilePrefix = "/VALUE";

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, bool append, Engine** eptr);

  explicit EngineRace(const std::string& dir, bool append):
    db_lock_(nullptr), dir_(dir), append_(append) {
    log = fopen("./engine.log", "war+");
    initFileIndexList();
    initValueFileList();
    initMutex();
  }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

  inline int partition(const PolarString& key) {
    return uint16_t(key.data()[0]) << 2 & uint16_t(key.data()[1]) >> 6;
  }
  
  inline void initMutex() {
    for (int i = 0; i <= kSliceCount; i++) {
      mutexList_.push_back(std::make_unique<std::mutex>());
    }
  }

  private:
    RetCode initFileIndexList();
    RetCode initValueFileList();
    FileLock* db_lock_;
    FILE* log;
    std::string dir_;
    std::vector<FileIndex*> keyIndexList_;
    std::vector<FileValue*> valueFileList_;
    std::vector<std::unique_ptr<std::mutex>> mutexList_;
    bool append_;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
