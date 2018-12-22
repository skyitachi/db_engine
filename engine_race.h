// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include "include/engine.h"
#include "util.h"
#include "door_plate.h"
#include "data_store.h"
#include "file_index.h"
#include "file_value.h"

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, bool append, Engine** eptr);

  explicit EngineRace(const std::string& dir, bool append):
    mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(nullptr), plate_(dir), store_(dir), append_(append) {
    log = fopen("./engine.log", "war+");
    keyIndex_ = new FileIndex(dir + std::string("/KEY_INDEX"), log);
    valueFile_ = new FileValue(dir + std::string("/VALUE"), append);
    valueFile_->setLog(log);
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

  private: 
    pthread_mutex_t mu_;
    FileLock* db_lock_;
    DoorPlate plate_;
    DataStore store_;
    FILE* log;
    FileIndex* keyIndex_;
    FileValue* valueFile_;
    bool append_;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
