// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include "include/engine.h"
#include "util.h"
#include "door_plate.h"
#include "data_store.h"
#include "log.h"
#include "memtable.h"

namespace polar_race {

static const char kLogFile[] = "LOG";
static const int kThreshold = 1024;

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir):
    mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(nullptr),
    plate_(dir),
    store_(dir),
    log_(dir + "/" + kLogFile),
    mem_(kThreshold) {

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
    Memtable mem_;
    Log log_;

};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
