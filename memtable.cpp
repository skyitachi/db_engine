//
// Created by skyitachi on 2018/12/6.
//

#include "memtable.h"

namespace polar_race {

RetCode Memtable::Insert(const polar_race::PolarString &k, const polar_race::PolarString &v) {
  map_.insert(k, v);
  size_ += v.size();
  if (size_ >= threshold_) {
    // TODO: write to *.ldb files
  }
  return kSucc;
}

RetCode Memtable::Get(const polar_race::PolarString &k, std::string *value) {
  auto it = map_.find(k);
  if (it == map_.end()) {
    return kNotFound;
  }
  *value = it->second.ToString();
  return kSucc;
}

}