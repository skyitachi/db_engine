//
// Created by skyitachi on 2018/12/6.
//

#ifndef NAIVE_DB_ENGINE_MEMTABLE_H
#define NAIVE_DB_ENGINE_MEMTABLE_H

#include <map>
#include "include/polar_string.h"
#include "include/engine.h"

namespace polar_race {

class Memtable {
public:
  Memtable(int threshold): threshold_(threshold) {
    size_ = 0;
  }
  RetCode Insert(const PolarString& k, const PolarString& v);
  RetCode Get(const PolarString& k, std::string *value);
private:
  int threshold_;
  size_t size_;
  std::map<PolarString, PolarString> map_;
};

}


#endif //NAIVE_DB_ENGINE_MEMTABLE_H
