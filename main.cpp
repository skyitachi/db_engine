#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <chrono>
#include <ctime>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio.hpp>
#include <mutex>
#include <thread>
#include <cstdint>
#include <stdlib.h>

#include "include/engine.h"

static const char kEnginePath[] = "/tmp/test_engine3";
static const char kDumpPath[] = "/tmp/test_dump";

using namespace polar_race;

extern int index_cnt;
extern std::chrono::duration<double> store_total;
extern std::chrono::duration<double> plate_total;
extern std::chrono::duration<double> calcindex_total;
extern std::chrono::duration<double> read_total;
extern std::chrono::duration<double> itemkeymatch_total;
extern std::chrono::duration<double> strhash_total;
extern std::chrono::duration<double> itemtry_total;
extern std::chrono::duration<double> access_item_total;

char gValueBuf[1024 * 4];
std::mutex gMutex;
std::mutex logMutex;
int counter = 0;
static FILE *engine_log;

void showBuffer(const char *buf, size_t n) {
  std::lock_guard<std::mutex> lock(logMutex);
  fprintf(engine_log, "buf is: ");
  for (int i = 0; i < n; i++) {
    fprintf(engine_log, "%x", buf[i]);
  }
  fprintf(engine_log, "\n");
}

class DumpVisitor : public Visitor {
public:
  DumpVisitor(int *kcnt)
      : key_cnt_(kcnt) {}

  ~DumpVisitor() {}

  void Visit(const PolarString &key, const PolarString &value) {
    printf("Visit %s --> %s\n", key.data(), value.data());
    (*key_cnt_)++;
  }

private:
  int *key_cnt_;
};

typedef uint64_t Key;

struct Comparator {
  int operator()(const Key &a, const Key &b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return 1;
    }
    return 0;
  }
};

std::string repeat(std::string repeated, int count) {
  std::string ret;
  for (int i = 0; i < count; i++) {
    ret += repeated;
  }
  return ret;
}

void benchmark_write(Engine *engine, int testCase) {
  std::chrono::duration<double> total;
  boost::asio::thread_pool pool(64);
  auto test_start = std::chrono::system_clock::now();
  for (int64_t i = 128; i < testCase; i++) {
    boost::asio::post(pool,
                      [i, engine, &total]() {
                        char keyBuf[8];
                        // 小端存储
                        memcpy(keyBuf, &i, sizeof(int64_t));
                        memcpy(gValueBuf, &i, sizeof(int64_t));
                        // showBuffer(keyBuf, 8);
                        polar_race::PolarString k(keyBuf, 8);
                        polar_race::PolarString v(gValueBuf, 4096);
                        auto start = std::chrono::system_clock::now();
                        RetCode ret = engine->Write(k, v);
                        auto end = std::chrono::system_clock::now();
                        std::lock_guard<std::mutex> lock(gMutex);
                        counter += 1;
                        auto el = (end - start);
                        total += el;
                      }
    );
  }
  pool.join();
  auto test_end = std::chrono::system_clock::now();
  std::cout << "global counter is " << counter << std::endl;
  std::cout << "write consumes: " << total.count() << " s\n";
  auto test_el = (test_end - test_start);
  std::chrono::duration<double> test_total;
  test_total += test_el;
  std::cout << "test case consumes: " << test_total.count() << " s\n";
  // std::cout << "consumes: " << std::chrono::duration_cast<std::chrono::milliseconds>(total.count()) << " ms\n";

}

void benchmark_read(Engine *engine, int testCase) {
  std::chrono::duration<double> total;
  char keyBuf[8];
  std::string value;
  int failed = 0;
  for (int64_t i = 0; i < testCase; i++) {
    memcpy(keyBuf, &i, sizeof(int64_t));
    polar_race::PolarString k(keyBuf, 8);
    auto start = std::chrono::system_clock::now();
    RetCode ret = engine->Read(k, &value);
    if (ret != kSucc) {
      failed += 1;
    }
    auto end = std::chrono::system_clock::now();
    auto el = (end - start);
    total += el;
    // std::cout << "value is: " << value << std::endl;
  }
  std::cout << "benchmark_read consumes: " << total.count() << " s, failed " << failed << std::endl;

}

void validate(Engine* engine) {
  for(int64_t i = 0; i < 10; i++) {
    char keyBuf[8];
    char valueBuf[4096];
    memset(valueBuf, 0, 4096);
    memcpy(keyBuf, &i, 8);
    memcpy(valueBuf, &i, 8);
    const PolarString key(keyBuf, 8);
    const PolarString value(valueBuf, 4096);

    polar_race::RetCode ret = engine->Write(key, value);
    if (ret != polar_race::kSucc) {
      std::cout << "write error happens\n";
    }

    std::string gotValue;
    ret = engine->Read(key, &gotValue);
    if (ret != polar_race::kSucc) {
      std::cout << "read error happens\n";
    }

    if (gotValue != value.ToString()) {
      std::cout << "read wrong value\n";
    }
  }

  std::cout << "engine validate ok\n";
}

int main(int argc, char **argv) {
  char cmd[1024];
  const std::string kEnginePathString(kEnginePath);
  const std::string kIndexPathString = kEnginePathString + "/KEY_INDEX";
  sprintf(cmd, "rm %s", kIndexPathString.c_str());
  std::cout << "delete cmds: " << cmd << std::endl;
//  system(cmd);

  const std::string kValuePathString = kEnginePathString + "/VALUE";
  sprintf(cmd, "rm %s", kValuePathString.c_str());
  std::cout << "delete cmds: " << cmd << std::endl;
//  system(cmd);
  int testCase = 10;
  if (argc > 1) {
    testCase = atoi(argv[1]);
  }
  std::cout << "test case: " << testCase << std::endl;

  Engine *engine = NULL;
  memset(gValueBuf, 0, 4 * 1024);
  auto start = std::chrono::system_clock::now();
  RetCode ret = Engine::Open(kEnginePath, false, &engine);
  assert (ret == kSucc);
  auto end = std::chrono::system_clock::now();
  auto el = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

//  validate(engine);
  // ret = engine->Write("aaa", "aaaaaaaaaaa");
  // assert (ret == kSucc);
  // ret = engine->Write("aaa", "111111111111111111111111111111111111111111");
  // ret = engine->Write("aaa", "2222222");
  // ret = engine->Write("aaa", "33333333333333333333");
  // ret = engine->Write("aaa", "4");

  // ret = engine->Write("bbb", "bbbbbbbbbbbb");
  // assert (ret == kSucc);

  // ret = engine->Write("ccd", "cbbbbbbbbbbbb");
  // std::string value;
  // ret = engine->Read("aaa", &value);
  // printf("Read aaa value: %s\n", value.c_str());

  // ret = engine->Read("bbb", &value);
  // assert (ret == kSucc);
  // printf("Read bbb value: %s\n", value.c_str());
  engine_log = fopen("./engine_test.log", "war+");
  benchmark_write(engine, testCase);
//
//  benchmark_read(engine, testCase);
  // std::cout << "calIndex_total consumes " << calcindex_total.count() << " s\n";
  // std::cout << "read_total consumes " << read_total.count() << " s\n";
  // std::cout << "index_count " << index_cnt << std::endl;
  // std::cout << "itemkeymatch_total consumes " << itemkeymatch_total.count() << " s\n";
  // std::cout << "strhash_total consumes " << strhash_total.count() << " s\n";
  // std::cout << "itemtry_total: " << itemtry_total.count() << " s\n";
  // std::cout << "access_item_total: " << access_item_total.count() << " s\n";

/*
  int key_cnt = 0;
  DumpVisitor vistor(&key_cnt);
  ret = engine->Range("b", "", vistor);
  assert (ret == kSucc);
  printf("Range key cnt: %d\n", key_cnt);

*/
  delete engine;
  return 0;
}
