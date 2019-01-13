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
int failed = 0;
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
    printf("Visit %s\n", key.data());
    (*key_cnt_)++;
  }

  const int Count() const { return *key_cnt_; }

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
  for (int64_t i = 0; i < testCase; i++) {
    boost::asio::post(pool,
                      [i, engine, &total]() {

                        char keyBuf[8];
                        // 小端存储
                        memcpy(keyBuf, &i, sizeof(int64_t));
                        memcpy(gValueBuf, &i, sizeof(int64_t));
                        // showBuffer(keyBuf, 8);
                        std::string sk = std::to_string(i);
                        sk.resize(8);
                        polar_race::PolarString k(sk);
                        polar_race::PolarString v(gValueBuf, 4096);
                        auto start = std::chrono::system_clock::now();
                        RetCode ret = engine->Write(k, v);
                        auto end = std::chrono::system_clock::now();
                        std::lock_guard<std::mutex> lock(gMutex);
                        counter += 1;
                        if (ret != kSucc) {
                          failed += 1;
                        }
                        auto el = (end - start);
                        total += el;
                      }
    );
  }
  pool.join();
  auto test_end = std::chrono::system_clock::now();
  std::cout << "global counter is " << counter << std::endl;
  std::cout << "global failed is " << failed << std::endl;
  std::cout << "write consumes: " << total.count() << " s\n";
  auto test_el = (test_end - test_start);
  std::chrono::duration<double> test_total;
  test_total += test_el;
  std::cout << "test case consumes: " << test_total.count() << " s\n";

}

void benchmark_read(Engine *engine, int testCase) {
  std::chrono::duration<double> total;
  int failed = 0;
  auto test_start = std::chrono::system_clock::now();
  boost::asio::thread_pool pool(64);
  for (int64_t i = 0; i < testCase; i++) {
    boost::asio::post(pool, [i, &failed, engine, &total]{
      char keyBuf[8];
      std::string value;
      memcpy(keyBuf, &i, sizeof(int64_t));
      polar_race::PolarString k(keyBuf, 8);
      auto start = std::chrono::system_clock::now();
      RetCode ret = engine->Read(k, &value);
      auto end = std::chrono::system_clock::now();
      auto el = (end - start);
      std::lock_guard<std::mutex> lock(gMutex);
      if (ret != kSucc) {
        failed += 1;
      }
      total += el;
    });
  }
  pool.join();
  auto test_end = std::chrono::system_clock::now();
  auto test_el = (test_end - test_start);
  std::chrono::duration<double> test_total;
  test_total += test_el;
  std::cout << "benchmark_read total read consumes: " << total.count() << " s, failed " << failed << std::endl;
  std::cout << "test case consumes: " << test_total.count() << " s\n";
}

void benchmark_read_one_thread(Engine *engine, int testCase) {
  std::chrono::duration<double> total;
  int failed = 0;
  auto test_start = std::chrono::system_clock::now();
  for (int64_t i = 0; i < testCase; i++) {
    char keyBuf[8];
    std::string value;
    memcpy(keyBuf, &i, sizeof(int64_t));
    polar_race::PolarString k(keyBuf, 8);
    auto start = std::chrono::system_clock::now();
    RetCode ret = engine->Read(k, &value);
    auto end = std::chrono::system_clock::now();
    auto el = (end - start);
    if (ret != kSucc) {
      failed += 1;
    }
    total += el;
  }
  auto test_end = std::chrono::system_clock::now();
  auto test_el = (test_end - test_start);
  std::chrono::duration<double> test_total;
  test_total += test_el;
  std::cout << "benchmark_read_one_thread total read consumes: " << total.count() << " s, failed " << failed << std::endl;
  std::cout << "test case consumes: " << test_total.count() << " s\n";
  std::cout << "pread consumes: " << read_total.count() << " s\n";
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
  int testCase = 10;
  if (argc > 1) {
    testCase = atoi(argv[1]);
  }
  std::cout << "test case: " << testCase << std::endl;

  Engine *engine = nullptr;
  memset(gValueBuf, 0, 4 * 1024);
  auto start = std::chrono::system_clock::now();
  RetCode ret = Engine::Open(kEnginePath, false, &engine);
  assert (ret == kSucc);
  auto end = std::chrono::system_clock::now();
  auto el = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  engine_log = fopen("./engine_test.log", "war+");
  
//  benchmark_write(engine, testCase);

  int key_count = 0;
  DumpVisitor visitor(&key_count);
  // have problem
  engine->Range("0", "1000", visitor);
  // 数据不一致的原因应该是用 int64_t 大小端的原因
  std::cout << visitor.Count() << std::endl;

//  benchmark_read_one_thread(engine, testCase);

  delete engine;
  return 0;
}
