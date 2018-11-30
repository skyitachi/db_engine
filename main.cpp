#include <iostream>
#include "include/engine.h"
#include "engine_race.h"

int main() {
  polar_race::Engine* engine = nullptr;
  polar_race::Engine::Open("/tmp/test_kv_engine", &engine);
  engine->Write("k1", "v1");
  return 0;
}