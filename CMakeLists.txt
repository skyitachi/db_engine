cmake_minimum_required(VERSION 3.12)
project(naive_db_engine)

set(CMAKE_CXX_STANDARD 14)

add_executable(naive_db_engine main.cpp data_store.cc door_plate.cc engine_race.cc util.cc log.cpp)