cmake_minimum_required(VERSION 3.12)
project(naive_db_engine)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(naive_db_engine main.cpp data_store.cc door_plate.cc engine_race.cc util.cc file_index.cc file_value.cpp file_value.h)

target_link_libraries(naive_db_engine pthread z boost_thread-mt boost_system-mt)