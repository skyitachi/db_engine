cmake_minimum_required(VERSION 3.10)
project(naive_db_engine)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(naive_db_engine main.cpp engine_race.cc util.cc file_index.cc file_value.cpp file_value.h)

target_link_libraries(naive_db_engine pthread z boost_thread boost_system)
