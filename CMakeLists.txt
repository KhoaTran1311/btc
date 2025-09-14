cmake_minimum_required(VERSION 3.28)
project(btc)

set(CMAKE_CXX_STANDARD 20)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(btc
        ParseData.cpp
        PriceSchema.h
        PriceSchema.cpp
        FileIOUtil.h
        FileIOUtil.cpp)
target_link_libraries(btc PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
