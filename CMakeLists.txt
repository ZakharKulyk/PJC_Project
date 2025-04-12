cmake_minimum_required(VERSION 3.27)
project(cpp_Project)

set(CMAKE_CXX_STANDARD 23)

add_executable(cpp_Project main.cpp)

include(FetchContent)

FetchContent_Declare(
        fmt
        GIT_REPOSITORY  https://github.com/fmtlib/fmt
        GIT_TAG         11.0.2
)

FetchContent_MakeAvailable(fmt)

target_link_libraries(cpp_Project fmt)

