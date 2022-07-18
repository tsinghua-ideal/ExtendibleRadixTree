cmake_minimum_required(VERSION 3.10...3.19)
project(nvmkv)
add_subdirectory(extendible_radix_tree)
add_subdirectory(fastalloc)
add_subdirectory(rng)

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")

add_executable(nvmkv main.cpp)

target_link_libraries(nvmkv PRIVATE
        nvmkv-ert nvmkv-rng nvmkv-fastalloc)
