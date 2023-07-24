set(LBT_SOURCES lbtree.cc lbtree.h nodeprof.h)
set(CMAKE_CXX_STANDARD 14)

add_library(nvmkv-lbt STATIC ${LBT_SOURCES})

target_link_libraries(nvmkv-lbt PUBLIC nvmkv-fastalloc)