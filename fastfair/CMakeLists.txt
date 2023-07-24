set(FASTFAIR_SOURCES fastfair.cpp fastfair.h)

add_library(nvmkv-fastfair STATIC ${FASTFAIR_SOURCES})

include_directories( /usr/local/include)

target_link_libraries(nvmkv-fastfair PUBLIC nvmkv-fastalloc)