set(WORT_SOURCES wort.cpp wort.h)

add_library(nvmkv-wort STATIC ${WORT_SOURCES})
target_compile_features(nvmkv-wort PUBLIC c_std_11 cxx_std_17)

target_link_libraries(nvmkv-wort PUBLIC nvmkv-fastalloc)