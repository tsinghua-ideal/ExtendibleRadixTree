set(FASTALLOC_SOURCES fastalloc.cpp fastalloc.h)

add_library (nvmkv-fastalloc STATIC ${FASTALLOC_SOURCES})
target_compile_features(nvmkv-fastalloc PUBLIC c_std_11 cxx_std_17)
