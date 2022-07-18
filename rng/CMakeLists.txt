set(RNG_SOURCES rng.cpp rng.h)

add_library(nvmkv-rng STATIC ${RNG_SOURCES})
target_compile_features(nvmkv-rng PUBLIC c_std_11 cxx_std_17)
