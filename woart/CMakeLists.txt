set(WOART_SOURCES woart.cpp woart.h)

add_library(nvmkv-woart STATIC ${WOART_SOURCES})
target_compile_features(nvmkv-woart PUBLIC c_std_11 cxx_std_17)

target_link_libraries(nvmkv-woart PUBLIC nvmkv-fastalloc)