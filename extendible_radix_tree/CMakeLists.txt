set(ERT_SOURCES
        ERT_int.cpp ERT_int.h
        ERT_node_int.cpp ERT_node_int.h)

add_library(nvmkv-ert STATIC ${ERT_SOURCES})
target_link_libraries(nvmkv-ert PUBLIC nvmkv-fastalloc)
