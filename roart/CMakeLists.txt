set(ROART_SOURCES
        roart.cpp roart.h
        roart_node.cpp roart_node.h
        roart_key.cpp roart_key.h)

add_library(nvmkv-roart STATIC ${ROART_SOURCES})
target_link_libraries(nvmkv-roart PUBLIC nvmkv-fastalloc)