# ERT

The source codes of Extendible Radix Tree. The implementation includes Extendible Radix Tree, a random number generator for generating test data, and a simple memory manager. 

The `fastalloc` memory manager supports allocating memory in DRAM and space allocation in NVM. Please refer to the `Environment` section for specific usage instructions.

### Dependence

* C++17
* Unix operating system
* CMake

### Environment

This data structure can run on Unix machines. The default code creates the data structure in DRAM and tests it. 

If you want to test it on the NVM, please follow the steps:

* uncomment the code in `fastalloc.cpp`. 

* specify the directory of the Persistent Memory mounted by replacing "/mnt/aep1/test" in line 15 in `fastalloc.cpp`.

```
string nvm_filename = "/mnt/aep1/test";
```

### Build and Run

```
cmake .
make
./nvmkv
```
