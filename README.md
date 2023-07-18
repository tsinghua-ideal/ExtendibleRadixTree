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

Note that Optane DCPMM should be mapped to a pre-defined address space through a DAX file system.

### Build and Run

```
cmake .
make
```
To run the experiment, specify the following parameters:

keyNum: the num of keys in the synthetic dataset

distribution: the synthetic dataset distributoin, 0 - dense, 1 - sparse

OptanePath: Optane DCPMM path where the memory will be allocated, by default

```
./nvmkv <keyNum> <distribution> <OptanePath>
```
For example:

```
./nvmkv 10000000 0 /mnt/aep1/test
```

### Reference

If you use this code in your research, please kindly cite the following paper.

Ke Wang, Guanqun Yang, Yiwei Li, Huanchen Zhang, and Mingyu Gao. When Tree Meets Hash: Reducing Random Reads for Index Structures on Persistent Memories. *Proc. ACM Manag. Data, Vol 1, No 1, Article 105* (SIGMOD). 2023.
