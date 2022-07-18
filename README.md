# ERT

The source codes of Extendible Radix Tree

Before running the codes, the directory of the Persistent Memory mounted should be specified.

You are supposed to replace "/mnt/aep1/test" with directory where you mount your PM. (the line 15 in the fastalloc.cpp)

```
    string nvm_filename = "/mnt/aep1/test";
```


how to run codes?

```
cmake .
make
./nvmkv
```
