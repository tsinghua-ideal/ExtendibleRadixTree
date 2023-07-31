# ERT

Extendible Radix Tree (ERT), an efficient indexing structure for PM that significantly reduces tree heights to minimize random reads, while still maintaining fast in-node search speed. The key idea is to use extendible hashing for each node in a radix tree. 

This repository contains the source codes of Extendible Radix Tree. The implementation includes Extendible Radix Tree, a random number generator for generating test data, and a simple memory manager as well as all other state-of-the-art works evaluated in the paper. 

Specifically, the `fastalloc` memory manager supports allocating memory in DRAM and space allocation in PM. 
The `extendible_radix_tree` contains the source codes of ERT. We also provide the source codes of `FAST&FAIR`, `LB+Trees`, `WORT`, `WOART`, and `ROART`.
To generate the graphs in the paper, we provide the scripts in the `Figure` part.


### Dependence

The evaluation requires the following hardware and software components to function properly:

#### Hardware
1. [Intel® Xeon® Platinum Processors](https://www.intel.com/content/www/us/en/products/details/processors/xeon/scalable/platinum.html)
2. [Intel Optane DCPMM](https://www.intel.com/content/www/us/en/products/docs/memory-storage/optane-persistent-memory/overview.html)                                                                                                             

#### Software
1. Linux 4.15 and above
2. C++17
3. CMake
4. Python 3.8

To set up the Optane DCPMM, please refer to the [document](https://www.intel.com/content/www/us/en/developer/articles/guide/qsg-intro-to-provisioning-pmem.html).
Note that Optane DCPMM should be mapped to a pre-defined address space through a DAX file system.

To facilitate the evaluation, we set up the evaluation environment on an Internet-accessible machine. The login credentials will be provided upon requests.

### Build and Run

By default, all the memory are allocated on DRAM. If you want to test it on persistent memory, please specify the PM file in the command line.
We provide scripts to evaluate in one-button. You can also build and run the benchmark manually.

#### Evaluate in one-button
```$xslt
sh run.sh
```

#### Build the benchmark
You can build the benchmark with the following command lines. In this benchmark, we evaluate insert and point query performance on synthetic data sets.
```
cmake .
make
```

#### Reproduce the results
To run the experiment, please specify the following parameters:

keyNum: the num of keys in the synthetic dataset

OptanePath: Optane DCPMM path where the memory will be allocated, by default, it will be allocated on DRAM.

```
./nvmkv <keyNum> <OptanePath>
```
For example:

```
// allocate memory on DRAM
./nvmkv 10000000

// allocate memory on PM
./nvmkv 10000000 /mnt/aep1/test
```

The results will be written into `./Result/insert.csv` and `./Result/query.csv` respectively.

#### Plot the figures

We provide the scripts to plot the insert and point query figures, corresponding to Figure 10 in the paper.
```asm
cd Figure
pip3 install -r requirements.txt
python3 plot_insert.py
python3 plot_query.py
```

### Contacts
Points of contacts for artifacts evaluation:

- [Ke Wang](https://skyelves.github.io/)

- [Yiwei Li](https://leepoly.com/about/)

### Reference

If you use this code in your research, please kindly cite the following paper.

Ke Wang, Guanqun Yang, Yiwei Li, Huanchen Zhang, and Mingyu Gao. [When Tree Meets Hash: Reducing Random Reads for Index Structures on Persistent Memories](https://dl.acm.org/doi/abs/10.1145/3588959). *Proc. ACM Manag. Data, Vol 1, No 1, Article 105* (SIGMOD). 2023.
