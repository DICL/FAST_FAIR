# FAST_FAIR

## Folder Structure

Files of interest:

- `single-no-ordering`: In-memory B-tree w/o any ordering constraints
- `single-fast-fair`: Paper version of the NVM B-tree, w/ both FAST & FAIR optimizations
- `single-full-ordering`: B-tree w/ full ordering constaints, i.e., w/o FAST optimization
- `quartz/nvmemul.ini`: Quartz NVM emulator config file


## Run with Quartz

Build Quartz: please follow the instructions in README inside the `quartz/` directory.

One-time load of the Quartz kernel module:

```bash
sudo quartz/scripts/setupdev.sh load
```

Set proper NVM parameter values in `quartz/nvmemul.ini`.

Run the B-tree app over Quartz:

```bash
cd single-xxx
make
sudo ../quartz/scripts/runenv.sh ./btree -n INPUT_NUM_KEYS -i INPUT_FILE -q
    # The `-q` turns of original cpu_pause() and hence lets Quartz
    # take care of all the latency emulation
```

Quartz is an epoch-based emulator, where the minimal epoch duration cannot be smaller than 1ms. Hence, I believe it is encouraged to test at large scale and feed in long input streams to amortize out this bit of inaccuracy.


## Original README

Implementation of the paper, "__Endurable Transient Inconsistency in Byte-Addressable Persistent B+-Tree__".

The paper is to appear in [FAST 2018](https://www.usenix.org/conference/fast18).

Failure-Atomic ShifT(FAST) and Failure-Atomic In-place Rebalancing(FAIR) are simple and novel algorithms that make B+-Tree tolerant againt system failures without expensive COW or logging for Non-Volatile Memory(NVM).
A B+-Tree with FAST and FAIR can achieve high performance comparing to the-state-of-the-art data structures for NVM.
Because the B+-Tree supports the sorted order of keys like a legacy B+-Tree, it is also beneficial for range queries.

In addition, a read query can detect a transient inconsistency in a B+-Tree node during a write query is modifying it.
It allows read threads to search keys without any lock. That is, the B+-Tree with FAST and FAIR increases throughputs of multi-threaded application with lock-free search.

We strongly recommend to refer to the paper for the details.

* Directories 
  * single - a single thread version without lock
  * concurrent - a multi-threaded version with std::mutex in C++11

* How to run (single)
    1. git clone https://github.com/DICL/FAST_FAIR.git
    2. cd FAST_FAIR/single
    3. make
    4. `./btree -n [the # of data] -w [write latency of NVM] -i [path]` (e.g. ./btree -n 10000 -w 300 -i ~/input.txt)

* How to run (concurrent)
    1. git clone https://github.com/DICL/FAST_FAIR.git
    2. cd FAST_FAIR/concurrent
    3. make
    4. There are two versions of concurrent test programs - One is only search and only insertion, the other is a mixed workload.
        1. `./btree_concurrent -n [the # of data] -w [write latency of NVM] -i [input path] -t [the # of threads]` (e.g. ./btree -n 10000 -w 300 -i ~/input.txt -t 16)
        2. `./btree_concurrent_mixed -n [the # of data] -w [write latency of NVM] -i [input path] -t [the # of threads]` (e.g. ./btree -n 10000 -w 300 -i ~/input.txt -t 16)
