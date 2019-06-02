# BzTree
An open-source BzTree implementation 

https://dl.acm.org/citation.cfm?id=3164147

## Build

### USE PMDK

```bash
mkdir build & cd build
cmake -DPMEM_BACKEND=PMDK ..
```

### Volatile only

```bash
mkdir build & cd build
cmake -DPMEM_BACKEND=VOLATILE ..
```

#### Other build options
`-DPMEM_BACKEND=EMU` to emulate persistent memory using DRAM

`-DGOOGLE_FRAMEWORK=0` if you're not comfortable with google frameworks (gtest/glog/gflags)

`-DBUILD_TESTS=0` to build shared library only (without tests)

`-DMAX_FREEZE_RETRY=n` to set max freeze retry times, default to 1, check the original paper for details

`-DENABLE_MERGE=1` to enable merge after delete, this is disabled by default, check the original paper for details.

## Benchmark on PiBench

Checkout PiBench here: https://github.com/wangtzh/pibench

### Build/Create BzTree shared lib

```bash
mkdir Releasee & cd Release
cmake -DCMAKE_BUILD_TYPE=Release -DPMEM_BACKEND=${BACKEND} -DGOOGLE_FRAMEWORK=0 -DBUILD_TESTS=0
```

### Benchmark Results

See our paper: 
