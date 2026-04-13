# SPM Module 2: Parallel Partitioned Hash Join Pipeline

**Author:** Teja Karuku  
**Course:** Software Performance and Scalability (SPM)  

## Overview
This project implements a Partitioned Hash Join algorithm with duplicates. It includes two versions:
1. **Sequential Baseline (`hashjoin_seq.cpp`)**: A standard, single-threaded implementation used as the baseline for correctness and performance.
2. **Parallel Version (`hashjoin_par.cpp`)**: An optimized, multi-threaded implementation utilizing C++ `<thread>`, local structures to prevent false sharing, and a lock-free scatter phase using a 2D offset matrix.

## Prerequisites
To compile and run this project, ensure you are in a Linux/Unix environment (such as the `spmcluster`) with:
* `g++` compiler supporting C++11 or higher.
* CPU architecture supporting AVX2 instructions.
* `make` utility installed (optional, manual instructions provided).

---

## Transferring to the Cluster
If you are developing on your local machine and need to transfer the project files to the university `spmcluster` for compilation and benchmarking, you can use the `scp` (Secure Copy) command in your local terminal.

**To transfer the entire project folder:**

scp -r /path/to/your/local/folder username@spmcluster_address:~/destination_folder
(Replace username, spmcluster_address, and the paths with your actual cluster credentials).

To transfer only specific files (e.g., just the C++ files and Makefile):

scp hashjoin_seq.cpp hashjoin_par.cpp Makefile run_benchmarks.sh username@spmcluster_address:~/SPM_Module2/
Files Included
hashjoin_seq.cpp: Source code for the sequential version.

hashjoin_par.cpp: Source code for the parallel version.

Makefile: Automated build script to compile and execute the programs.

run_benchmarks.sh: Shell script used to run automated scaling tests.

SPM_Assignment_Report_module_02.pdf: Performance analysis and design choices.

Compilation
Option 1: Using Make (Recommended)
The project uses a Makefile for streamlined compilation. To compile both the sequential and parallel executables with the necessary optimization flags (-O3, -mavx2, -pthread), simply run:


make
To remove the compiled binaries and start fresh, run:

 
make clean
Option 2: Manual Compilation
If you prefer to compile the files manually using g++, use the following commands in your terminal:

Compile Sequential Version:

 
g++ -O3 -mavx2 -pthread hashjoin_seq.cpp -o hashjoin_seq
Compile Parallel Version:

 
g++ -O3 -mavx2 -pthread hashjoin_par.cpp -o hashjoin_par
Execution & Testing
Option 1: Using Make (Automated)
You can use the Makefile to automatically compile and run both versions sequentially with the default parameters:

 
make run
You can also override default parameters from the command line. For example, to run a smaller test with 32 threads:

 
make run NR=10000 NS=10000 T=32
Option 2: Manual Execution
Once compiled, you can run the executables directly from the terminal.

Run Sequential Version:

 
./hashjoin_seq -nr 50000000 -ns 50000000 -seed 12345 -max-key 1000000 -p 256
Run Parallel Version:
(Note the addition of the -t flag for the number of threads)

 
./hashjoin_par -nr 50000000 -ns 50000000 -seed 12345 -max-key 1000000 -p 256 -t 16
Command Line Arguments Breakdown:

-nr <size> : Size of Relation R (default: 50,000,000)

-ns <size> : Size of Relation S (default: 50,000,000)

-seed <val>: Seed for the random data generator (default: 12345)

-max-key <val>: Maximum key value generated (default: 1,000,000)

-p <count> : Number of partitions (default: 256)

-t <count> : Number of threads (Parallel version only, default: 16)

Correctness Check
To verify that the parallel implementation is correct, run both versions with the exact same dataset parameters (-nr, -ns, -seed, -max-key, -p) and compare the terminal output.

Correctness is guaranteed if and only if both implementations output the exact same values for:

Total Matches: The total number of valid joins found.

Checksums: The aggregate checksums used to verify data integrity across all partitions.

Automated Benchmarking
To perform full performance evaluations (strong and weak scalability), a benchmark script is provided. You can trigger it via the Makefile:

 
make bench
Or run it manually:

Note: Please ensure you are running these benchmarks on a dedicated compute node (e.g., node09) to prevent interference from other users and ensure accurate timing.
