# Compiler and Flags                                                                                                                                                                                           
CXX = g++
CXXFLAGS = -O3 -mavx2 -pthread

# Targets                                                                                                                                                                                                      
TARGETS = hashjoin_seq hashjoin_par

# Default Run Arguments (You can override these from the command line)                                                                                                                                         
NR ?= 50000000
NS ?= 50000000
SEED ?= 12345
MAX_KEY ?= 1000000
P ?= 256
T ?= 16

# Default target: compile both                                                                                                                                                                                 
all: $(TARGETS)

# Compilation rules                                                                                                                                                                                            
hashjoin_seq: hashjoin_seq.cpp
        $(CXX) $(CXXFLAGS) hashjoin_seq.cpp -o hashjoin_seq

hashjoin_par: hashjoin_par.cpp
        $(CXX) $(CXXFLAGS) hashjoin_par.cpp -o hashjoin_par

# Compile and run both programs sequentially with the arguments                                                                                                                                                
run: all
        @echo "--- Running Sequential Version ---"
        ./hashjoin_seq -nr $(NR) -ns $(NS) -seed $(SEED) -max-key $(MAX_KEY) -p $(P)
        @echo "\n--- Running Parallel Version ---"
        ./hashjoin_par -nr $(NR) -ns $(NS) -seed $(SEED) -max-key $(MAX_KEY) -p $(P) -t $(T)

# Run the benchmark script                                                                                                                                                                                     
bench: all
        chmod +x run_benchmarks.sh
        ./run_benchmarks.sh

# Clean up compiled binaries                                                                                                                                                                                   
clean:
        rm -f $(TARGETS)

.PHONY: all run bench clean