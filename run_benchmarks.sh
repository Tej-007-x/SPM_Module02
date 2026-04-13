#!/bin/bash

# Ensure the executable is up to date
g++ -O3 -mavx2 -pthread hashjoin_par.cpp -o hashjoin_par

echo "======================================================"
echo "            STRONG SCALABILITY TESTING                "
echo "======================================================"
echo "Fixed Problem Size: NR=50,000,000, NS=50,000,000"
echo "Threads | Total Time (s) | Speedup"
echo "------------------------------------------------------"

# Run T=1 to get the base time for speedup math
BASE_TIME=$(./hashjoin_par -nr 50000000 -ns 50000000 -seed 12345 -max-key 1000000 -p 256 -t 1 | grep "Total time_sec" | awk '{print $4}')
echo "   1    |   $BASE_TIME     |  1.00x"

# Loop through remaining thread counts
for t in 2 4 8 16 32; do
    TIME=$(./hashjoin_par -nr 50000000 -ns 50000000 -seed 12345 -max-key 1000000 -p 256 -t $t | grep "Total time_sec" | awk '{print $4}')
    # Calculate speedup using 'bc' (basic calculator)
    SPEEDUP=$(echo "scale=2; $BASE_TIME / $TIME" | bc)
    printf "  %2d    |   %-10s   |  %s\n" "$t" "$TIME" "${SPEEDUP}x"
done

echo ""
echo "======================================================"
echo "             WEAK SCALABILITY TESTING                 "
echo "======================================================"
echo "Scaling Problem Size: NR and NS = Threads * 5,000,000"
echo "Threads |   Dataset Size (N)   | Total Time (s)"
echo "------------------------------------------------------"

# Scale the dataset linearly with the number of threads
for t in 1 2 4 8 16 32; do
    N=$((t * 5000000))
    TIME=$(./hashjoin_par -nr $N -ns $N -seed 12345 -max-key 1000000 -p 256 -t $t | grep "Total time_sec" | awk '{print $4}')
    printf "  %2d    |      %-10d      |   %s\n" "$t" "$N" "$TIME"
done

echo "======================================================"
echo "Benchmarks Complete!"
