// hashjoin_par.cpp
//
// Parallel C++ Thread implementation for Module 2
// Partitioned Hash Join with Duplicates

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <algorithm>

// ------------------------------------------------------------
// Record definition
// ------------------------------------------------------------
struct Record {
    std::uint64_t key{};
};

// ------------------------------------------------------------
// Utility: command-line parsing
// ------------------------------------------------------------
static bool read_arg_u64(int argc, char** argv, const std::string& name, std::uint64_t& out) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (name == argv[i]) {
            out = std::strtoull(argv[i + 1], nullptr, 10);
            return true;
        }
    }
    return false;
}

static void usage(const char* prog) {
    std::cerr
        << "Usage:\n"
        << "  " << prog << " -nr NR -ns NS -seed SEED -max-key K -p P -t T\n\n"
        << "Parameters:\n"
        << "  -nr         Number of records in relation R\n"
        << "  -ns         Number of records in relation S\n"
        << "  -seed       Deterministic seed\n"
        << "  -max-key    Keys are generated in [0, max-key)\n"
        << "  -p          Number of partitions (power of two required)\n"
        << "  -t          Number of C++ threads to spawn\n";
}

static bool is_power_of_two(std::uint32_t x) {
    return x != 0 && (x & (x - 1U)) == 0;
}

// ------------------------------------------------------------
// Deterministic pseudo-random generation
// ------------------------------------------------------------
static inline std::uint64_t splitmix64_mix(std::uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}
static inline std::uint64_t splitmix64(std::uint64_t x) {
    return splitmix64_mix(x + 0x9e3779b97f4a7c15ULL);
}
static inline std::uint64_t splitmix64_next(std::uint64_t& state) {
    state += 0x9e3779b97f4a7c15ULL;
    return splitmix64_mix(state);
}

static std::vector<Record> generate_relation(std::size_t n, std::uint64_t seed, std::uint64_t max_key) {
    std::vector<Record> out(n);
    std::uint64_t state = seed;
    for (std::size_t i = 0; i < n; ++i) {
        const std::uint64_t r = splitmix64_next(state);
        out[i].key = (max_key == 0) ? 0ULL : (r % max_key);
    }
    return out;
}

// ------------------------------------------------------------
// Module 1: Partition mapping 
// ------------------------------------------------------------
static inline std::uint32_t compute_partition_id(std::uint64_t key, std::uint32_t p) {
    return static_cast<std::uint32_t>(key & static_cast<std::uint64_t>(p - 1U));
}

// ------------------------------------------------------------
// Phase Timers Structure & Metadata Structs
// ------------------------------------------------------------
struct TimingStats {
    double hist_time = 0.0;
    double prefix_time = 0.0;
    double scatter_time = 0.0;
    double join_time = 0.0;
};

struct PartitionedRelation {
    std::vector<Record> data;
    std::vector<std::size_t> begin;
    std::vector<std::size_t> end;
};

struct JoinResult {
    std::uint64_t join_count = 0;
    std::uint64_t checksum1 = 0;
    std::uint64_t checksum2 = 0;
};

// ------------------------------------------------------------
// Local join on one partition
// ------------------------------------------------------------
static JoinResult join_one_partition(const PartitionedRelation& Rpart,
                                     const PartitionedRelation& Spart,
                                     std::uint32_t pid) {
    JoinResult result{};

    const std::size_t r_begin = Rpart.begin[pid];
    const std::size_t r_end = Rpart.end[pid];
    const std::size_t s_begin = Spart.begin[pid];
    const std::size_t s_end = Spart.end[pid];

    if (r_begin == r_end || s_begin == s_end) return result;

    std::unordered_map<std::uint64_t, std::uint32_t> countR;
    countR.reserve((r_end - r_begin) * 2);

    for (std::size_t i = r_begin; i < r_end; ++i) {
        ++countR[Rpart.data[i].key];
    }

    for (std::size_t i = s_begin; i < s_end; ++i) {
        const std::uint64_t key = Spart.data[i].key;
        const auto it = countR.find(key);
        if (it != countR.end()) {
            const std::uint64_t multiplicity = it->second;
            result.join_count += multiplicity;
            result.checksum1 += splitmix64(key) * multiplicity;
            result.checksum2 += splitmix64(key ^ 0x9e3779b97f4a7c15ULL) * multiplicity;
        }
    }
    return result;
}

// ------------------------------------------------------------
// Full Parallel Partitioned Hash Join
// ------------------------------------------------------------
static JoinResult partitioned_hash_join_parallel(const std::vector<Record>& R,
                                                 const std::vector<Record>& S,
                                                 std::uint32_t p,
                                                 std::uint32_t num_threads,
                                                 TimingStats& stats) {
    const std::size_t NR = R.size();
    const std::size_t NS = S.size();
    
    // Arrays to hold the threads
    std::vector<std::thread> threads;
    
    // =========================================================================
    // PHASE 1: Parallel Histograms
    // =========================================================================
    auto t0 = std::chrono::steady_clock::now();
    
    // hist_R[thread_id][partition_id]
    std::vector<std::vector<std::size_t>> hist_R(num_threads, std::vector<std::size_t>(p, 0));
    std::vector<std::vector<std::size_t>> hist_S(num_threads, std::vector<std::size_t>(p, 0));
    
    auto hist_worker = [&](std::uint32_t tid) {
        // Compute chunk boundaries for relation R
        std::size_t chunk_R = (NR + num_threads - 1) / num_threads;
        std::size_t start_R = std::min(tid * chunk_R, NR);
        std::size_t end_R   = std::min(start_R + chunk_R, NR);
        
        for (std::size_t i = start_R; i < end_R; ++i) {
            hist_R[tid][compute_partition_id(R[i].key, p)]++;
        }
        
        // Compute chunk boundaries for relation S
        std::size_t chunk_S = (NS + num_threads - 1) / num_threads;
        std::size_t start_S = std::min(tid * chunk_S, NS);
        std::size_t end_S   = std::min(start_S + chunk_S, NS);
        
        for (std::size_t i = start_S; i < end_S; ++i) {
            hist_S[tid][compute_partition_id(S[i].key, p)]++;
        }
    };
    
    for (std::uint32_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(hist_worker, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();
    
    auto t1 = std::chrono::steady_clock::now();
    stats.hist_time += std::chrono::duration<double>(t1 - t0).count();

    // =========================================================================
    // PHASE 2: Global Prefix Sum (Sequential)
    // =========================================================================
    // Extremely fast mathematics handled entirely by the main thread.
    std::vector<std::size_t> begin_R(p, 0), end_R(p, 0);
    std::vector<std::size_t> begin_S(p, 0), end_S(p, 0);
    
    std::vector<std::vector<std::size_t>> write_offset_R(num_threads, std::vector<std::size_t>(p, 0));
    std::vector<std::vector<std::size_t>> write_offset_S(num_threads, std::vector<std::size_t>(p, 0));
    
    std::size_t running_R = 0;
    std::size_t running_S = 0;
    
    for (std::uint32_t pid = 0; pid < p; ++pid) {
        begin_R[pid] = running_R;
        begin_S[pid] = running_S;
        
        for (std::uint32_t t = 0; t < num_threads; ++t) {
            write_offset_R[t][pid] = running_R;
            running_R += hist_R[t][pid];
            
            write_offset_S[t][pid] = running_S;
            running_S += hist_S[t][pid];
        }
        
        end_R[pid] = running_R;
        end_S[pid] = running_S;
    }
    
    auto t2 = std::chrono::steady_clock::now();
    stats.prefix_time += std::chrono::duration<double>(t2 - t1).count();

    // =========================================================================
    // PHASE 3: Parallel Scatter (Lock-Free)
    // =========================================================================
    PartitionedRelation Rpart{std::vector<Record>(NR), begin_R, end_R};
    PartitionedRelation Spart{std::vector<Record>(NS), begin_S, end_S};
    
    auto scatter_worker = [&](std::uint32_t tid) {
        // Scatter R
        std::size_t chunk_R = (NR + num_threads - 1) / num_threads;
        std::size_t start_R = std::min(tid * chunk_R, NR);
        std::size_t end_R   = std::min(start_R + chunk_R, NR);
        
        auto next_R = write_offset_R[tid]; // Local copy for speed
        for (std::size_t i = start_R; i < end_R; ++i) {
            std::uint32_t pid = compute_partition_id(R[i].key, p);
            Rpart.data[next_R[pid]++] = R[i];
        }
        
        // Scatter S
        std::size_t chunk_S = (NS + num_threads - 1) / num_threads;
        std::size_t start_S = std::min(tid * chunk_S, NS);
        std::size_t end_S   = std::min(start_S + chunk_S, NS);
        
        auto next_S = write_offset_S[tid]; // Local copy for speed
        for (std::size_t i = start_S; i < end_S; ++i) {
            std::uint32_t pid = compute_partition_id(S[i].key, p);
            Spart.data[next_S[pid]++] = S[i];
        }
    };
    
    for (std::uint32_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(scatter_worker, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();
    
    auto t3 = std::chrono::steady_clock::now();
    stats.scatter_time += std::chrono::duration<double>(t3 - t2).count();

    // =========================================================================
    // PHASE 4: Parallel Join Processing
    // =========================================================================
    std::vector<JoinResult> local_results(num_threads);
    
    auto join_worker = [&](std::uint32_t tid) {
        // Divide the P partitions among the threads
        std::size_t chunk_p = (p + num_threads - 1) / num_threads;
        std::size_t start_p = std::min(tid * chunk_p, (std::size_t)p);
        std::size_t end_p   = std::min(start_p + chunk_p, (std::size_t)p);
        
        JoinResult res{};
        for (std::size_t pid = start_p; pid < end_p; ++pid) {
            JoinResult p_res = join_one_partition(Rpart, Spart, pid);
            res.join_count += p_res.join_count;
            res.checksum1 += p_res.checksum1;
            res.checksum2 += p_res.checksum2;
        }
        local_results[tid] = res; // Store locally to avoid false sharing
    };
    
    for (std::uint32_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(join_worker, t);
    }
    for (auto& th : threads) th.join();
    
    // Final aggregation by the main thread
    JoinResult total{};
    for (const auto& res : local_results) {
        total.join_count += res.join_count;
        total.checksum1 += res.checksum1;
        total.checksum2 += res.checksum2;
    }
    
    auto t4 = std::chrono::steady_clock::now();
    stats.join_time += std::chrono::duration<double>(t4 - t3).count();

    return total;
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
int main(int argc, char** argv) {
    std::uint64_t nr = 0, ns = 0, seed = 0, max_key = 0, p = 0, t = 1;

    if (!read_arg_u64(argc, argv, "-nr", nr) ||
        !read_arg_u64(argc, argv, "-ns", ns) ||
        !read_arg_u64(argc, argv, "-seed", seed) ||
        !read_arg_u64(argc, argv, "-max-key", max_key) ||
        !read_arg_u64(argc, argv, "-p", p)) {
        usage(argv[0]);
        return 1;
    }
    
    // Optional thread parameter (defaults to 1 if not provided)
    read_arg_u64(argc, argv, "-t", t);

    const std::uint32_t P = static_cast<std::uint32_t>(p);
    const std::uint32_t T = static_cast<std::uint32_t>(t);
    const std::size_t NR = static_cast<std::size_t>(nr);
    const std::size_t NS = static_cast<std::size_t>(ns);

    const auto R = generate_relation(NR, seed, max_key);
    const auto S = generate_relation(NS, seed ^ 0xdeadebdecdeedef1ULL, max_key);

    TimingStats stats;

    const auto t0 = std::chrono::steady_clock::now();
    const JoinResult result = partitioned_hash_join_parallel(R, S, P, T, stats);
    const auto t1 = std::chrono::steady_clock::now();

    const double sec = std::chrono::duration<double>(t1 - t0).count();

    std::cout << "\n=== Parallel C++ Threads Benchmark ===" << "\n";
    std::cout << "NR=" << NR << " NS=" << NS << " P=" << P << " Threads=" << T
              << " seed=" << seed << " [0, " << max_key << ")\n";

    std::cout << "join_count=" << result.join_count << "\n";
    std::cout << "checksum1=" << result.checksum1 << "\n";
    std::cout << "checksum2=" << result.checksum2 << "\n";

    std::cout << std::fixed << std::setprecision(6);
    std::cout << "\n--- Execution Time Breakdown ---\n";
    std::cout << "Histogram time : " << stats.hist_time << " sec\n";
    std::cout << "Prefix sum time: " << stats.prefix_time << " sec\n";
    std::cout << "Scatter time   : " << stats.scatter_time << " sec\n";
    std::cout << "Join processing: " << stats.join_time << " sec\n";
    std::cout << "--------------------------------\n";
    std::cout << "Total time_sec = " << sec << "\n\n";

    return 0;
}
