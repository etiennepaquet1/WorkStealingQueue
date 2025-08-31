#include "wsq.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <pthread.h>
#include <sched.h>

//TODO:

void pinThread(int cpu) {
	if (cpu < 0) {
		return;
	}
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu, &cpuset);
	if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == -1) {
		perror("pthread_setaffinity_np");
		exit(1);
	}
}

void printCurrentCPU(const char* tag) {
	int cpu = sched_getcpu();
	std::cout << tag << " running on CPU " << cpu << std::endl;
}

namespace {
	using example_queue = WorkStealingQueue<int, (1 << 20)>;
}

int main(int argc, char *argv[]) {
	(void) argc, (void) argv;

	int cpu1 = -1;
	int cpu2 = -1;

	if (argc == 3) {
		cpu1 = std::stoi(argv[1]);
		cpu2 = std::stoi(argv[2]);
	}

	const int64_t iters = 10'000'000;

	std::cout << "WorkStealingQueue Benchmarks:" << std::endl;

	for (int j = 0; j < 1; ++j)
	{
		// ---------------------------------------------------
		// 1. Single Producer, Single Consumer (SPSC)
		// ---------------------------------------------------
		{
			example_queue q;
			auto t = std::thread([&] {
				pinThread(cpu1);
				// printCurrentCPU("Consumer thread");
				for (int i = 0; i < iters; ++i) {
					std::optional<int> val;
					while (!(val = q.steal())) {
						// spin until value available
					}
					if (*val != i) {
						std::cerr << "Mismatch detected: val=" << *val << " i=" << i << std::endl;
						throw std::runtime_error("mismatch in SPSC test");
					}
				}
			});

			pinThread(cpu2);
			// printCurrentCPU("Producer thread");
			auto start = std::chrono::steady_clock::now();
			for (int i = 0; i < iters; ++i) {
				q.emplace(i);
			}
			t.join();
			auto stop = std::chrono::steady_clock::now();
			std::cout << "SPSC throughput: "
					<< iters * 1000000 /
					std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count()
					<< " ops/ms" << std::endl;
		}

		// ---------------------------------------------------
		// 2. Single producer, multiple consumers
		// ---------------------------------------------------
		{
			const int numThieves = 2;
			std::vector<std::thread> thieves;
			std::vector<example_queue> thieves_queues(numThieves);
			example_queue producer;

			std::atomic done{false};

			// start thieves
			for (int tId = 0; tId < numThieves; ++tId) {
				thieves.emplace_back([&, tId] {
					pinThread((cpu1 >= 0) ? cpu1 + tId : -1);
					// printCurrentCPU("Consumer thread");

					auto &localQ = thieves_queues[tId];
					while (!done.load(std::memory_order_relaxed)) {
						if (auto opt = producer.steal()) {
							// exercise local queue push+pop to avoid shared contention
							localQ.emplace(*opt);
							(void) localQ.pop();
						}
					}
				});
			}
			pinThread(cpu2);
			// printCurrentCPU("Producer thread");

			auto start = std::chrono::steady_clock::now();
			for (int i = 0; i < iters; ++i) {
				producer.emplace(i);
			}
			done.store(true, std::memory_order_relaxed);
			for (auto &t: thieves) t.join();
			auto stop = std::chrono::steady_clock::now();

			std::cout << "SPMT throughput (" << numThieves << " thieves, per-thief queues): "
					<< iters * 1000000 /
					std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count()
					<< " ops/ms" << std::endl;
		}

		// ---------------------------------------------------
		// 3. Round-Trip Latency Test
		// ---------------------------------------------------
		{
			example_queue q1, q2;

			auto t = std::thread([&] {
				pinThread(cpu1);
				// printCurrentCPU("Consumer thread");
				for (int i = 0; i < iters; ++i) {
					std::optional<int> val;
					while (!(val = q1.steal())) {
					}
					q2.emplace(*val);
				}
			});

			pinThread(cpu2);
			// printCurrentCPU("Producer thread");

			std::vector<std::chrono::nanoseconds> latencies;
			latencies.reserve(iters);

			for (int i = 0; i < iters; ++i) {
				auto t0 = std::chrono::steady_clock::now();
				q1.emplace(i);

				while (!q2.steal()) {
				} // wait for reply

				auto t1 = std::chrono::steady_clock::now();
				latencies.push_back(t1 - t0);
			}

			std::ranges::sort(latencies);
			// compute average, p50, p95, p99 latencies here
			auto total = std::accumulate(latencies.begin(), latencies.end(),
										 std::chrono::nanoseconds{0});
			auto average = total / latencies.size();
			auto p50 = latencies[latencies.size() / 2];
			auto p95 = latencies[latencies.size() * 95 / 100];
			auto p99 = latencies[latencies.size() * 99 / 100];
			auto p99_9 = latencies[latencies.size() * 999 / 1000];
			auto p99_99 = latencies[latencies.size() * 9999 / 10000];

			t.join();

			std::cout << "Round-trip latency: \n";
			std::cout << "    average: " << average << "\n";
			std::cout << "    p50: " << p50 << "\n";
			std::cout << "    p95: " << p95 << "\n";
			std::cout << "    p99: " << p99 << std::endl;
			std::cout << "    p99.9: " << p99_9 << std::endl;
			std::cout << "    p99.99: " << p99_99 << std::endl;
		}
	}
	return 0;
}
