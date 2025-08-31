#include "wsq.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <pthread.h>
#include <sched.h>

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

	const size_t queueSize = 1 << 20; // 1M capacity
	const int64_t iters = 10'000'000;

	std::cout << "WorkStealingQueue benchmarks:" << std::endl;

	// ---------------------------------------------------
	// 1. Single Producer, Single Consumer (SPSC)
	// ---------------------------------------------------
	{
		example_queue q;
		auto t = std::thread([&] {
			pinThread(cpu1);
			for (int i = 0; i < iters; ++i) {
				std::optional<int> val;
				while (!(val = q.steal())) {
					// spin until value available
				}
				if (*val != i) {
					throw std::runtime_error("mismatch in SPSC test");
				}
			}
		});

		pinThread(cpu2);
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
	// 2.
	// ---------------------------------------------------
	{
		using example_queue = WorkStealingQueue<int, (1 << 20)>;

		const int numThieves = 4;
		std::vector<std::thread> thieves;
		std::vector<example_queue> thieves_queues(numThieves);
		example_queue producer;

		std::atomic done{false};

		// start thieves
		for (int tId = 0; tId < numThieves; ++tId) {
			thieves.emplace_back([&, tId] {
				pinThread((cpu1 >= 0) ? cpu1 + tId : -1);
				auto &localQ = thieves_queues[tId];
				while (!done.load(std::memory_order_relaxed)){
					if (auto opt = producer.steal()) {
						// exercise local queue push+pop to avoid shared contention
						localQ.emplace(*opt);
						(void) localQ.pop();
					}
				}
			});
		}
		pinThread(cpu2);
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
	// 3. Round-Trip Latency Test (safe version)
	// ---------------------------------------------------
	{
		example_queue q1, q2;
		std::atomic<bool> done{false};

		auto t = std::thread([&] {
			pinThread(cpu1);
			for (int i = 0; i < iters; ++i) {
				std::optional<int> val;
				// spin until we either get a value or main sets done
				while (!(val = q1.steal()) && !done.load(std::memory_order_relaxed)) {}
				q2.emplace(*val);
				std:: cout << "q2 emplace" << std::endl;
			}
		});

		pinThread(cpu2);
		auto start = std::chrono::steady_clock::now();
		for (int i = 0; i < iters; ++i) {
			// enqueue request
			q1.emplace(i);
			std:: cout << "q1 emplace" << std::endl;


			// wait for reply
			while (!q2.pop()) {}
			std:: cout << "q2 pop" << std::endl;

		}
		done.store(true, std::memory_order_relaxed);
		t.join();
		auto stop = std::chrono::steady_clock::now();

		std::cout << "Round-trip latency: "
				  << std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count() / iters
				  << " ns RTT" << std::endl;
	}

	return 0;
}
