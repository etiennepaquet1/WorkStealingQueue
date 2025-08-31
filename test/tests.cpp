// Adapted from https://github.com/ssbl/concurrent-deque and https://github.com/taskflow/work-stealing-queue

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>
#include "wsq.h"
#include <thread>

#include <atomic>
#include <cassert>
#include <vector>
#include <array>
#include <deque>
#include <set>


namespace {
    using example_wsq = WorkStealingQueue<int, (1 << 12)>;
}

TEST_CASE("Examples") {
    // Work-stealing deque of ints
    example_wsq deque;

    // One thread can push and pop items from one end (like a stack)
    std::thread owner([&]() {
        for (int i = 0; i < deque.capacity(); i = i + 1) {
            deque.emplace(i);
        }
        while (!deque.empty()) {
            [[maybe_unused]] std::optional item = deque.pop();
        }
    });

    // While multiple (any) threads can steal items from the other end
    std::thread thief([&]() {
        while (!deque.empty()) {
            [[maybe_unused]] std::optional item = deque.steal();
        }
    });

    owner.join();
    thief.join();

    REQUIRE(deque.empty());
}


TEST_CASE("basic operations, [wsq]") {
    auto deque = example_wsq();
    auto& worker  = deque;
    auto& stealer = deque;

    // Empty deque.
    REQUIRE(!worker.pop());

    // Single emplace, pop.
    deque.emplace(100);
    auto v = worker.pop();
    REQUIRE(v.has_value());
    REQUIRE(*v == 100);

    // Steal when empty.
    REQUIRE(!stealer.steal());

    // Single emplace, steal.
    worker.emplace(100);
    auto s = stealer.steal();
    REQUIRE(s.has_value());
    REQUIRE(*s == 100);
}


TEST_CASE("multiple steals on deque of length 1, [wsq]") {
    auto deque = example_wsq();
    auto& worker  = deque;
    auto& stealer = deque;

    deque.emplace(100);
    const int nthreads = 4;
    std::atomic<int> seen{0};
    std::vector<std::thread> stealers;
    stealers.reserve(nthreads);

    for (int i = 0; i < nthreads; ++i) {
        stealers.emplace_back([&stealer, &seen]() {
            auto x = stealer.steal();
            if (x) seen.fetch_add(1, std::memory_order_relaxed);
        });
    }
    for (auto& t : stealers) t.join();

    REQUIRE(seen.load() == 1);
}

TEST_CASE("emplace against steals, [wsq]") {
    auto deque = example_wsq();
    auto& worker  = deque;
    auto& stealer = deque;

    const int max_items = 100000;
    const int nthreads  = 4;

    std::atomic<int> remaining{max_items};
    std::vector<std::thread> threads;
    threads.reserve(nthreads);

    for (int i = 0; i < nthreads; ++i) {
        threads.emplace_back([&stealer, &remaining]() {
            while (remaining.load(std::memory_order_seq_cst) > 0) {
                auto x = stealer.steal();
                if (x) {
                    assert(*x == 1);
                    remaining.fetch_sub(1, std::memory_order_seq_cst);
                }
            }
        });
    }

    for (int i = 0; i < max_items; ++i)
        worker.emplace(1);

    for (auto& t : threads) t.join();
    REQUIRE(remaining.load() == 0);
}

TEST_CASE("pop and steal, [wsq]") {
    auto deque = example_wsq();
    auto& worker  = deque;

    const auto max_items = deque.capacity();
    const int nthreads  = 4;

    for (int i = 0; i < max_items; ++i)
        worker.emplace(1);

    std::atomic remaining{max_items};
    std::vector<std::thread> threads;
    threads.reserve(nthreads);

    std::array<example_wsq, nthreads> stealers;

    for (int i = 0; i < nthreads; ++i) {
        threads.emplace_back([&stealers, &remaining, i]() {
            while (remaining.load(std::memory_order_seq_cst) > 0) {
                auto x = stealers.at(i).steal();
                if (x) {
                    assert(x.value() == 1);
                    remaining.fetch_sub(1, std::memory_order_seq_cst);
                }
            }
        });
    }

    while (remaining.load(std::memory_order_seq_cst) > 0) {
        auto x = worker.pop();
        if (x) {
            assert(x.value() == 1);
            remaining.fetch_sub(1, std::memory_order_seq_cst);
        }
    }

    for (auto& t : threads) t.join();
    REQUIRE(remaining.load() == 0);
}


// Procedure: wsq_test_owner
void wsq_test_owner() {
    constexpr int64_t cap = 1 << 16;

    WorkStealingQueue<int, cap> queue;
    std::deque<int> gold;

    REQUIRE(queue.capacity() == cap);
    REQUIRE(queue.empty());

    for (int i = 2; i <= (1 << 16); i <<= 1) {
        REQUIRE(queue.empty());

        for (int j = 0; j < i; ++j) {
            queue.emplace(j);
        }

        for (int j = 0; j < i; ++j) {
            auto item = queue.pop();
            REQUIRE((item && *item == i - j - 1));
        }
        REQUIRE(!queue.pop());

        REQUIRE(queue.empty());
        for (int j = 0; j < i; ++j) {
            queue.emplace(j);
        }

        for (int j = 0; j < i; ++j) {
            auto item = queue.steal();
            REQUIRE((item && *item == j));
        }
        REQUIRE(!queue.pop());

        REQUIRE(queue.empty());

        for (int j = 0; j < i; ++j) {
            // enqueue
            if (auto dice = ::rand() % 3; dice == 0) {
                queue.emplace(j);
                gold.push_back(j);
            }
            // pop back
            else if (dice == 1) {
                auto item = queue.pop();
                if (gold.empty()) {
                    REQUIRE(!item);
                } else {
                    REQUIRE(*item == gold.back());
                    gold.pop_back();
                }
            }
            // pop front
            else {
                auto item = queue.steal();
                if (gold.empty()) {
                    REQUIRE(!item);
                } else {
                    REQUIRE(*item == gold.front());
                    gold.pop_front();
                }
            }

            REQUIRE(queue.size() == (int)gold.size());
        }

        while (!queue.empty()) {
            auto item = queue.pop();
            REQUIRE((item && *item == gold.back()));
            gold.pop_back();
        }

        REQUIRE(gold.empty());
    }
}

// Procedure: wsq_test_n_thieves
void wsq_test_n_thieves(int N) {
    constexpr int64_t cap = 1 << 16;

    WorkStealingQueue<int, cap> queue;

    REQUIRE(queue.capacity() == cap);
    REQUIRE(queue.empty());

    for (int i = 2; i <= (1 << 16); i <<= 1) {
        REQUIRE(queue.empty());

        int p = 0;

        std::vector<std::deque<int>> cdeqs(N);
        std::vector<std::thread> consumers;
        std::deque<int> pdeq;

        auto num_stolen = [&]() {
            int total = 0;
            for (const auto& cdeq : cdeqs) {
                total += static_cast<int>(cdeq.size());
            }
            return total;
        };

        for (int n = 0; n < N; n++) {
            consumers.emplace_back([&, n]() {
                while (num_stolen() + (int)pdeq.size() != i) {
                    if (auto dice = ::rand() % 4; dice == 0) {
                        if (auto item = queue.steal(); item) {
                            cdeqs[n].push_back(*item);
                        }
                    }
                }
            });
        }

        std::thread producer([&]() {
            while (p < i) {
                if (auto dice = ::rand() % 4; dice == 0) {
                    queue.emplace(p++);
                } else if (dice == 1) {
                    if (auto item = queue.pop(); item) {
                        pdeq.push_back(*item);
                    }
                }
            }
        });

        producer.join();

        for (auto& c : consumers) {
            c.join();
        }

        REQUIRE(queue.empty());

        std::set<int> set;

        for (const auto& cdeq : cdeqs) {
            for (auto k : cdeq) {
                set.insert(k);
            }
        }

        for (auto k : pdeq) {
            set.insert(k);
        }

        for (int j = 0; j < i; ++j) {
            REQUIRE(set.find(j) != set.end());
        }

        REQUIRE((int)set.size() == i);
    }
}

// ----------------------------------------------------------------------------
// Testcase: WSQTest.Owner
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.Owner" * doctest::timeout(300)) { wsq_test_owner(); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.1Thief
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.1Thief" * doctest::timeout(300)) { wsq_test_n_thieves(1); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.2Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.2Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(2); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.3Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.3Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(3); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.4Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.4Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(4); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.5Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.5Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(5); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.6Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.6Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(6); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.7Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.7Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(7); }

// ----------------------------------------------------------------------------
// Testcase: WSQTest.8Thieves
// ----------------------------------------------------------------------------
TEST_CASE("WSQ.8Thieves" * doctest::timeout(300)) { wsq_test_n_thieves(8); }