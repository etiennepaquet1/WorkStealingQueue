#pragma once

/*
Copyright (c) 2025 Etienne Paquet

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <atomic>
#include <iostream>
#include <optional>
#include <memory>
#include <algorithm>
#include <cassert>
#include <new>
#include <type_traits>
#include <cstddef>


template<typename T>
class WorkStealingQueue {
	// static_assert(std::is_trivially_destructible_v<T>);

public:
	explicit WorkStealingQueue(size_t capacity = 1024, const std::allocator<T> &allocator = std::allocator<T>()):
		capacity_(capacity),
		mask_(capacity - 1),
		allocator_{allocator},
		tail_guard_{} {
		static_assert(alignof(WorkStealingQueue) == kCacheLineSize);
		static_assert(sizeof(WorkStealingQueue) >= 4 * kCacheLineSize);
		assert(reinterpret_cast<char *>(&bottom_) -
			reinterpret_cast<char *>(&top_) >=
			static_cast<std::ptrdiff_t>(kCacheLineSize));
		buffer_ = std::allocator_traits<std::allocator<T> >::allocate(allocator_, capacity_);
	}

	~WorkStealingQueue() noexcept (std::is_nothrow_destructible_v<T>) {
		const auto top = top_.load(std::memory_order_acquire);
		const auto bottom = bottom_.load(std::memory_order_acquire);
		for (auto i = top; i < bottom; ++i) {
			if constexpr (!std::is_trivially_destructible_v<T>)
				buffer_[i & mask_].~T();
		}
		std::allocator_traits<std::allocator<T> >::deallocate(allocator_, buffer_, capacity_);
	}

	// Delete copy and move constructors
	WorkStealingQueue(const WorkStealingQueue &) = delete;
	WorkStealingQueue &operator=(const WorkStealingQueue &) = delete;

	[[nodiscard]]
	size_t capacity() const noexcept { return capacity_; }

	[[nodiscard]]
	size_t size() const noexcept {
		const auto bottom = static_cast<long>(bottom_.load(std::memory_order_acquire));
		const auto top = static_cast<long>(top_.load(std::memory_order_acquire));
		return bottom >= top ? bottom - top : 0;
	}

	[[nodiscard]]
	bool empty() const noexcept { return size() == 0; }

	template<typename... Args>
	void emplace(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args &&...>) {
		while (!try_emplace(std::forward<Args>(args)...));
	}

	template<typename... Args>
	[[nodiscard]]
	bool try_emplace(Args &&... args) noexcept(
		std::is_nothrow_constructible_v<T, Args &&...>) {
		static_assert(std::is_constructible_v<T, Args &&...>,
		              "T must be constructible with Args&&...");
		const auto write_idx = bottom_.load(std::memory_order_relaxed);
		const auto top = top_.load(std::memory_order_acquire);

		// Add +1 to prevent from writing into newly stolen-from but not destructed slot
		// This reduces the effective capacity by 1 but prevents UB by concurrent access
		// The Chase–Lev invariants (empty ⇔ top ≥ bottom, single-item race on t == b-1) stay intact
		if (write_idx - top >= capacity_) {
			return false;
		}
		new(&buffer_[write_idx & mask_]) T(std::forward<Args>(args)...);
		bottom_.store(write_idx + 1, std::memory_order_release);
		return true;
	}

	[[nodiscard]]
	std::optional<T> pop() noexcept(std::is_nothrow_move_constructible_v<T> &&
	                                std::is_nothrow_destructible_v<T>) {
		static_assert(std::is_move_constructible_v<T>, "T must be move-constructible");
		static_assert(std::is_trivially_destructible_v<T>, "T must be trivially destructible");
		// Decrement bottom_ to prevent thieves from initiating a steal().
		const auto pop_idx = bottom_.load(std::memory_order_relaxed) - 1;
		bottom_.store(pop_idx, std::memory_order_release);
		auto top = top_.load(std::memory_order_acquire);
		if (pop_idx < top) {
			// Revert decrement of bottom_.
			bottom_.store(pop_idx + 1, std::memory_order_relaxed);
			return std::nullopt;
		} else if (pop_idx == top) {
			// Race against thief thread to increment top_. If owner wins race, return bottom_ to original position.
			if (top_.compare_exchange_strong(top,
			                                 top + 1,
			                                 std::memory_order_seq_cst,
			                                 std::memory_order_relaxed)) {
				bottom_.store(pop_idx + 1, std::memory_order_relaxed);
				return std::move(buffer_[pop_idx & mask_]);
			} else {
				bottom_.store(pop_idx + 1, std::memory_order_relaxed);
				return std::nullopt;
			}
		} else {
			auto out = std::move(buffer_[pop_idx & mask_]);
			return out;
		}
	}

	[[nodiscard]]
	std::optional<T> steal() noexcept(std::is_nothrow_move_constructible_v<T> &&
	                                  std::is_nothrow_destructible_v<T>) {
		static_assert(std::is_move_constructible_v<T>,
					  "T must be move-constructible");
		static_assert(std::is_destructible_v<T>,
					  "T must be destructible");

		auto steal_idx = top_.load(std::memory_order_acquire);
		const auto bottom = bottom_.load(std::memory_order_acquire);

		if (steal_idx >= bottom) {
			return std::nullopt;
		}
		auto out = std::move(buffer_[steal_idx & mask_]); // TODO: this can't be right

		if (top_.compare_exchange_strong(steal_idx, steal_idx + 1,
			std::memory_order_seq_cst,
			std::memory_order_relaxed)) {
			// Thief wins the race for top_.
			return out;
		} else {
			// Thief loses: cancel steal.
			return std::nullopt;
		}
	}

private:
#ifdef __cpp_lib_hardware_interference_size
	static constexpr size_t kCacheLineSize =
			std::hardware_destructive_interference_size;
#else
  static constexpr size_t kCacheLineSize = 64;
  static constexpr size_t mask_ = capacity - 1;
#endif

	const size_t capacity_;
	const size_t mask_;
	// Start buffer on new cache line to avoid false sharing with previous elements in memory
	std::allocator<T> allocator_ [[no_unique_address]];
	T *buffer_;

	// Isolate heavily accessed resources.
	alignas(kCacheLineSize) std::atomic<long long> top_{0};
	alignas(kCacheLineSize) long long top_cache_{0};
	alignas(kCacheLineSize) std::atomic<long long> bottom_{0};

	// Tail guard to ensure there isn't false sharing with next elements in memory.
	alignas(kCacheLineSize) std::byte tail_guard_[kCacheLineSize];
};
