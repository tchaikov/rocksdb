// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <functional>
#include <limits>
#include <seastar/core/cacheline.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/prefetch.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/thread.hh>

#define ROCKSDB_PRIszt "zu"
#define ROCKSDB_NOEXCEPT noexcept

static constexpr size_t CACHE_LINE_SIZE = seastar::cache_line_size;

namespace rocksdb {

extern const bool kDefaultToAdaptiveMutex;

namespace port {

static constexpr bool kLittleEndian = (__ORDER_LITTLE_ENDIAN__ == __BYTE_ORDER__);
const uint32_t kMaxUint32 = std::numeric_limits<uint32_t>::max();
const int kMaxInt32 = std::numeric_limits<int32_t>::max();
const int kMinInt32 = std::numeric_limits<int32_t>::min();
const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();
const int64_t kMinInt64 = std::numeric_limits<int64_t>::min();
const size_t kMaxSizet = std::numeric_limits<size_t>::max();

class Mutex {
 public:
  explicit Mutex(bool adaptive = kDefaultToAdaptiveMutex) {}
  ~Mutex() = default;

  void Lock();
  void Unlock();
  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();

#ifndef NDEBUG
  bool locked_ = false;
#endif

private:
  friend class CondVar;
  seastar::shared_mutex mutex_;

  // No copying
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;
};

class RWMutex {
 public:
  RWMutex();
  ~RWMutex();

  void ReadLock();
  void ReadUnlock();
  void WriteLock();
  void WriteUnlock();
  void AssertHeld() { }

 private:
  seastar::shared_mutex mutex_;

  RWMutex(const RWMutex&) = delete;
  void operator=(const RWMutex&) = delete;
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);
  void Signal();
  void SignalAll();
 private:
  Mutex* mu_;
  seastar::condition_variable cond_;
};

class Thread {
public:
  Thread() noexcept = default;
  template<class Function, class... Args>
  explicit Thread(Function&& f, Args&&... args)
    : thread_{std::bind(std::forward<Function>(f),
			std::forward<Args>(args)...)}

  {}
  Thread(const Thread&) = delete;
  Thread& operator=(const Thread&) = delete;
  Thread(Thread&&) noexcept = default;
  Thread& operator=(Thread&& other) noexcept {
    thread_ = std::move(other.thread_);
    return *this;
  }
  bool joinable() const noexcept;
  void join();
  bool detach();
  using native_handle_type = std::thread::native_handle_type;
  native_handle_type native_handle();
private:
  seastar::thread thread_;
};

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("wfe");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

// Returns -1 if not available on this platform
int PhysicalCoreID();

using OnceType = std::once_flag;
extern void InitOnce(OnceType* once, void (*initializer)());

void *cacheline_aligned_alloc(size_t size);

void cacheline_aligned_free(void *memblock);

#define ALIGN_AS(n) alignas(n)

#define PREFETCH(addr, rw, locality) \
  seastar::prefetcher<0, rw, locality>(reinterpret_cast<uintptr_t>(addr))

extern void Crash(const std::string& srcfile, int srcline);

int GetMaxOpenFiles();

} // namespace port
} // namespace rocksdb
