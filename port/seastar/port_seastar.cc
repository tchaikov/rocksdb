// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "port/seastar/port_seastar.h"

#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <iostream>

namespace rocksdb {

extern const bool kDefaultToAdaptiveMutex = false;

namespace port {

void Mutex::Lock() {
  mutex_.lock().get();
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  mutex_.unlock();
}

void Mutex::AssertHeld() {
#ifndef NDEBUG
  assert(locked_);
#endif
}

RWMutex::RWMutex() = default;

RWMutex::~RWMutex() = default;

void RWMutex::ReadLock() {
  mutex_.lock_shared().get();
}

void RWMutex::ReadUnlock() {
  mutex_.unlock_shared();
}

void RWMutex::WriteLock() {
  mutex_.lock().get();
}

void RWMutex::WriteUnlock() {
  mutex_.unlock();
}

CondVar::CondVar(Mutex* mu)
  : mu_{mu}
{}

void CondVar::Wait() {
  mu_->AssertHeld();
  mu_->mutex_.unlock();
  cond_.wait().finally([this] {
    return mu_->mutex_.lock().then([this] {
#ifndef NDEBUG
      mu_->locked_ = true;
#endif
    });
  }).get();
}

CondVar::~CondVar() = default;

bool CondVar::TimedWait(uint64_t abs_time_us) {
  mu_->AssertHeld();
  using time_point = seastar::timer<>::time_point;
  const time_point abs_time{std::chrono::microseconds{abs_time_us}};
  try {
    mu_->mutex_.unlock();
    cond_.wait(abs_time).finally([this] {
      return mu_->mutex_.lock().then([this] {
#ifndef NDEBUG
        mu_->locked_ = true;
#endif
      });
    }).get();
    return true;
  } catch (seastar::condition_variable_timed_out&) {
    return false;
  }
}

void CondVar::Signal() {
  mu_->AssertHeld();
  cond_.signal();
}

void CondVar::SignalAll() {
  mu_->AssertHeld();
  cond_.broadcast();
}

bool Thread::joinable() const noexcept {
  return thread_.joinable();  
}

void Thread::join() {
  thread_.join().get();
}

Thread::native_handle_type Thread::native_handle() {
  // for appeasing RepeatableThread
  return pthread_self();
}

int PhysicalCoreID() {
  return seastar::engine().cpu_id();
}

void *cacheline_aligned_alloc(size_t size) {
  return aligned_alloc(seastar::cache_line_size, size);
}

void cacheline_aligned_free(void *memblock) {
  free(memblock);
}

void Crash(const std::string& srcfile, int srcline) {
  std::cout << "Crashing at " << srcfile << ":" << srcline << std::endl;
  kill(getpid(), SIGTERM);
}

int GetMaxOpenFiles() {
  return -1;
}
  
} // namespace port
} // namespace rocksdb
