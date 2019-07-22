// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <array>
#include <set>
#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>
#include "rocksdb/env.h"

namespace rocksdb::port {

class SeastarEnv : public ::rocksdb::Env {
public:
  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<rocksdb::SequentialFile>* result,
                           const EnvOptions& options) final;
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<rocksdb::RandomAccessFile>* result,
                             const EnvOptions& options) final;
  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<rocksdb::WritableFile>* result,
                         const EnvOptions& options) final;
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<rocksdb::WritableFile>* result,
                           const EnvOptions& options) final;
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<rocksdb::Directory>* result) final;
  Status FileExists(const std::string& fname) final;
  Status GetChildren(const std::string& dir, std::vector<std::string>* result);
  Status DeleteFile(const std::string& fname) final;
  Status CreateDir(const std::string& dirname) final;
  Status CreateDirIfMissing(const std::string& dirname) final;
  Status DeleteDir(const std::string& dirname) final;
  Status GetFileSize(const std::string& fname, uint64_t* file_size) final;
  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) final;
  Status RenameFile(const std::string& src, const std::string& target) final;
  Status LinkFile(const std::string& src, const std::string& target) final;
  Status LockFile(const std::string& fname, FileLock** lock);
  Status UnlockFile(FileLock* lock);

  void Schedule(void (*function)(void* arg), void* arg,
		Priority pri = LOW, void* tag = nullptr,
		void (*unschedFunction)(void* arg) = nullptr) final;
  int UnSchedule(void* arg, Priority pri) final;
  void StartThread(void (*function)(void* arg), void* arg) final;
  void WaitForJoin() final;
  Status GetTestDirectory(std::string* result) final;
  uint64_t NowMicros() final;
  void SleepForMicroseconds(int micros) final;
  Status GetHostName(char* name, uint64_t len) final;
  Status GetCurrentTime(int64_t* unix_time) final;
  Status GetAbsolutePath(const std::string& db_path, std::string* output_path) final;

  void SetBackgroundThreads(int number, Priority pri = LOW) final;
  int GetBackgroundThreads(Priority pri = LOW) final;
  void IncBackgroundThreadsIfNeeded(int num, Priority pri) final;

  std::string TimeToString(uint64_t time) final;

  SeastarEnv();
  ~SeastarEnv();
private:
  seastar::shared_mutex mutex_locked_files;
  std::set<std::string>  locked_files;
  std::array<int, rocksdb::Env::TOTAL> bg_threads{0};
  std::vector<seastar::future<>> started_threads;
};

}
