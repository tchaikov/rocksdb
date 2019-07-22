#include "port/seastar/env_seastar.h"

#include <time.h>
#include <chrono>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <seastar/core/file-types.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/std-compat.hh>

#include "port/seastar/io_seastar.h"

namespace rocksdb::port {

namespace fs = seastar::compat::filesystem;

Status SeastarEnv::NewSequentialFile(const std::string& fname,
                                     std::unique_ptr<rocksdb::SequentialFile>* result,
                                     const rocksdb::EnvOptions& options)
{
  try {
    auto f = seastar::open_file_dma(fname, seastar::open_flags::ro).get0();
    result->reset(new port_seastar::SequentialFile{std::move(f), options});
  } catch (std::system_error&) {
    return Status::IOError("While opening a file for sequentially reading",
                           fname);
  }
  return Status::OK();
}

Status SeastarEnv::NewRandomAccessFile(const std::string& fname,
                                       std::unique_ptr<RandomAccessFile>* result,
                                       const rocksdb::EnvOptions& options)
{
  try {
    auto f = seastar::open_file_dma(fname, seastar::open_flags::ro).get0();
    result->reset(new port_seastar::RandomAccessFile{std::move(f)});
  } catch (std::system_error& e) {
    return Status::IOError("While opening a file for random reading", fname);
  }
  return Status::OK();
}

Status SeastarEnv::NewWritableFile(const std::string& fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options)
{
  try {
    auto f = seastar::open_file_dma(fname,
                                    (seastar::open_flags::wo |
                                     seastar::open_flags::create)).get0();
    result->reset(new port_seastar::WritableFile{std::move(f)});
  } catch (std::system_error& e) {
    return Status::IOError("While opening a file for writing", fname);
  }
  return Status::OK();
}

Status SeastarEnv::ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     std::unique_ptr<WritableFile>* result,
                                     const EnvOptions& options)
{
  seastar::rename_file(old_fname, fname).get();
  return Status::OK();
}

Status SeastarEnv::NewDirectory(const std::string& name,
                         std::unique_ptr<Directory>* result)
{
  try {
    auto f = seastar::open_directory(name).get0();
    result->reset(new port_seastar::Directory(std::move(f)));
    return Status::OK();
  } catch (fs::filesystem_error& e) {
    return Status::IOError("While open directory", name);
  }
}

Status SeastarEnv::FileExists(const std::string& fname) {
  if (seastar::file_exists(fname).get0()) {
    return Status::OK();
  } else {
    return Status::NotFound();
  }
}

Status SeastarEnv::GetChildren(const std::string& dir, std::vector<std::string>* result)
{
  try {
    seastar::open_directory(dir).then([result](auto f) {
      using listing_t = seastar::subscription<seastar::directory_entry>;
      listing_t listing{
        f.list_directory([result](seastar::directory_entry de) {
          result->push_back(de.name);
          return seastar::now();
        })};
      return seastar::do_with(std::move(f), std::move(listing), [](auto& f, auto& listing) {
        return listing.done();
      });
    }).get();
    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    if (auto ec = e.code();
        ec == std::errc::no_such_file_or_directory ||
        ec == std::errc::permission_denied ||
        ec == std::errc::not_a_directory) {
      return Status::NotFound();
    } else {
      return Status::IOError("While opendir", dir);
    }
  }
}

Status SeastarEnv::DeleteFile(const std::string& fname) {
  try {
    seastar::remove_file(fname).get();
  } catch (const fs::filesystem_error& e) {
    if (auto ec = e.code(); ec == std::errc::no_such_file_or_directory) {
      return Status::PathNotFound("While remove_file", fname);
    } else {
      return Status::IOError("while remove_file()", fname);
    }
  }
  return Status::OK();
}

Status SeastarEnv::CreateDir(const std::string& dirname) {
  try {
    seastar::make_directory(dirname).get();
  } catch (fs::filesystem_error& e) {
    return Status::IOError("While mkdir", dirname);
  }
  return Status::OK();
}

Status SeastarEnv::CreateDirIfMissing(const std::string& dirname) {
  try {
    seastar::make_directory(dirname).then_wrapped([dirname](auto f) {
      try {
        f.get();
        return seastar::now();
      } catch (fs::filesystem_error& e) {
        return seastar::open_directory(dirname).then([](auto f) {
          std::move(f);
        });
      }
    }).get();
  } catch (fs::filesystem_error& e) {
    return Status::IOError("While mkdir if missing", dirname);
  }
  return Status::OK();
}

Status SeastarEnv::DeleteDir(const std::string& dirname) {
  seastar::remove_file(dirname).get();
  return Status::OK();
}

Status SeastarEnv::GetFileSize(const std::string& fname, uint64_t* file_size)
{
  *file_size = seastar::file_size(fname).get0();
  return Status::OK();
}

Status SeastarEnv::GetFileModificationTime(const std::string& fname,
                               uint64_t* file_mtime)
{
  seastar::stat_data s = seastar::file_stat(fname).get0();
  auto epoch = s.time_modified.time_since_epoch();
  *file_mtime = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
  return Status::OK();
}

Status SeastarEnv::RenameFile(const std::string& src, const std::string& target)
{
  try {
    seastar::rename_file(src, target).get();
  } catch (std::system_error&) {
    return Status::IOError("While renaming a file to " + target, src);
  }
  return Status::OK();
}

Status SeastarEnv::LinkFile(const std::string& src, const std::string& target)
{
  seastar::link_file(src, target).get();
  return Status::OK();
}

namespace {
  class FileLock : public::rocksdb::FileLock {
  public:
    FileLock(const std::string fname)
      : fname{fname}
    {}
    const std::string fname;
  };
}

Status SeastarEnv::LockFile(const std::string& fname, ::rocksdb::FileLock** lock)
{
  mutex_locked_files.lock().get();
  if (locked_files.insert(fname).second) {
    *lock = new FileLock{fname};
    mutex_locked_files.unlock();
    return Status::OK();
  } else {
    // already locked
    mutex_locked_files.unlock();
    return Status::IOError("lock ", fname);
  }
}

Status SeastarEnv::UnlockFile(::rocksdb::FileLock* lock)
{
  auto my_lock = static_cast<FileLock*>(lock);
  Status result;
  mutex_locked_files.lock().get();
  if (locked_files.erase(my_lock->fname) == 1) {
    mutex_locked_files.unlock();
  } else {
    // already unlocked
    mutex_locked_files.unlock();
    result = Status::IOError("unlock ", my_lock->fname);
  }
  delete my_lock;
  return result;
}

void SeastarEnv::Schedule(void (*function)(void* arg1), void* arg, Priority pri,
                   void* tag,
                   void (*unschedFunction)(void* arg))
{
  (void)seastar::async([function, arg] { (*function)(arg); });
}

int SeastarEnv::UnSchedule(void* arg, Priority pri) {
  return 0;
}

void SeastarEnv::StartThread(void (*function)(void* arg), void* arg)
{
  started_threads.push_back(seastar::async([function, arg] { (*function)(arg); }));
}

void SeastarEnv::WaitForJoin()
{
  seastar::when_all(started_threads.begin(),
                    started_threads.end()).get();
  started_threads.clear();
}

Status SeastarEnv::GetTestDirectory(std::string* result)
{
  if (const char* env = getenv("TEST_TMPDIR"); env && env[0] != '\0') {
    *result = env;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "/tmp/rocksdbtest-%d", int(geteuid()));
    *result = buf;
  }
  return CreateDir(*result);
}

uint64_t SeastarEnv::NowMicros()
{
  auto now = std::chrono::steady_clock::now();
  auto epoch = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::microseconds>(epoch).count();
}

void SeastarEnv::SleepForMicroseconds(int micros)
{
  seastar::sleep(std::chrono::microseconds(micros)).get();
}

Status SeastarEnv::GetHostName(char* name, uint64_t len)
{
  int ret = gethostname(name, static_cast<size_t>(len));
  if (ret < 0) {
    if (errno == EFAULT || errno == EINVAL)
      return Status::InvalidArgument(strerror(errno));
    else
      return Status::IOError("GetHostName", name);
  } else {
    return Status::OK();
  }
}

Status SeastarEnv::GetCurrentTime(int64_t* unix_time)
{
  auto now = std::chrono::steady_clock::now();
  auto epoch = now.time_since_epoch();
  *unix_time = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
  return Status::OK();
}

Status SeastarEnv::GetAbsolutePath(const std::string& db_path, std::string* output_path)
{
  if (!db_path.empty() && db_path[0] == '/') {
    *output_path = db_path;
    return Status::OK();
  }
  char the_path[256];
  char* ret = getcwd(the_path, 256);
  if (ret == nullptr) {
    return Status::IOError(strerror(errno));
  } else {
    *output_path = ret;
    return Status::OK();
  }
}

void SeastarEnv::SetBackgroundThreads(int number, rocksdb::Env::Priority pri)
{
  bg_threads[pri] = number;
}

int SeastarEnv::GetBackgroundThreads(rocksdb::Env::Priority pri)
{
  return bg_threads[pri];
}

void SeastarEnv::IncBackgroundThreadsIfNeeded(int num, Priority pri)
{}

std::string SeastarEnv::TimeToString(uint64_t epoch)
{
  time_t time = (time_t)(epoch);
  std::string result{21, ' '};
  char* p = &result[0];
  tm t;
  auto n = std::strftime(p, result.size(), "%Y/%m/%d-%T",
                         localtime_r(&time, &t));
  result.resize(n);
  return result;
}

SeastarEnv::SeastarEnv() {}
SeastarEnv::~SeastarEnv()
{
  WaitForJoin();
}
}

namespace rocksdb {
std::string Env::GenerateUniqueId() {
  return boost::uuids::to_string(boost::uuids::random_generator()());
}

Env* Env::Default() {
  static rocksdb::port::SeastarEnv default_env;
  return &default_env;
}
}
