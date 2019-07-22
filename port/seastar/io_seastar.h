// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

#include "rocksdb/env.h"

namespace port_seastar {

using rocksdb::Status;
using rocksdb::Slice;

class SequentialFile : public ::rocksdb::SequentialFile {
public:
  SequentialFile(::seastar::file&& f, const rocksdb::EnvOptions& options)
    : f_{std::move(f)},
      use_direct_io_{options.use_direct_reads}
  {
    if (!use_direct_io_) {
      input_ = ::seastar::make_file_input_stream(std::move(f_));
    }
  }

  Status Read(size_t n, Slice* result, char* scratch) final {
    assert(!use_direct_io());
    size_t to_read = n;
    auto consumer = [&to_read, scratch](seastar::temporary_buffer<char> buf) mutable {
      using consumption_result_type =
	typename seastar::input_stream<char>::consumption_result_type;
      if (to_read > buf.size()) {
	if (!buf.empty()) {
	  memcpy(scratch, buf.get(), buf.size());
	  scratch += buf.size();
	  to_read -= buf.size();
	}
	if (to_read > 0) {
	  return seastar::make_ready_future<consumption_result_type>(
            seastar::continue_consuming{});
	} else {
	  return seastar::make_ready_future<consumption_result_type>(
            consumption_result_type::stop_consuming_type({}));
	}
      } else {
	if (to_read > 0) {
	  memcpy(scratch, buf.get(), to_read);
	  scratch += to_read;
	  to_read = 0;
	}
	return seastar::make_ready_future<consumption_result_type>(
          consumption_result_type::stop_consuming_type{std::move(buf)});
      }
    };
    input_.consume(std::move(consumer)).get();
    *result = Slice(scratch, n - to_read);
    return Status::OK();
  }
  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
			char* scratch) final {
    assert(use_direct_io());
    size_t len = f_.dma_read(offset, scratch, n).get0();
    *result = Slice(scratch, len);
    return Status::OK();
  }
  Status Skip(uint64_t n) final {
    assert(!use_direct_io());
    input_.skip(n).get();
    return Status::OK();
  }
  bool use_direct_io() const final {
    return use_direct_io_;
  }
private:
  ::seastar::file f_;
  bool use_direct_io_;
  ::seastar::input_stream<char> input_;
};

class RandomAccessFile : public ::rocksdb::RandomAccessFile {
public:
  RandomAccessFile(::seastar::file&& f)
    : f_{std::move(f)}
  {}

  Status Read(uint64_t offset, size_t n, Slice* result,
	      char* scratch) const final {
    assert(use_direct_io());
    size_t len = f_.dma_read(offset, scratch, n).get0();
    *result = Slice(scratch, len);
    return Status::OK();
  }
  bool use_direct_io() const final {
    return true;
  }
private:
  mutable ::seastar::file f_;
};

class WritableFile : public ::rocksdb::WritableFile {
public:
  WritableFile(::seastar::file&& f) {
    seastar::file_output_stream_options options;
    // use a larger write buffer to amortize the overhead caused by context
    // switch
    options.buffer_size = f.disk_write_dma_alignment() * 64;
    output_ = ::seastar::make_file_output_stream(std::move(f), options);
  }
  ~WritableFile() {
    if (!closed_) {
      output_.close().get();
    }
  }
  Status Append(const Slice& data) final {
    output_.write(data.data(), data.size()).get();
    return Status::OK();
  }
  Status Close() final {
    if (!closed_) {
      output_.close().get();
      closed_ = true;
    }
    return Status::OK();
  }
  Status Flush() final {
    // TODO: implment a variant of seastar::output_stream (and its sink) to
    //   - flush the _zc_bufs but keep it around until its size reaches _size,
    //     update the sink's pos by multiple of file::dma_alignment. so
    //     if one flush() the stream with unaligned tail, the next flush() will
    //     write the buffer starting with that unaligned tail with the same pos
    //   - prefill _zc_bufs with the unaligned parts in the end of file when
    //     the output_stream is constructed. and update sink's pos to
    //     align_down(file_size) when opening an existing file.
    return Status::OK();
  }
  Status Sync() final {
    return Status::OK();
  }

private:
  ::seastar::output_stream<char> output_;
  bool closed_ = false;
};

class RandomRWFile : public ::rocksdb::RandomRWFile {
 public:
  explicit RandomRWFile(::seastar::file&& f)
    : file_{std::move(f)}
  {}
  ~RandomRWFile() final {
    if (!closed_) {
      file_.close().get();
    }
  }
  Status Write(uint64_t offset, const Slice& data) final {
    auto ptr =
      ::seastar::allocate_aligned_buffer<char>(data.size(),
				    file_.disk_write_dma_alignment());
    auto buf = ptr.get();
    memcpy(buf, data.data(), data.size());
    file_.dma_write(offset, buf, data.size()).finally([ptr=std::move(ptr)] {
    }).get();
    return Status::OK();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
	      char* scratch) const final {
    auto buf = const_cast<::seastar::file&>(file_).dma_read<char>(offset, n).get0();
    memcpy(scratch, buf.get(), buf.size());
    *result = Slice(scratch, n);
    return Status::OK();
  }
  Status Flush() final {
    file_.flush().get();
    return Status::OK();
  }
  Status Sync() final {
    return Status::OK();
  }
  Status Fsync() final {
    return Status::OK();
  }
  Status Close() final {
    file_.close().get();
    closed_ = true;
    return Status::OK();
  }
private:
  ::seastar::file file_;
  bool closed_ = false;
};

class Directory : public ::rocksdb::Directory {
public:
  Directory(::seastar::file&& f)
    : f_{std::move(f)}
  {}
  ~Directory() {
    f_.close().get();
  }
  Status Fsync() final {
    f_.flush().get();
    return Status::OK();
  }
private:
  ::seastar::file f_;
};

}
