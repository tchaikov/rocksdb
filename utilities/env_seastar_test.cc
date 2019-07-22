// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>

#include "util/stderr_logger.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

SEASTAR_TEST_CASE(DBBasics) {
  return seastar::async([] {
    std::string kDBPath = "/tmp/DBBasics";
    rocksdb::DB* db;
    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.info_log.reset(new rocksdb::StderrLogger(rocksdb::InfoLogLevel::WARN_LEVEL));
    // XXX: use seastar env
    // options.env = env_;

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
    assert(s.ok());

    // Put key-value
    s = db->Put(rocksdb::WriteOptions(), "key1", "value");
    assert(s.ok());
    std::string value;
    // get value
    s = db->Get(rocksdb::ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value");

    // atomically apply a set of updates
    {
      rocksdb::WriteBatch batch;
      batch.Delete("key1");
      batch.Put("key2", value);
      s = db->Write(rocksdb::WriteOptions(), &batch);
    }

    s = db->Get(rocksdb::ReadOptions(), "key1", &value);
    assert(s.IsNotFound());

    db->Get(rocksdb::ReadOptions(), "key2", &value);
    assert(value == "value");

    delete db;
  });
}
