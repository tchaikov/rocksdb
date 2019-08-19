//  Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#elif defined(SEASTAR_API_LEVEL)
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <rocksdb/db_bench_tool.h>

int main(int argc, char** argv) {
  seastar::app_template app;
  std::vector<char*> args{argv[0]};
  return app.run(args.size(), args.data(), [&] {
    return seastar::async([=] {
      rocksdb::db_bench_tool(argc, argv);
    }).or_terminate();
  });
}
#else
#include <rocksdb/db_bench_tool.h>
int main(int argc, char** argv) { return rocksdb::db_bench_tool(argc, argv); }
#endif  // GFLAGS
