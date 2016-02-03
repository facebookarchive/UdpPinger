/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <thread>
#include <gflags/gflags.h>
#include <chrono>
#include <folly/ThreadName.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include "NoradTargetThread.h"

using namespace std;
using namespace facebook::netnorad;
using namespace apache::thrift::concurrency;

// Define commandline args
DEFINE_int32(num_netnorad_threads, 8,
             "Number of netnorad thread pairs to start");
DEFINE_int32(netnorad_port, 31338,
             "The UDP port to listen for Net NORAD agent probes.");
DEFINE_int32(netnorad_queue_cap, 64000, "Capacity of Net NORAD shared queue.");

int pinToCore(int coreId) {
  int numCores = std::thread::hardware_concurrency();

  coreId = coreId % numCores;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(coreId, &cpuset);

  pthread_t current_thread = pthread_self();

  return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

int main(int argc, char *argv[]) {
  // Lets start out with some help from our friends Google :)
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  // All the logs!
  FLAGS_logtostderr = 1;

  // Netnorad threads
  std::vector<std::thread> noradReceiverThreads;
  std::vector<std::thread> noradSenderThreads;
  for (int i = 0; i < FLAGS_num_netnorad_threads; i++) {
    int noradSocket = kSockFdInvalid;
    try {
      noradSocket = initUdpServer(FLAGS_netnorad_port);
    } catch (const std::exception &e) {
      LOG(ERROR) << "Error starting Server";
    }
    if (noradSocket != kSockFdInvalid) {
      LOG(INFO) << "UDP server initialized, listening on "
                << FLAGS_netnorad_port;

      auto noradQueue = make_shared<folly::MPMCQueue<unique_ptr<NoradProbe>>>(
          FLAGS_netnorad_queue_cap);

      noradReceiverThreads.emplace_back(
          std::thread([noradQueue, noradSocket, i]() {
            pinToCore(i + ::sysconf(_SC_NPROCESSORS_ONLN) / 2);
            auto netNoradReceiver =
                make_shared<NoradTargetReceiverThread>(noradSocket, noradQueue);
            folly::setThreadName(pthread_self(), "NetNorad Receiver");
            netNoradReceiver->run();
          }));

      noradSenderThreads.emplace_back(
          std::thread([noradSocket, noradQueue, i]() {
            pinToCore(i + ::sysconf(_SC_NPROCESSORS_ONLN) / 2);
            auto netNoradSender =
                make_shared<NoradTargetSenderThread>(noradSocket, noradQueue);
            folly::setThreadName(pthread_self(), "NetNorad Sender");
            netNoradSender->run();
          }));
    }
  }
  while (1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  }

  for (auto &thread : noradReceiverThreads) {
    thread.join();
  }

  for (auto &thread : noradSenderThreads) {
    thread.join();
  }

  return 0;
}
