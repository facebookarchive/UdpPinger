/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

namespace cpp facebook.netnorad.thrift

struct TargetList {
  1 : list<Target> target_list;
}

struct Target {
  1 : string hostname;
  2 : string clustertype;
  3 : bool ignore;
  4 : string cluster;
  5 : string v4;
  6 : string v6;
  7 : string pod;
  8 : string rack;
}

struct Config {
  1 : i32 ignore_my_samples = false;
  2 : i32 pinger_cooldown_time = 2;
  3 : i32 pinger_rate = 500000;
  4 : i32 pinger_target_port = 65534;
  5 : i32 pinger_sender_threads = 2;
  6 : i32 pinger_receiver_threads = 8;

  // Number of racks from which 100% loss hosts will be ignored, once this
  // threshold is exceeded 100% loss hosts will contribute to pod / cluster
  // rollup stats (unless excluded by dead host detection)
  7 : i32 dead_racks_threshold = 4;

  // the starting base port to probe from
  8 : i32 base_src_port = 25000;

  // the port count to source probes from:
  // i.e. means we'll ping from [udp_port,  base_port + port_count]
  9 : i32 src_port_count = 256;

  // the buffer size (in bytes) we use for sender/receiver sockets
  10 : i32 socket_buffer_size = 4000000;
}

struct SiteInfo {
  1 : string dc;
  2 : string cluster;
  3 : string clustertype;
  4 : string pod;
  5 : string region;
  6 : string rack;
  7 : string hostname;
  8 : string hostprefix;
  9 : string ip;
}

struct Metadata {
  1 : SiteInfo src;
  2 : SiteInfo dst;
  3 : bool ipv6;
  4 : string proto;
  5 : string scope;
  6 : i32 tos;
  // is the target detected to be dead?
  8 : bool dead;
}

struct Metrics {
  1 : i32 numRecv;
  2 : i32 numXmit;
  3 : double avg;
  4 : double rttP90;
  5 : double rttP50;
  6 : double lossRatio;
}

struct TestResult {
  1 : double timestamp;
  2 : bool ignore;
  3 : Metadata metadata;
  4 : Metrics metrics;
  5 : bool aggregated;
}
