/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <fstream>
#include <thread>
#include <gflags/gflags.h>

#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "UdpPinger.h"

using namespace std;
using namespace folly;
using namespace facebook::netnorad;

// Define our commandline arguments
DEFINE_string(target_file, "target_file.txt",
              "Target file to ping. One target per line");
DEFINE_int32(num_packets, 200, "Number of packets to send per target");
DEFINE_int32(sender_threads, 2, "Number of sender threads");
DEFINE_int32(receiver_threads, 8, "Number of receiver threads");
DEFINE_int32(target_port, 31338, "Target port");
DEFINE_int32(cooldown_time, 1, "Cooldown time");
DEFINE_int32(port_count, 64, "number of ports to ping from");
DEFINE_int32(base_port, 25000, "The starting UDP port to bind to");
DEFINE_int32(sender_rate, 5000, "The rate we ping with");
DEFINE_int32(socket_buffer_size, 425984, "Socket buffer size to send/recv");
DEFINE_bool(output_csv, false, "Print the target results as csv");
DEFINE_string(srcIpv4, "", "IPv4 source address to use in probe");
DEFINE_string(srcIpv6, "", "IPv6 source address to use in probe");
DEFINE_string(
    source_interface, "eth0",
    "The interface to use for source "
    "address of packets. Not required if ipv4/ipv6 source are defined.");

vector<UdpTestPlan> getTestPlans(const string &filename) {
  vector<UdpTestPlan> testPlans;
  ifstream inputFile(filename);
  // Input file is a set of IPv6 addresses, one per line
  string line;
  auto count = 0;

  // Read the lines, and use the address as the cluster and hostname (this
  // isn't super useful, but it is enough to make the UdpPinger work)
  while (inputFile >> line) {
    try {
      auto ipaddress = folly::IPAddress(line);
      ++count;
      thrift::Target target;
      UdpTestPlan testPlan;
      testPlan.target.hostname = line;
      testPlan.target.cluster = line;
      if (ipaddress.isV6()) {
        testPlan.target.v6 = line;
      } else {
        testPlan.target.v4 = line;
      }
      testPlan.numPackets = FLAGS_num_packets;
      testPlans.push_back(testPlan);
    } catch (folly::IPAddressFormatException const &e) {
      LOG(WARNING) << line << " isn't an IP Address :/";
    }
  }
  return testPlans;
}

std::string getAddressFromInterface(uint32_t inet_family) {
  // Make some structs
  struct ifaddrs *ifAddrStruct = NULL;
  struct ifaddrs *ifa = NULL;
  void *tmpAddrPtr = NULL;

  getifaddrs(&ifAddrStruct);

  // Let's find out what our source address should be
  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr->sa_family == AF_INET && inet_family == AF_INET) {
      // We're looking at an AF_NET address
      tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(inet_family, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (FLAGS_source_interface.compare(ifa->ifa_name) == 0) {
        return addressBuffer;
      }
    } else if (ifa->ifa_addr->sa_family == AF_INET6 &&
               inet_family == AF_INET6) { // check it is IP6
      // We're looking at an AF_NET6 address
      tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
      if (FLAGS_source_interface.compare(ifa->ifa_name) == 0) {
        return addressBuffer;
      }
    }
  }
  // Return something that's going to throw an error. If we get here, we've
  // already failed :(
  return "";
}

int main(int argc, char *argv[]) {
  // Lets start out with some help from our friends Google :)
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  // All the logs!
  FLAGS_logtostderr = 1;

  // Build a config object for our UdpPinger
  thrift::Config config;
  config.pinger_target_port = FLAGS_target_port;
  config.pinger_cooldown_time = FLAGS_cooldown_time;
  config.pinger_sender_threads = FLAGS_sender_threads;
  LOG(INFO) << config.pinger_sender_threads;
  config.pinger_receiver_threads = FLAGS_receiver_threads;
  config.pinger_rate = FLAGS_sender_rate;
  config.socket_buffer_size = FLAGS_socket_buffer_size;
  config.src_port_count = FLAGS_port_count;
  config.base_src_port = FLAGS_base_port;

  // Let's find our source address, if we've given specified it, use that
  // address, if not lets grab an address from an interface
  folly::IPAddress srcIpv4, srcIpv6;
  try {
    if (FLAGS_srcIpv4.length()) {
      srcIpv4 = folly::IPAddress(FLAGS_srcIpv4);
    } else {
      srcIpv4 = folly::IPAddress(getAddressFromInterface(AF_INET));
    }
  } catch (folly::IPAddressFormatException const &e) {
    srcIpv4 = folly::IPAddress("127.0.0.1");
    LOG(WARNING) << "Oh no! We are using loopback for IPv4";
  }

  try {
    if (FLAGS_srcIpv6.length()) {
      srcIpv6 = folly::IPAddress(FLAGS_srcIpv6);
    } else {
      srcIpv6 = folly::IPAddress(getAddressFromInterface(AF_INET6));
    }
  } catch (folly::IPAddressFormatException const &e) {
    srcIpv6 = folly::IPAddress("::1");
    LOG(WARNING) << "Oh no! We are using loopback for IPv6";
  }

  // Build TestPland from our targets file
  LOG(INFO) << "Reading targets from " << FLAGS_target_file;
  auto testPlans = getTestPlans(FLAGS_target_file);
  LOG(INFO) << "Pinging " << testPlans.size() << " targets";

  // Here is the magic, create our fancy UdpPinger
  UdpPinger pinger(config, srcIpv4, srcIpv6);
  auto results = pinger.run(testPlans, 0);

  // Print the result
  LOG(INFO) << "Finished with " << results.hostResults.size()
            << " host results";

  auto goodHosts = 0;
  auto totalHosts = 0;
  auto numPktsSent = 0;
  auto numPktsRecv = 0;

  for (const auto &hostResult : results.hostResults) {
    if (hostResult->metrics.numRecv > 0) {
      ++goodHosts;
      numPktsSent += hostResult->metrics.numXmit;
      numPktsRecv += hostResult->metrics.numRecv;
      if (FLAGS_output_csv){
        cout << hostResult->metadata.dst.ip << ","
          << hostResult->metrics.rttP50 << ","
          << hostResult->metrics.lossRatio << endl;
      }
    }
    ++totalHosts;
  }

  LOG(INFO) << "Good host " << goodHosts;
  LOG(INFO) << "Total host " << totalHosts;
  LOG(INFO) << "Healthy rate " << (float)goodHosts / totalHosts;
  LOG(INFO) << "Pkts sent " << numPktsSent;
  LOG(INFO) << "Pkts recv " << numPktsRecv;
  LOG(INFO) << "Hit rate " << (float)numPktsRecv / numPktsSent;

  return 0;
}
