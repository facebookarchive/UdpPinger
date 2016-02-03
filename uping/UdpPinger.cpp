/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "UdpPinger.h"

#include <linux/filter.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cmath>
#include <thread>
#include <future>

#include <folly/gen/Base.h>
#include <folly/gen/Core.h>
#include <folly/stats/Histogram-defs.h>
#include <folly/ThreadName.h>

#include <folly/Logging.h>

using namespace std;
using namespace folly;

using folly::gen::as;
using folly::gen::from;
using folly::gen::mapped;

DEFINE_int32(bucket_min, 1e3, "Minimum RTT in us");
DEFINE_int32(bucket_max, 3e5, "Maximum RTT in us");
DEFINE_int32(bucket_size, 5e3, "Bucket size for histograms");

namespace {

using facebook::netnorad::AsyncUDPSocket;

const string kUdp = "udp";
const string kClusterPodDelimiter = "||";
default_random_engine generator;
uniform_int_distribution<uint32_t> distribution(0, INT_MAX);

//
// Get a timestamp and store it in uint32_t object. Note that if the timestamp
// is larger than 32 bit, only the LSBs are recorded, which is useful to
// calculate time difference.
// NOTE: we use non-monitonic clock since kernel time-stamps do not support
// monotonic timer :(
//
template <class T> inline uint32_t getTimestamp() {
  return chrono::duration_cast<T>(
             chrono::system_clock::now().time_since_epoch())
      .count();
}

void addToHistograms(
    const uint32_t rtt, const IPAddress &address, const string &cluster,
    shared_ptr<unordered_map<IPAddress, facebook::netnorad::Histogram>>
        hostHistograms,
    shared_ptr<unordered_map<string, facebook::netnorad::Histogram>>
        clusterHistograms) {
  auto iter = hostHistograms->find(address);
  if (iter == hostHistograms->end()) {
    facebook::netnorad::Histogram histogram(FLAGS_bucket_size, FLAGS_bucket_min,
                                            FLAGS_bucket_max);
    auto result = hostHistograms->emplace(address, std::move(histogram));
    iter = result.first;
  }
  iter->second.addValue(rtt);
  if (!cluster.empty()) {
    auto clusterIter = clusterHistograms->find(cluster);
    if (clusterIter == clusterHistograms->end()) {
      facebook::netnorad::Histogram histogram(
          FLAGS_bucket_size, FLAGS_bucket_min, FLAGS_bucket_max);
      auto result = clusterHistograms->emplace(cluster, std::move(histogram));
      clusterIter = result.first;
    }
    clusterIter->second.addValue(rtt);
  }
}

std::pair<string, string> clusterPodSplit(const string &clusterPod) {
  std::pair<string, string> result;
  if (!folly::split(kClusterPodDelimiter, clusterPod, result.first,
                    result.second)) {
    result.first = clusterPod;
    result.second.clear();
  }
  return result;
}

std::string clusterPodJoin(const std::string &cluster, const std::string &pod) {
  auto clusterPod = cluster;
  if (!pod.empty()) {
    clusterPod += kClusterPodDelimiter + pod;
  }
  return clusterPod;
}

int pinToCore(int coreId) {
  int numCores = ::sysconf(_SC_NPROCESSORS_ONLN);

  coreId = coreId % numCores;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(coreId, &cpuset);

  pthread_t current_thread = pthread_self();

  return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

//
// The following two methods are used for checksum computations
//

inline uint32_t uint16_checksum(uint16_t *data, size_t size) {
  uint32_t sum = 0;

  while (size >= 2) {
    sum += (uint32_t)ntohs(*data);
    data++;
    size -= 2;
  }

  if (size == 1) {
    sum += (uint32_t)ntohs(*(uint8_t *)data);
  }

  return sum;
}

uint16_t ipv6UdpCheckSum(uint16_t *data, uint16_t size, struct in6_addr *src,
                         struct in6_addr *dst, uint8_t proto) {
  uint32_t phdr[2];
  uint32_t sum = 0;
  uint16_t sum2;

  sum += uint16_checksum((uint16_t *)(void *)src, 16);
  sum += uint16_checksum((uint16_t *)(void *)dst, 16);

  phdr[0] = htonl(size);
  phdr[1] = htonl(proto);

  sum += uint16_checksum((uint16_t *)phdr, 8);
  sum += uint16_checksum(data, size);

  sum = (sum & 0xFFFF) + (sum >> 16);
  sum = (sum & 0xFFFF) + (sum >> 16);
  sum2 = htons((uint16_t)sum);
  sum2 = ~sum2;

  if (sum2 == 0) {
    return 0xFFFF;
  }

  return sum2;
}

//
// Creates a v4 or v6 UDP raw socket
//

int createRawSocket(bool isV4, int qos, int bufferSize) {
  int sock;

  if (isV4) {
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = 0;

    sock = ::socket(AF_INET, SOCK_RAW, IPPROTO_UDP);
    if (sock < 0) {
      LOG(ERROR) << "Error creating raw v4 socket '" << errnoStr(errno) << "'";
    }

    if (::bind(sock, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) <
        0) {
      LOG(ERROR) << "Error binding v4 socket '" << errnoStr(errno) << "'";
    }

    if (::setsockopt(sock, IPPROTO_IP, IP_TOS, &qos, sizeof(qos)) == -1) {
      LOG(ERROR) << "setsockopt IP_TOS failed '" << errnoStr(errno) << "'";
    }

  } else {
    struct sockaddr_in6 addr;
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = IN6ADDR_ANY_INIT;
    addr.sin6_flowinfo = 0;
    addr.sin6_port = 0;

    sock = ::socket(AF_INET6, SOCK_RAW, IPPROTO_UDP);
    if (sock < 0) {
      LOG(ERROR) << "Error creating raw v6 socket '" << errnoStr(errno) << "'";
    }

    if (::bind(sock, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) <
        0) {
      LOG(ERROR) << "Error binding v6 socket '" << errnoStr(errno) << "'";
    }

    if (::setsockopt(sock, IPPROTO_IPV6, IPV6_TCLASS, &qos, sizeof(qos)) ==
        -1) {
      LOG(ERROR) << "UdpWorker: setsockopt IPV6_TCLASS failed '"
                 << errnoStr(errno) << "'";
    }
  }

  // this is 'ret 0' BPF instruction - discard all packets
  struct sock_filter code[] = {
      {0x06, 0, 0, 0x00000000},
  };

  struct sock_fprog bpf = {
      .len = sizeof(code) / sizeof(code[0]), .filter = code,
  };

  if (::setsockopt(sock, SOL_SOCKET, SO_ATTACH_FILTER, &bpf, sizeof(bpf)) < 0) {
    LOG(ERROR) << "Cannot assign BPF filter to send socket";
  }

  if (bufferSize) {
    LOG(INFO) << "Setting raw socket buffer to " << bufferSize;
    if (::setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufferSize,
                     sizeof(bufferSize)) == -1) {
      LOG(ERROR) << "Raw Socket: setsockopt SO_SNDBUF failed '"
                 << errnoStr(errno) << "'";
    }

    int optval;
    socklen_t optlen;
    optlen = sizeof(optval);
    ::getsockopt(sock, SOL_SOCKET, SO_SNDBUF, &optval, &optlen);

    LOG(INFO) << "Raw socket: getsockopt SO_SNDBUF returned '" << optval << "'";
  }

  return sock;
}

//
// Creates and binds UDP sockets for receiving, reports
// the local ports that could not be bound
//

pair<vector<shared_ptr<AsyncUDPSocket>> /* sockets */,
     set<int> /* missing ports */>
createUdpSockets(EventBase *evb, bool isV4, int basePort, int portCount,
                 int bufferSize, AsyncUDPSocket::ReadCallback *cob) {

  CHECK(evb) << "Event base can't be null";

  std::vector<std::shared_ptr<AsyncUDPSocket>> sockets;
  std::set<int> missingPorts;

  // create N sockets and try binding them to successive ports
  // if this fails, use random port
  for (int i = 0; i < portCount; ++i) {
    sockets.emplace_back(make_shared<AsyncUDPSocket>(evb));
    auto &socket = sockets.back();
    socket->setReusePort(true);

    if (isV4) {
      try {
        socket->bind(SocketAddress("0.0.0.0", basePort + i));
      } catch (folly::AsyncSocketException const &e) {
        missingPorts.emplace(basePort + i);
        continue;
      }
    } else {
      try {
        socket->bind(SocketAddress("::", basePort + i));
      } catch (folly::AsyncSocketException const &e) {
        missingPorts.emplace(basePort + i);
        continue;
      }
    }

    // enable timestamping for this socket
    int enabled = 1;
    if (::setsockopt(socket->getFD(), SOL_SOCKET, SO_TIMESTAMPNS, &enabled,
                     sizeof(enabled)) < 0) {
      LOG(ERROR) << "UdpWorker: setsockopt SO_TIMESTAMPNS failed '"
                 << errnoStr(errno) << "'";
    }

    if (bufferSize) {
      if (::setsockopt(socket->getFD(), SOL_SOCKET, SO_RCVBUF, &bufferSize,
                       sizeof(bufferSize)) == -1) {
        LOG(ERROR) << "UdpWorker: setsockopt SO_RCVBUF failed '"
                   << errnoStr(errno) << "'";
      }

      int optval;
      socklen_t optlen;
      optlen = sizeof(optval);
      ::getsockopt(socket->getFD(), SOL_SOCKET, SO_RCVBUF, &optval, &optlen);

      if (2 * bufferSize != optval) {
        LOG(WARNING) << "Udp socket: getsockopt SO_RCVBUF returned '" << optval
                     << "'"
                     << " when requested " << bufferSize;
      }
    }

    if (cob) {
      socket->resumeRead(cob);
    }
  }

  return make_pair(sockets, missingPorts);
}
}

namespace facebook {
namespace netnorad {

//
// UdpReceiver
//

void UdpReceiver::waitForSocketsToBind() {
  // busy wait for variable to be setp
  while (!socketsAreBound_) {
    std::this_thread::yield();
  }
}

UdpReceiver::UdpReceiver(
    const thrift::Config &config, uint32_t signature, int receiverId,
    std::vector<std::shared_ptr<folly::NotificationQueue<ReceiveProbe>>>
        recvQueues,
    const std::unordered_map<folly::IPAddress, thrift::Target> &ipToTargetMap,
    const std::unordered_map<std::string, std::string> &clusterToClusterTypeMap)
    : config_(config), signature_(signature), receiverId_(receiverId),
      recvQueues_(recvQueues), ipToTargetMap_(ipToTargetMap),
      clusterToClusterTypeMap_(clusterToClusterTypeMap),
      hostHistogramsV4_(make_shared<unordered_map<IPAddress, Histogram>>()),
      clusterHistogramsV4_(make_shared<unordered_map<string, Histogram>>()),
      hostHistogramsV6_(make_shared<unordered_map<IPAddress, Histogram>>()),
      clusterHistogramsV6_(make_shared<unordered_map<string, Histogram>>()) {

  // wipe out message header first
  memset(&msg_, 0, sizeof(msg_));

  // we only expect to receive one block of data, single entry
  // in the vector
  msg_.msg_iov = &entry_;
  msg_.msg_iovlen = 1;

  entry_.iov_base = readBuf_;
  entry_.iov_len = sizeof(readBuf_);

  // control message buffer used to receive time-stamps from
  // the kernel
  msg_.msg_control = ctrlBuf_;
  msg_.msg_controllen = sizeof(ctrlBuf_);

  // prepare to receive either v4 or v6 addresses
  ::memset(&addrStorage_, 0, sizeof(addrStorage_));
  msg_.msg_name = &addrStorage_;
  msg_.msg_namelen = sizeof(sockaddr_storage);
}

void UdpReceiver::run(int qos) {
  // the below won't be used in receiving thread, they are a byproduct of
  // the createUdpSockets function
  set<int> _missingPortsV4, _missingPortsV6;

  // create the sockets we'll be listening on
  tie(socketsV4_, _missingPortsV4) = createUdpSockets(
      &evb_, true /* is v4 */, config_.base_src_port, config_.src_port_count,
      config_.socket_buffer_size /* buff size */, this /* cob */);

  tie(socketsV6_, _missingPortsV6) = createUdpSockets(
      &evb_, false /* is v4 */, config_.base_src_port, config_.src_port_count,
      config_.socket_buffer_size /* buff size */, this /* cob */);

  socketsAreBound_ = true;

  receivingConsumer_ = NotificationQueue<ReceiveProbe>::Consumer::make([this](
      ReceiveProbe && msg) noexcept {
    consumeMessage(forward<ReceiveProbe>(msg));
  });
  receivingConsumer_->startConsuming(&evb_, recvQueues_[receiverId_].get());

  // this will loop until stopped explicitly by an external caller
  evb_.loopForever();

  // aggregate the stats that we have accumulated
  summarizeResults(qos);
}

void UdpReceiver::stop() {
  evb_.runInEventBaseThread([&] {
    closeSockets();
    receivingConsumer_->stopConsuming();
    evb_.terminateLoopSoon();
  });
}

const UdpTestResults &UdpReceiver::getResults() { return results_; }

void UdpReceiver::summarizeResults(int qos) {
  LOG(INFO) << "UDP receiver starting result summarization";

  CHECK(!evb_.isRunning());

  uint32_t now =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  for (const auto &iter : *hostHistogramsV4_) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->aggregated = false;
    result->metadata.ipv6 = false;
    result->metadata.proto = kUdp;
    result->metadata.tos = qos;
    try {
      const auto &target = ipToTargetMap_.at(iter.first);
      result->ignore = config_.ignore_my_samples | target.ignore;
      result->metadata.dst.cluster = target.cluster;
      result->metadata.dst.clustertype = target.clustertype;
      result->metadata.dst.pod = target.pod;
      result->metadata.dst.rack = target.cluster + ":" + target.rack;
      result->metadata.dst.hostname = target.hostname;
      result->metadata.dst.ip = target.v4;
    } catch (const out_of_range &e) {
      // We receive some packets that were not in our test plan
      VLOG(1) << "Received unexpected packet from " << iter.first;
      continue;
    }
    const auto &histogram = iter.second;
    result->metrics.numRecv = histogram.computeTotalCount();
    // The result metrics is in ms.
    result->metrics.rttP50 =
        (double)histogram.getPercentileEstimate(0.5) / 1000;
    result->metrics.rttP90 =
        (double)histogram.getPercentileEstimate(0.9) / 1000;
    result->metrics.avg = histogram.getAverage() / 1000;
    results_.hostResults.push_back(std::move(result));
  }
  hostHistogramsV4_->clear();

  for (const auto &iter : *hostHistogramsV6_) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->aggregated = false;
    result->metadata.ipv6 = true;
    result->metadata.proto = kUdp;
    result->metadata.tos = qos;
    try {
      const auto &target = ipToTargetMap_.at(iter.first);
      result->ignore = config_.ignore_my_samples | target.ignore;
      result->metadata.dst.cluster = target.cluster;
      result->metadata.dst.clustertype = target.clustertype;
      result->metadata.dst.pod = target.pod;
      result->metadata.dst.rack = target.cluster + ":" + target.rack;
      result->metadata.dst.hostname = target.hostname;
      result->metadata.dst.ip = target.v6;
    } catch (const out_of_range &e) {
      // We receive some packets that were not in our test plan
      VLOG(1) << "Received unexpected packet from " << iter.first;
      continue;
    }

    const auto &histogram = iter.second;
    result->metrics.numRecv = histogram.computeTotalCount();
    // The result metrics is in ms.
    result->metrics.rttP50 =
        (double)histogram.getPercentileEstimate(0.5) / 1000;
    result->metrics.rttP90 =
        (double)histogram.getPercentileEstimate(0.9) / 1000;
    result->metrics.avg = histogram.getAverage() / 1000;
    results_.hostResults.push_back(std::move(result));
  }
  hostHistogramsV6_->clear();

  for (const auto &iter : *clusterHistogramsV4_) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->ignore = config_.ignore_my_samples;
    result->aggregated = true;
    result->metadata.ipv6 = false;
    result->metadata.proto = "udp";
    result->metadata.tos = qos;
    auto clusterPod = clusterPodSplit(iter.first);
    result->metadata.dst.cluster = clusterPod.first;
    result->metadata.dst.clustertype =
        clusterToClusterTypeMap_.at(clusterPod.first);
    result->metadata.dst.pod = clusterPod.second;
    const auto &histogram = iter.second;
    result->metrics.numRecv = histogram.computeTotalCount();
    // The result metrics is in ms.
    result->metrics.rttP50 =
        (double)histogram.getPercentileEstimate(0.5) / 1000;
    result->metrics.rttP90 =
        (double)histogram.getPercentileEstimate(0.9) / 1000;
    result->metrics.avg = histogram.getAverage() / 1000;
    results_.clusterResults.push_back(std::move(result));
  }
  clusterHistogramsV4_->clear();

  for (const auto &iter : *clusterHistogramsV6_) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->ignore = config_.ignore_my_samples;
    result->aggregated = true;
    result->metadata.ipv6 = true;
    result->metadata.proto = "udp";
    result->metadata.tos = qos;
    auto clusterPod = clusterPodSplit(iter.first);
    result->metadata.dst.cluster = clusterPod.first;
    result->metadata.dst.clustertype =
        clusterToClusterTypeMap_.at(clusterPod.first);
    result->metadata.dst.pod = clusterPod.second;
    const auto &histogram = iter.second;
    result->metrics.numRecv = histogram.computeTotalCount();
    // The result metrics is in ms.
    result->metrics.rttP50 =
        (double)histogram.getPercentileEstimate(0.5) / 1000;
    result->metrics.rttP90 =
        (double)histogram.getPercentileEstimate(0.9) / 1000;
    result->metrics.avg = histogram.getAverage() / 1000;
    results_.clusterResults.push_back(std::move(result));
  }
  clusterHistogramsV6_->clear();

  LOG(INFO) << "Receiver done summarizing results";

  LOG(INFO) << "Built partial results host size " << results_.hostResults.size()
            << " cluster size " << results_.clusterResults.size();
}

void UdpReceiver::consumeMessage(ReceiveProbe &&message) noexcept {
  const auto rtt = message.rtt;
  auto address = message.remoteAddress.getIPAddress();
  auto targetIter = ipToTargetMap_.find(address);

  if (targetIter != ipToTargetMap_.end()) {
    auto clusterPod =
        clusterPodJoin(targetIter->second.cluster, targetIter->second.pod);
    if (address.isV4()) {
      addToHistograms(rtt, address, clusterPod, hostHistogramsV4_,
                      clusterHistogramsV4_);
    } else {
      addToHistograms(rtt, address, clusterPod, hostHistogramsV6_,
                      clusterHistogramsV6_);
    }
  } else {
    FB_LOG_EVERY_MS(ERROR, 100) << "Received unexpected packet from "
                                << address;
  }
}

void UdpReceiver::closeSockets() {
  for (auto &socket : socketsV4_) {
    socket->pauseRead();
    socket->close();
  }
  for (auto &socket : socketsV6_) {
    socket->pauseRead();
    socket->close();
  }
  socketsV4_.clear();
  socketsV6_.clear();
}

//
// UdpReceiver: AsyncUDPCallback methods
//

void UdpReceiver::getMessageHeader(struct msghdr **msg) noexcept {
  ::memset(&addrStorage_, 0, sizeof(addrStorage_));
  msg_.msg_namelen = sizeof(sockaddr_storage);
  *msg = &msg_;
}

void UdpReceiver::onMessageAvailable(size_t len) noexcept {
  uint32_t now = getTimestamp<chrono::microseconds>();

  folly::SocketAddress addr;
  addr.setFromSockaddr(reinterpret_cast<sockaddr *>(&addrStorage_),
                       sizeof(addrStorage_));

  if (msg_.msg_flags & MSG_TRUNC) {
    LOG(ERROR) << "UdpReadCallback: Dropping truncated data packet from "
               << addr;
    return;
  }

  if (len < kNoradProbeDataLen) {
    LOG(ERROR) << "UdpReadCallback: Received malformed packet";
    return;
  }

  // get the kernel timestamp
  struct cmsghdr *cmsg;
  struct timespec *stamp{nullptr};

  for (cmsg = CMSG_FIRSTHDR(&msg_); cmsg; cmsg = CMSG_NXTHDR(&msg_, cmsg)) {
    switch (cmsg->cmsg_level) {
    case SOL_SOCKET:
      switch (cmsg->cmsg_type) {
      case SO_TIMESTAMPNS: {
        stamp = (struct timespec *)CMSG_DATA(cmsg);
        break;
      }
      }
    }
  } // for

  // set to the same as now if kernel timestamp is not supported
  uint32_t recvTs = now;
  if (stamp) {
    // the kernel receive time-stamp
    recvTs = stamp->tv_nsec / 1000 + stamp->tv_sec * 1000000;
  }

  ReceiveProbe probe;
  probe.remoteAddress = addr;
  NoradProbeBody probeBody;
  ::memcpy(&probeBody, readBuf_, sizeof(probeBody));

  auto signature = ntohl(probeBody.signature);
  if (signature != signature_) {
    // Bogus packet
    LOG(ERROR) << "Signature mismatched in packet from " << addr;
    return;
  }

  // how large is the adjustment on receive side. Used only for logging
  uint32_t adjPinger = now - recvTs;

  // how much to adjust RTT based on target-collected timestamps
  // we use this data to compensate for wait time in the target process
  uint32_t adjTarget =
      (ntohl(probeBody.targetRespTime) - ntohl(probeBody.targetRcvdTime));

  probe.rtt = recvTs - ntohl(probeBody.pingerSentTime) - adjTarget;

  // ucomment if needed for debugging purposes - log the RTT adjustments
  FB_LOG_EVERY_MS(INFO, 1000) << folly::sformat(
      "Measured RTT {} usec for addr {}, adjustement by pinger {}, adjustment "
      "by target {}",
      probe.rtt, addr.describe(), adjPinger, adjTarget);

  string cluster;
  try {
    cluster = ipToTargetMap_.at(addr.getIPAddress()).cluster;
  } catch (std::out_of_range const &e) {
    FB_LOG_EVERY_MS(ERROR, 500) << "Response from unknown IP address "
                                << addr.getIPAddress();
    return;
  }

  auto it = clusterToQueueId_.find(cluster);
  int queueId;
  if (it == clusterToQueueId_.end()) {
    queueId = hasher_(cluster) % recvQueues_.size();
    clusterToQueueId_.emplace(std::move(cluster), queueId);
  } else {
    queueId = it->second;
  }

  // shortcut and bypass NotificationQueue, since this is for ourselves
  if (queueId == receiverId_) {
    consumeMessage(std::move(probe));
  } else {
    auto result = recvQueues_[queueId]->tryPutMessageNoThrow(std::move(probe));
    if (!result) {
      LOG(ERROR) << "Failed to enqueue packet from " << addr;
    }
  }
}

//
// UdpSender
//

UdpSender::UdpSender(
    const thrift::Config &config, int qos, int senderId, int numSenders,
    uint32_t signature, folly::IPAddress srcV4, folly::IPAddress srcV6,
    const set<int> &missingPortsV4, const set<int> &missingPortsV6,
    std::shared_ptr<folly::NotificationQueue<UdpTestPlan>> inputQueue)
    : config_(config), qos_(qos), senderId_(senderId), numSenders_(numSenders),
      signature_(signature), srcV4_(srcV4), srcV6_(srcV6),
      missingPortsV4_(missingPortsV4), missingPortsV6_(missingPortsV6),
      inputQueue_(std::move(inputQueue)) {}

void UdpSender::run() {
  socketV4_ = createRawSocket(true /* is v4 */, qos_,
                              config_.socket_buffer_size /* buff size */);
  socketV6_ = createRawSocket(false /* is v4 */, qos_,
                              config_.socket_buffer_size /* buff size */);

  // this guy will drain our input queue
  prepareConsumer();
  CHECK(sendingConsumer_);
  sendingConsumer_->startConsuming(&evb_, inputQueue_.get());

  // this will run out pinging loop
  evb_.loopForever();
  closeSockets();

  LOG(INFO) << "UdpSender " << senderId_ << " finished the run loop";
}

void UdpSender::buildAddressMap() {
  for (auto &tp : testPlans_) {
    if (!tp.target.v4.empty()) {
      // target address to send to
      struct sockaddr_in addr;
      addr.sin_family = AF_INET;
      addr.sin_port = 0;

      if (::inet_pton(AF_INET, tp.target.v4.c_str(), &addr.sin_addr.s_addr) <
          0) {
        LOG(ERROR) << "Malformed v4 address " << tp.target.v4;
      }

      auto &mappedAddr = addressMap_[tp.target.v4];
      ::memcpy(&mappedAddr, &addr, sizeof(addr));
    }

    if (!tp.target.v6.empty()) {
      // target address to send to
      struct sockaddr_in6 addr;
      addr.sin6_family = AF_INET6;
      addr.sin6_port = 0;

      if (::inet_pton(AF_INET6, tp.target.v6.c_str(), &addr.sin6_addr.s6_addr) <
          0) {
        LOG(ERROR) << "Malformed v6 address " << tp.target.v6;
      }

      auto &mappedAddr = addressMap_[tp.target.v6];
      ::memcpy(&mappedAddr, &addr, sizeof(addr));
    }
  }
}

void UdpSender::prepareConsumer() {
  sendingConsumer_ = NotificationQueue<UdpTestPlan>::Consumer::make([this](
      UdpTestPlan && testPlan) noexcept {

    // enqueue test-plans until we receive the signal to start probing
    if (testPlan.numPackets != 0) {
      testPlans_.push_back(std::move(testPlan));
      return;
    }

    sendingConsumer_->stopConsuming();
    buildAddressMap();
    pingAllTargets();

    evb_.runAfterDelay([this]() noexcept { evb_.terminateLoopSoon(); },
                       config_.pinger_cooldown_time * 1000 /* in ms */
                       );
  });
}

void UdpSender::pingAllTargets() {
  LOG(INFO) << "Worker " << senderId_ << " preparing to ping "
            << testPlans_.size() << " targets";

  // ranges of source ports we can use
  // we add the full allowed range except for
  // the ports that receivers could not bind to
  vector<int> srcPortsV4;
  vector<int> srcPortsV6;

  for (int port = config_.base_src_port;
       port < config_.base_src_port + config_.src_port_count; port++) {
    if (missingPortsV4_.find(port) != missingPortsV4_.end()) {
      continue;
    }
    srcPortsV4.push_back(port);
  }

  for (int port = config_.base_src_port;
       port < config_.base_src_port + config_.src_port_count; port++) {
    if (missingPortsV6_.find(port) != missingPortsV6_.end()) {
      continue;
    }
    srcPortsV6.push_back(port);
  }

  // some people bind all UDP ports in our range... weirdos
  bool v4Enabled = !srcV4_.isLoopback() && srcPortsV4.size();
  bool v6Enabled = !srcV6_.isLoopback() && srcPortsV6.size();

  while (true) {
    bool done = true;

    // we want to measure how long a full swing takes
    auto start = std::chrono::high_resolution_clock::now();
    // how many packets we sent in a single sweep - needed
    // for delay loop calibration
    int packetsSent = 0;
    int sendFailed = 0;

    NoradProbeBody *probeBody;
    UdpHeader *udpHeader;

    // we'll be updating counters inside testPlan, hence reference
    // this loop walks over all targets and sees if we need to send
    // packets for each one in turn. This effectively allow for very
    // broad "sweeps" over all targets, without sending large burts
    // to a single target
    for (auto &tp : testPlans_) {
      if (v4Enabled && !tp.target.v4.empty() &&
          (tp.packetsSentV4 < tp.numPackets)) {

        done = false;

        // target address to send to
        auto &addr = addressMap_[tp.target.v4];

        ::memset(&buf_[0], 0, sizeof(buf_));
        udpHeader = reinterpret_cast<UdpHeader *>(&buf_[0]);

        udpHeader->srcPort =
            htons(srcPortsV4[tp.packetsSentV4++ % srcPortsV4.size()]);
        udpHeader->dstPort = htons(config_.pinger_target_port);
        udpHeader->length = htons(sizeof(UdpHeader) + sizeof(NoradProbeBody));

        probeBody =
            reinterpret_cast<NoradProbeBody *>(&buf_[sizeof(UdpHeader)]);
        probeBody->signature = htonl(signature_);
        probeBody->pingerSentTime = htonl(getTimestamp<chrono::microseconds>());
        probeBody->tclass = qos_;

        if (::sendto(socketV4_, &buf_[0], sizeof(buf_), MSG_DONTWAIT,
                     reinterpret_cast<struct sockaddr *>(&addr),
                     sizeof(struct sockaddr_in)) == -1) {
          // if we can try again, de-count the packet, so we can
          // retry sending later
          if (errno == EAGAIN) {
            --tp.packetsSentV4;
          } else {
            FB_LOG_EVERY_MS(ERROR, 1000) << "UdpSender: v4 write error '"
                                         << errnoStr(errno) << "'";
            sendFailed++;
          }
        } else {
          packetsSent++;
        }
      }

      if (v6Enabled && !tp.target.v6.empty() &&
          (tp.packetsSentV6 < tp.numPackets)) {

        done = false;

        // target address to send to
        auto &addr = addressMap_[tp.target.v6];

        ::memset(&buf_[0], 0, sizeof(buf_));
        udpHeader = reinterpret_cast<UdpHeader *>(&buf_[0]);
        probeBody =
            reinterpret_cast<NoradProbeBody *>(&buf_[sizeof(UdpHeader)]);

        udpHeader->srcPort =
            htons(srcPortsV6[tp.packetsSentV6++ % srcPortsV6.size()]);
        udpHeader->dstPort = htons(config_.pinger_target_port);
        udpHeader->length = htons(sizeof(UdpHeader) + sizeof(NoradProbeBody));

        probeBody->signature = htonl(signature_);
        probeBody->pingerSentTime = htonl(getTimestamp<chrono::microseconds>());
        probeBody->tclass = qos_;

        struct in6_addr srcAddr =
            *reinterpret_cast<const struct in6_addr *>(srcV6_.bytes());

        udpHeader->checkSum = ipv6UdpCheckSum(
            reinterpret_cast<uint16_t *>(&buf_[0]), sizeof(buf_), &srcAddr,
            &((reinterpret_cast<struct sockaddr_in6 *>(&addr))->sin6_addr),
            IPPROTO_UDP);

        if (::sendto(socketV6_, &buf_[0], sizeof(buf_), MSG_DONTWAIT,
                     reinterpret_cast<struct sockaddr *>(&addr),
                     sizeof(struct sockaddr_in6)) == -1) {
          // if we can try again, de-count the packet, so we can
          // retry sending later
          if (errno == EAGAIN) {
            FB_LOG_EVERY_MS(INFO, 1000) << "EAGAIN in v6 sendto";
            --tp.packetsSentV6;
          } else {
            sendFailed++;
            FB_LOG_EVERY_MS(ERROR, 1000) << "UdpSender: v6 write error '"
                                         << errnoStr(errno) << "'";
          }
        } else {
          packetsSent++;
        }
      }
    }

    auto finish = std::chrono::high_resolution_clock::now();

    if (sendFailed) {
      LOG(ERROR) << "Failed sending " << sendFailed << " out of "
                 << packetsSent + sendFailed << " packets";
    }

    if (done) {
      break;
    }

    auto elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(finish - start);

    // the pps we achieved in this sweep
    double pps = std::ceil((1000000.0 / elapsed.count()) * packetsSent);

    FB_LOG_EVERY_MS(INFO, 1000)
        << "Ping sweep took: " << elapsed.count() << " usec, sent "
        << packetsSent << " packets, pps was " << pps << " target pps is "
        << config_.pinger_rate / numSenders_;

    // wait to hit the desired pps goal
    if (pps > config_.pinger_rate / numSenders_) {
      double delay = std::ceil(numSenders_ * packetsSent * 1000000.0 /
                               config_.pinger_rate);
      auto delayUsec =
          std::chrono::microseconds(folly::to<int64_t>(delay)) - elapsed;
      FB_LOG_EVERY_MS(INFO, 1000) << "Sleeping for " << delayUsec.count()
                                  << " usecs";
      /* sleep override */
      std::this_thread::sleep_for(delayUsec);
    }

  } // while

  LOG(INFO) << "Sender " << senderId_
            << " sent all probes, waiting for responses for "
            << config_.pinger_cooldown_time << " seconds";
}

void UdpSender::closeSockets() {
  ::close(socketV4_);
  ::close(socketV6_);
}

//
// UdpPinger
//

UdpPinger::UdpPinger(const thrift::Config &config, IPAddress srcV4,
                     IPAddress srcV6)
    : config_(config), srcV4_(srcV4), srcV6_(srcV6) {}

UdpTestResults UdpPinger::run(const vector<UdpTestPlan> &testPlans, int qos) {
  UdpTestResults results;
  int numCores = ::sysconf(_SC_NPROCESSORS_ONLN);
  auto numSenders = (int)config_.pinger_sender_threads;
  auto numReceivers = (int)config_.pinger_receiver_threads;

  LOG(INFO) << "UdpPinger for QoS " << qos << " and pps " << config_.pinger_rate
            << " starting with " << numSenders << " senders and "
            << numReceivers << " receivers";

  // the signature value that we'll be using for this pinger run
  uint32_t signature = distribution(generator);

  //
  // Pre-bind the UDP sockets to find any "blocked" source ports
  //

  // this won't be really used, only needed to create sockets
  EventBase _evb;
  vector<shared_ptr<AsyncUDPSocket>> _socketsV4, _socketsV6;
  set<int> missingPortsV4, missingPortsV6;

  tie(_socketsV4, missingPortsV4) = createUdpSockets(
      &_evb, true /* is v4 */, config_.base_src_port, config_.src_port_count,
      config_.socket_buffer_size /* buff size */, nullptr /* cob */);

  if (missingPortsV4.size()) {
    LOG(WARNING) << "IPv4: could not bind UDP ports "
                 << sformat(join(",", from(missingPortsV4) |
                                          mapped([](int port) {
                                            return to<string>(port);
                                          }) |
                                          as<vector>()));
  }

  tie(_socketsV6, missingPortsV6) = createUdpSockets(
      &_evb, false /* is v4 */, config_.base_src_port, config_.src_port_count,
      config_.socket_buffer_size /* buff size */, nullptr /* cob */);

  if (missingPortsV6.size()) {
    LOG(WARNING) << "IPv6: could not bind UDP ports "
                 << sformat(join(",", from(missingPortsV6) |
                                          mapped([](int port) {
                                            return to<string>(port);
                                          }) |
                                          as<vector>()));
  }

  //
  // create sender queues - this is where we'll put the test plans into
  // the UdpSenders will be reading from those queues
  //
  vector<future<void>> sendFutures;
  vector<shared_ptr<NotificationQueue<UdpTestPlan>>> sendingQueues;
  vector<thread> senderThreads;

  for (int i = 0; i < numSenders; ++i) {
    sendingQueues.emplace_back(make_shared<NotificationQueue<UdpTestPlan>>());
    auto sender = make_shared<UdpSender>(
        config_, qos, i /* senderId */, numSenders, signature, srcV4_, srcV6_,
        missingPortsV4, missingPortsV6, sendingQueues[i]);
    senderThreads.emplace_back(thread([sender, i] {
      pinToCore(i /* senderId */ + (::sysconf(_SC_NPROCESSORS_ONLN) / 2));
      folly::setThreadName(folly::sformat("UdpPinger-Sender-{}", i));
      sender->run();
    }));
  }

  //
  // create mapping tables used by receivers
  //
  unordered_map<IPAddress, thrift::Target> ipToTargetMap;
  unordered_map<std::string, std::string> clusterToClusterTypeMap;

  for (auto &testPlan : testPlans) {
    clusterToClusterTypeMap.emplace(testPlan.target.cluster,
                                    testPlan.target.clustertype);
    if (!testPlan.target.v4.empty()) {
      try {
        IPAddress ip(testPlan.target.v4);
        ipToTargetMap.emplace(std::move(ip), testPlan.target);
      } catch (const IPAddressFormatException &e) {
        LOG(ERROR) << testPlan.target.v4 << ":" << exceptionStr(e);
      }
    }
    if (!testPlan.target.v6.empty()) {
      try {
        IPAddress ip(testPlan.target.v6);
        ipToTargetMap.emplace(std::move(ip), testPlan.target);
      } catch (const IPAddressFormatException &e) {
        LOG(ERROR) << testPlan.target.v6 << ":" << exceptionStr(e);
      }
    }
  }

  // create receive queues: this is where UdpReceivers will dump results
  // we have to create them here, so we can pass those to each UdpReceiver
  // that we'll be creating
  vector<shared_ptr<NotificationQueue<ReceiveProbe>>> recvQueues;

  // we create all queues together since every receiver needs to know them all
  for (int i = 0; i < numReceivers; ++i) {
    recvQueues.emplace_back(make_shared<NotificationQueue<ReceiveProbe>>());
  }

  //
  // start the receivers
  //
  vector<thread> recvThreads;
  vector<shared_ptr<UdpReceiver>> receivers;
  for (int i = 0; i < numReceivers; ++i) {
    receivers.emplace_back(make_shared<UdpReceiver>(
        config_, signature, i /* receiverId */, recvQueues, ipToTargetMap,
        clusterToClusterTypeMap));
    recvThreads.emplace_back(thread([i, receivers, qos] {
      pinToCore(i /* receiverId */);
      folly::setThreadName(folly::sformat("UdpPinger-Receiver-{}", i));
      receivers[i]->run(qos);
    }));

    // wait till the guy we started binds its sockets
    receivers[i]->waitForSocketsToBind();
  }

  //
  // Now we can close sockets in out thread, since those are no longer needed
  // to hold the source UDP ports (all receivers have been created, and sockets
  // have been bound).
  //
  for (auto &socket : _socketsV4) {
    socket->close();
  }

  for (auto &socket : _socketsV6) {
    socket->close();
  }

  //
  // Build the maps we use to track packet loss per host/cluster/pod
  //

  // Probes we send (and expect to recv) per host
  unordered_map<string, int> hostProbeCountV4;
  // Probes we send (and expect to recv) per clusterPod
  unordered_map<string, int> clusterPodProbeCountV4;
  // Probes we send (and expect to recv) per clusterPod, per rack (for dead
  // rack detection)
  unordered_map<string, unordered_map<string, int>> clusterPodRackProbeCountV4;
  // Probes we send (and expect to recv) per host
  unordered_map<string, int> hostProbeCountV6;
  // Probes we send (and expect to recv) per clusterPod
  unordered_map<string, int> clusterPodProbeCountV6;
  // Probes we send (and expect to recv) per clusterPod, per rack (for dead
  // rack detection)
  unordered_map<string, unordered_map<string, int>> clusterPodRackProbeCountV6;

  auto count = 0;
  auto isV4Enabled = true;
  auto isV6Enabled = true;

  for (const auto &testPlan : testPlans) {
    if (isV4Enabled && !testPlan.target.v4.empty()) {
      hostProbeCountV4[testPlan.target.hostname] = testPlan.numPackets;
      auto clusterPod =
          clusterPodJoin(testPlan.target.cluster, testPlan.target.pod);
      clusterPodProbeCountV4[clusterPod] += testPlan.numPackets;
      clusterPodRackProbeCountV4[clusterPod][clusterPod + ":" +
                                             testPlan.target.rack] +=
          testPlan.numPackets;
      count += testPlan.numPackets;
    }
    if (isV6Enabled && !testPlan.target.v6.empty()) {
      hostProbeCountV6[testPlan.target.hostname] = testPlan.numPackets;
      auto clusterPod =
          clusterPodJoin(testPlan.target.cluster, testPlan.target.pod);
      clusterPodProbeCountV6[clusterPod] += testPlan.numPackets;
      clusterPodRackProbeCountV6[clusterPod][clusterPod + ":" +
                                             testPlan.target.rack] +=
          testPlan.numPackets;
      count += testPlan.numPackets;
    }
  }

  unordered_map<string, thrift::Target> hostToTargetMap;
  for (auto &testPlan : testPlans) {
    hostToTargetMap.emplace(testPlan.target.hostname, testPlan.target);
  }

  //
  // Submit test plans to senders
  //

  std::hash<std::string> hasher;
  for (const auto &testPlan : testPlans) {
    count += testPlan.numPackets;
    auto queueNum = hasher(testPlan.target.cluster) % numSenders;
    auto result =
        sendingQueues[queueNum]->tryPutMessageNoThrow(std::move(testPlan));
    if (!result) {
      LOG(ERROR) << "Cannot enqueue test plan";
    }
  }

  for (auto &queue : sendingQueues) {
    // In-band stop signal
    UdpTestPlan dummyPlan;
    dummyPlan.numPackets = 0;
    queue->tryPutMessageNoThrow(dummyPlan);
  }

  LOG(INFO) << "Finished dispatching all plans";

  //
  // Wait for senders to finish
  //

  for (auto &thread : senderThreads) {
    thread.join();
  }

  LOG(INFO) << "Telling all receivers to stop...";

  // tell the receivers to stop

  for (int i = 0; i < numReceivers; ++i) {
    receivers[i]->stop();
    recvThreads[i].join();
  }

  LOG(INFO) << "All receivers have stopped...";

  // combine results from all receivers

  for (auto &receiver : receivers) {
    auto &partialResults = receiver->getResults();

    LOG(INFO) << "Got partial results host size "
              << partialResults.hostResults.size() << " cluster size "
              << partialResults.clusterResults.size();

    results.hostResults.insert(results.hostResults.end(),
                               partialResults.hostResults.begin(),
                               partialResults.hostResults.end());
    results.clusterResults.insert(results.clusterResults.end(),
                                  partialResults.clusterResults.begin(),
                                  partialResults.clusterResults.end());
  }

  for (auto &result : results.hostResults) {
    if (result->metadata.ipv6) {
      result->metrics.numXmit =
          hostProbeCountV6.at(result->metadata.dst.hostname);
      hostProbeCountV6.erase(result->metadata.dst.hostname);
    } else {
      result->metrics.numXmit =
          hostProbeCountV4.at(result->metadata.dst.hostname);
      hostProbeCountV4.erase(result->metadata.dst.hostname);
    }

    result->metrics.lossRatio =
        1 - (float)result->metrics.numRecv / result->metrics.numXmit;
  }

  // Output empty results
  uint32_t now =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  // At this point, hostProbeCountXX contains the records for the
  // hosts that did never respond at all. We will try to see if
  // if those are dead hosts by using simple dead host elimination
  // logic.

  // When a host returns 0 probes, assume it is dead and remove its prob count
  // from the rack sent counters.
  // For each cluster, check how many racks have 0'd out as a result of the
  // above. Store this value as dead racks.
  // If the number of dead racks < n for a given cluster, subtract the sum of
  // probe count to hosts which returned 0 probes from the cluster prob count.

  unordered_map<string /* rack name */,
                vector<pair<string /* host name */, int>>>
      deadHostInRackProbeCountV4;
  unordered_map<string /* rack name */,
                vector<pair<string /* host name */, int>>>
      deadHostInRackProbeCountV6;

  // As the hosts which returned no results don't have a result they would not
  // be submitted to hostResults (raw). Here we itterate over the non responders
  // in order to create results for them.
  unordered_map<string, string> rackToClusterPod;
  for (const auto &iter : hostProbeCountV4) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->ignore = config_.ignore_my_samples;
    result->aggregated = false;
    result->metrics.numXmit = iter.second;
    result->metrics.lossRatio = 1;
    const auto &target = hostToTargetMap[iter.first];
    result->metadata.dst.hostname = iter.first;
    result->metadata.dst.cluster = target.cluster;
    result->metadata.dst.clustertype = target.clustertype;
    result->metadata.dst.rack = target.cluster + ":" + target.rack;
    result->metadata.dst.pod = target.pod;
    auto clusterPod =
        clusterPodJoin(result->metadata.dst.cluster, result->metadata.dst.pod);
    deadHostInRackProbeCountV4[result->metadata.dst.rack].push_back(iter);
    rackToClusterPod.emplace(result->metadata.dst.rack, clusterPod);
    result->metadata.dst.ip = target.v4;
    result->metadata.ipv6 = false;
    result->metadata.proto = kUdp;
    result->metadata.tos = qos;
    results.hostResults.push_back(std::move(result));
  }
  for (const auto &iter : hostProbeCountV6) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->ignore = config_.ignore_my_samples;
    result->aggregated = false;
    result->metrics.numXmit = iter.second;
    result->metrics.lossRatio = 1;
    const auto &target = hostToTargetMap[iter.first];
    result->metadata.dst.hostname = iter.first;
    result->metadata.dst.cluster = target.cluster;
    result->metadata.dst.clustertype = target.clustertype;
    result->metadata.dst.rack = target.cluster + ":" + target.rack;
    result->metadata.dst.pod = target.pod;
    auto clusterPod =
        clusterPodJoin(result->metadata.dst.cluster, result->metadata.dst.pod);
    deadHostInRackProbeCountV6[result->metadata.dst.rack].push_back(iter);
    rackToClusterPod.emplace(result->metadata.dst.rack, clusterPod);
    result->metadata.dst.ip = target.v6;
    result->metadata.ipv6 = true;
    result->metadata.proto = kUdp;
    result->metadata.tos = qos;
    results.hostResults.push_back(std::move(result));
  }

  // keep track of all the hosts we presume to be dead
  unordered_set<pair<string /* hostname */, bool /* is v6 */>> deadHostNames;
  // Count the number of dead racks in each clusterPod
  unordered_map<string /* cluster */,
                unordered_map<string /*rack */, int /* dead rack count */>>
      deadHostByRackV4;
  unordered_map<string /* cluster */,
                unordered_map<string /*rack */, int /* dead rack count */>>
      deadHostByRackV6;

  // Remove non responder probe counts from rack probe counts
  for (const auto &deadHostsInRack : deadHostInRackProbeCountV4) {
    auto rackName = deadHostsInRack.first;
    auto clusterPod = rackToClusterPod.at(deadHostsInRack.first);
    auto hostProbeCounts = deadHostsInRack.second;
    // Remove hostProbCounts from rackProbeCount
    for (const auto &deadHostProbes : hostProbeCounts) {
      clusterPodRackProbeCountV4[clusterPod][rackName] -= deadHostProbes.second;
      // Check if this rack has given no responses
      if (clusterPodRackProbeCountV4[clusterPod][rackName] < 1) {
        deadHostByRackV4[clusterPod][rackName] += 1;
        LOG(INFO) << "Dead Rack Detected: " << rackName << "\n";
      }
    }
  }
  // Remove non responder probe counts from cluster roll ups if number of
  // dead racks < threshold
  for (const auto &deadHostsInRack : deadHostInRackProbeCountV4) {
    auto rackName = deadHostsInRack.first;
    auto clusterPod = rackToClusterPod.at(deadHostsInRack.first);
    auto hostProbeCounts = deadHostsInRack.second;
    if (deadHostByRackV4[clusterPod].size() < config_.dead_racks_threshold) {
      // The number of dead racks in the cluster is below the threshold
      for (const auto &hostProbeCount : hostProbeCounts) {
        auto probeCount = hostProbeCount.second;
        auto hostName = hostProbeCount.first;
        deadHostNames.insert(make_pair(hostName, true));
        clusterPodProbeCountV4[clusterPod] -= probeCount;
        CHECK(clusterPodProbeCountV4[clusterPod] >= 0);
      }
    }
  }

  // Remove non responder probe counts from rack probe counts
  for (const auto &deadHostsInRack : deadHostInRackProbeCountV6) {
    auto rackName = deadHostsInRack.first;
    auto clusterPod = rackToClusterPod.at(deadHostsInRack.first);
    auto hostProbeCounts = deadHostsInRack.second;
    // Remove hostProbCounts from rackProbeCount
    for (const auto &deadHostProbes : hostProbeCounts) {
      clusterPodRackProbeCountV6[clusterPod][rackName] -= deadHostProbes.second;
      // Check if this rack has given no responses
      if (clusterPodRackProbeCountV6[clusterPod][rackName] < 1) {
        deadHostByRackV6[clusterPod][rackName] += 1;
        LOG(INFO) << "Dead Rack Detected: " << rackName << "\n";
      }
    }
  }

  // Remove non responder probe counts from cluster roll ups if number of
  // dead racks < threshold
  for (const auto &deadHostsInRack : deadHostInRackProbeCountV6) {
    auto rackName = deadHostsInRack.first;
    auto clusterPod = rackToClusterPod.at(deadHostsInRack.first);
    auto hostProbeCounts = deadHostsInRack.second;
    if (deadHostByRackV6[clusterPod].size() < config_.dead_racks_threshold) {
      // The number of dead racks in the cluster is below the threshold
      for (const auto &hostProbeCount : hostProbeCounts) {
        auto probeCount = hostProbeCount.second;
        auto hostName = hostProbeCount.first;
        deadHostNames.insert(make_pair(hostName, true));
        clusterPodProbeCountV6[clusterPod] -= probeCount;
        CHECK(clusterPodProbeCountV6[clusterPod] >= 0);
      }
    }
  }

  // mark dead hosts in results, so this could be logged in Scuba
  for (auto &result : results.hostResults) {
    if (deadHostNames.find(
            make_pair(result->metadata.dst.hostname, result->metadata.ipv6)) !=
        deadHostNames.end()) {
      result->metadata.dead = true;
      string isV6 = result->metadata.ipv6 ? "true" : "false";
      VLOG(1) << "Tagged host '" << result->metadata.dst.hostname << "' v6 is "
              << isV6 << " as being dead";
    }
  }

  // Now calculate the cleaned cluster results
  for (auto &result : results.clusterResults) {
    auto clusterPod =
        clusterPodJoin(result->metadata.dst.cluster, result->metadata.dst.pod);
    if (result->metadata.ipv6) {
      result->metrics.numXmit = clusterPodProbeCountV6.at(clusterPod);
      clusterPodProbeCountV6.erase(clusterPod);
    } else {
      result->metrics.numXmit = clusterPodProbeCountV4.at(clusterPod);
      clusterPodProbeCountV4.erase(clusterPod);
    }
    result->metrics.lossRatio =
        1 - (float)result->metrics.numRecv / result->metrics.numXmit;
  }

  for (const auto &iter : clusterPodProbeCountV4) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->ignore = config_.ignore_my_samples;
    result->aggregated = true;
    result->metrics.numXmit = iter.second;
    result->metrics.lossRatio = 1;
    auto clusterPod = clusterPodSplit(iter.first);
    result->metadata.dst.cluster = clusterPod.first;
    result->metadata.dst.clustertype =
        clusterToClusterTypeMap.at(clusterPod.first);
    result->metadata.dst.pod = clusterPod.second;
    result->metadata.ipv6 = false;
    results.clusterResults.push_back(std::move(result));
  }

  for (const auto &iter : clusterPodProbeCountV6) {
    auto result = make_shared<thrift::TestResult>();
    result->timestamp = now;
    result->ignore = config_.ignore_my_samples;
    result->aggregated = true;
    result->metrics.numXmit = iter.second;
    result->metrics.lossRatio = 1;
    auto clusterPod = clusterPodSplit(iter.first);
    result->metadata.dst.cluster = clusterPod.first;
    result->metadata.dst.clustertype =
        clusterToClusterTypeMap.at(clusterPod.first);
    result->metadata.dst.pod = clusterPod.second;
    result->metadata.ipv6 = true;
    results.clusterResults.push_back(std::move(result));
  }

  return results;
}
}
}
