/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#ifndef NETNORAD_UDP_PINGER_H
#define NETNORAD_UDP_PINGER_H

#include <set>

#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/NotificationQueue.h>
#include <folly/stats/Histogram.h>

#include "AsyncUDPSocket.h"
#include "../common/Probe.h"
#include "gen-cpp/Pinger_types.h"

namespace facebook {
namespace netnorad {

//
// The UDP header struct we use to inject packets
//
struct UdpHeader {
  uint16_t srcPort{0};
  uint16_t dstPort{0};
  uint16_t length{0};
  uint16_t checkSum{0};
};

//
// Test plan for the sender - how many probes to send,
// and how many have been sent so far
//

struct UdpTestPlan {
  thrift::Target target;

  // how many packets to send
  int numPackets{0};

  // how many packets have been sent so far for V4
  int packetsSentV4{0};

  // how many packets sent for V6
  int packetsSentV6{0};
};

struct UdpTestResults {
  std::vector<std::shared_ptr<thrift::TestResult>> hostResults;
  std::vector<std::shared_ptr<thrift::TestResult>> clusterResults;
};

struct ReceiveProbe {
  uint32_t rtt; // In microseconds
  folly::SocketAddress remoteAddress;
};

class Histogram : public folly::Histogram<uint32_t> {
public:
  Histogram(uint32_t bucketSize, uint32_t min, uint32_t max)
      : folly::Histogram<uint32_t>(bucketSize, min, max) {}
  void addValue(uint32_t value) {
    folly::Histogram<uint32_t>::addValue(value);
    average_ =
        average_ * ((float)count_ / (count_ + 1)) + (float)value / (count_ + 1);
    ++count_;
  }
  double getAverage(void) const { return average_; }
  uint64_t computeTotalCount(void) const { return count_; }

private:
  double average_ = 0;
  uint64_t count_ = 0;
};

//
// UdpSender
//

class UdpSender {
public:
  UdpSender(const thrift::Config &config, int qos, int senderId, int numSenders,
            uint32_t signature, folly::IPAddress srcV4, folly::IPAddress srcV6,
            const std::set<int> &missingPortsV4,
            const std::set<int> &missingPortsV6,
            std::shared_ptr<folly::NotificationQueue<UdpTestPlan>> inputQueue);

  void run();

private:
  UdpSender(const UdpSender &) = delete;
  UdpSender &operator=(const UdpSender &) = delete;

  //
  // Class invariants
  //

  // global udp pinger config object
  const thrift::Config config_;

  // the QoS value to use in probes
  const int qos_;

  // the unique identifier of this sender
  const int senderId_;

  // total number of senders
  const int numSenders_;

  // signature to embed in our probes
  const uint32_t signature_;

  // source v4 address for pinging
  const folly::IPAddress srcV4_;

  // srouce v6 address for pinging
  const folly::IPAddress srcV6_;

  // the set of source v4 ports we can't use
  const std::set<int> missingPortsV4_;

  // the set of source v6 ports we can't use
  const std::set<int> missingPortsV6_;

  // queue to reade test plans from
  std::shared_ptr<folly::NotificationQueue<UdpTestPlan>> inputQueue_;

  // raw v4 socket
  int socketV4_{-1};

  // raw v6 socket
  int socketV6_{-1};

  //
  // Run-time state
  //

  // the callback we use to accumulate input data and send pings
  std::unique_ptr<folly::NotificationQueue<UdpTestPlan>::Consumer,
                  folly::DelayedDestruction::Destructor> sendingConsumer_;

  // the test plan accumulated from input queue
  std::vector<UdpTestPlan> testPlans_;

  // mapping of the string address to its parsed binary repr
  std::unordered_map<std::string, struct sockaddr_storage> addressMap_;

  //
  // IO stuff
  //
  folly::EventBase evb_;

  // the buffer we use to send probes
  uint8_t buf_[kNoradProbeDataLen + sizeof(UdpHeader)];

  //
  // Private Methods
  //

  // set the callback that accumulates incoming targets
  void prepareConsumer();

  // runs over the test-plans and pre-builds addresses
  void buildAddressMap();

  // ping all accumulated targets
  void pingAllTargets();

  // close all relevant sockets
  void closeSockets();
};

//
// UdpReceiver runs an event loop that dispatches two type of
//
// callbacks:
// (1) coming from UDP socket read events. Those are processes
//     and passed other to a receive queue that must handle them
// (2) notification queue that receives samples from all other
//     read callbacks (running in all receive threads) and
//     accumulates them in internal storage
//
// Receiver accumulates samples for N clusters allocated to it
// This means that other threads the receive samples for the
// clusters belonging to this receiver are responsible for
// redirecting it here
//
class UdpReceiver final : public AsyncUDPSocket::ReadCallback {
public:
  UdpReceiver(
      const thrift::Config &config, uint32_t signature, int receiverId,
      std::vector<std::shared_ptr<folly::NotificationQueue<ReceiveProbe>>>
          recvQueues,
      const std::unordered_map<folly::IPAddress, thrift::Target> &ipToTargetMap,
      const std::unordered_map<std::string, std::string>
          &clusterToClusterTypeMap);
  virtual ~UdpReceiver() {}

  //
  // AsyncUDPSocket::ReadCallback methods
  //
  void onMessageAvailable(size_t len) noexcept override;
  void getMessageHeader(struct msghdr **msg) noexcept override;
  void onReadClosed() noexcept override {}
  void onReadError(const folly::AsyncSocketException &ex) noexcept override {
    LOG(ERROR) << "UdpReadCallback error: " << folly::exceptionStr(ex);
  }

  // called from main thread to wait until we finish binding our sockets
  void waitForSocketsToBind();

  // invokes the main receiver loop
  void run(int qos);

  // stops our event loop
  void stop();

  // called on stopped receiver retrieve results
  const UdpTestResults &getResults();

private:
  // non-copyable object
  UdpReceiver(const UdpReceiver &) = delete;
  UdpReceiver &operator=(const UdpReceiver &) = delete;

  //
  // Class invariants
  //

  // the global pinger config
  const thrift::Config &config_;

  // the signature we expect in received probes
  const uint32_t signature_;

  // our number in receiver set
  const int receiverId_;

  const std::vector<std::shared_ptr<folly::NotificationQueue<ReceiveProbe>>>
      recvQueues_;

  // read-only lookup tables to pull various meta-data
  const std::unordered_map<folly::IPAddress, thrift::Target> &ipToTargetMap_;
  const std::unordered_map<std::string, std::string> &clusterToClusterTypeMap_;

  // the hash function that we use to load-balance among queues
  std::hash<std::string> hasher_;

  //
  // Runtime state
  //

  // set to true once the receiver is done binding the sockets
  // used to synchronize with the main thread
  std::atomic<bool> socketsAreBound_{false};

  // the consumer that will be processing samples read from UDP sockets
  // this guy might be invoked when any socket receives a message
  std::unique_ptr<folly::NotificationQueue<ReceiveProbe>::Consumer,
                  folly::DelayedDestruction::Destructor> receivingConsumer_;

  // this caches the cluster name to queue id
  std::unordered_map<std::string, int> clusterToQueueId_;

  // histograms that we build for hosts and clusters
  std::shared_ptr<std::unordered_map<folly::IPAddress, Histogram>>
      hostHistogramsV4_;
  std::shared_ptr<std::unordered_map<std::string, Histogram>>
      clusterHistogramsV4_;
  std::shared_ptr<std::unordered_map<folly::IPAddress, Histogram>>
      hostHistogramsV6_;
  std::shared_ptr<std::unordered_map<std::string, Histogram>>
      clusterHistogramsV6_;

  // the results of the tests for this receiver
  UdpTestResults results_;

  //
  // IO related structures
  //

  // the event loop running in the thread
  folly::EventBase evb_;

  // V4 sockets to receive data on
  std::vector<std::shared_ptr<AsyncUDPSocket>> socketsV4_;

  // V6 sockets to receive data on
  std::vector<std::shared_ptr<AsyncUDPSocket>> socketsV6_;

  // the data buffer
  char readBuf_[kNoradProbeDataLen];

  // the control message buffer
  char ctrlBuf_[kNoradProbeDataLen];

  // the message header to receive into
  struct msghdr msg_;

  // the IO vector for data to be received with recvmsg
  struct iovec entry_;

  // the address of the peer who sent us the message
  sockaddr_storage addrStorage_;

  //
  // Helper methods
  //

  // consume one probe
  void consumeMessage(ReceiveProbe &&message) noexcept;

  // close our sockets
  void closeSockets();

  void summarizeResults(int qos);
};

// Interface with the rest of agent
// Agent will call run() to fetch results synchronously
class UdpPinger {
public:
  UdpPinger(const thrift::Config &config, folly::IPAddress srcV4,
            folly::IPAddress srcV6);
  UdpTestResults run(const std::vector<UdpTestPlan> &testPlans, int qos);

private:
  // the global configuration object
  thrift::Config config_;

  // source v4 address used for pinging
  folly::IPAddress srcV4_;

  // source v6 address used for pinging
  folly::IPAddress srcV6_;
};
}
}

#endif
