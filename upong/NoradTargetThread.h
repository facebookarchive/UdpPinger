/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <sys/socket.h>
#include <memory>
#include <string>

#include <folly/MPMCQueue.h>

#include "../common/Probe.h"

namespace facebook {
namespace netnorad {

const int kSockFdInvalid = -1;

/**
 * Contents/metadata of a probe message from the Net NORAD pinging agent.
 */
struct NoradProbe {
  union {
    char data[facebook::netnorad::kNoradProbeDataLen];
    facebook::netnorad::NoradProbeBody probeBody;
  };
  struct sockaddr_storage clientAddr;
  socklen_t clientAddrLen;
};

/**
 * Creates and binds UDP/IPv6 server socket to 'port'.
 *
 * @return socket descriptor of bound socket.
 *
 * @throw std::runtime_error if cannot bind any UDP/IPv6 socket to 'port'.
 */
int initUdpServer(int port);

/**
 * A listening thread that receives UDP probes from Net NORAD pinging agents.
 * Such probes are enqueued for processing/response by the sender thread.
 */
class NoradTargetReceiverThread {
public:
  NoradTargetReceiverThread(
      int sockFd, std::shared_ptr<folly::MPMCQueue<std::unique_ptr<NoradProbe>>>
                      probeQueue);

  /**
   * Listens on socket 'sockFd_' and enqueues incoming probes.
   *
   * @throw std::runtime_error if unable to setup socket.
   */
  void run();

private:
  /**
   * Receives UDP probe on sockFd_ from some pinging agent. Blocks until a probe
   * is received or recvfrom() fails (e.g., if sender thread closes the socket).
   *
   * @param probe Where to store the result.
   *
   * @throw std::runtime_error if recvfrom() fails.
   */
  void receiveProbe(NoradProbe *probe);

  /**
   * Enqueues probe for echoing by sender thread. Blocks until the queue has
   * room for the probe to be written.
   *
   * @param probe Probe to enqueue
   */
  void enqueueProbe(std::unique_ptr<NoradProbe> &&probe);

  int sockFd_ = kSockFdInvalid;
  std::shared_ptr<folly::MPMCQueue<std::unique_ptr<NoradProbe>>> probeQueue_;
};

/**
 * A sending thread that dequeues and echoes probes back to Net NORAD pinging
 * agents.
 */
class NoradTargetSenderThread {
public:
  NoradTargetSenderThread(
      int sockFd, std::shared_ptr<folly::MPMCQueue<std::unique_ptr<NoradProbe>>>
                      probeQueue);

  /**
   * Dequeues received probes and echoes them back to the client.
   */
  void run();

private:
  /**
   * Dequeues a probe from the queue. Blocks until a probe arrives on the queue.
   */
  std::unique_ptr<NoradProbe> dequeueProbe();

  /**
   * Echoes a probe back to pinging agent. Blocks until able to write to
   * socket's send buffer.
   */
  void echoProbe(NoradProbe *probe);

  int sockFd_ = kSockFdInvalid;
  std::shared_ptr<folly::MPMCQueue<std::unique_ptr<NoradProbe>>> probeQueue_;
};
}
} // namespaces
