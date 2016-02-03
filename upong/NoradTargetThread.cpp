/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NoradTargetThread.h"

#include <chrono>
#include <cstring>
#include <exception>
#include <string>
#include <utility>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <folly/Format.h>
#include <folly/Memory.h>
#include <folly/Logging.h>

// Due to the way kernel headers are included, this may or may not be defined.
// Number pulled from 3.10 kernel headers.
#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif

DEFINE_int32(netnorad_socket_buffer_size, 4000000,
             "The UDP socket receive buffer size");

namespace facebook {
namespace netnorad {

using namespace facebook::netnorad;

using std::move;
using std::runtime_error;
using std::shared_ptr;
using std::strerror;
using std::string;
using std::unique_ptr;

using folly::make_unique;
using folly::MPMCQueue;
using folly::to;

/**
 * Utility function for translating client address to IP/port pair in human-
 * readable form.
 *
 * @param probe Pointer to probe containing client's socket address.
 *
 * @throw runtime_error if translation fails.
 */
static string getIpPortStr(const NoradProbe *probe) {
  char host[NI_MAXHOST], port[NI_MAXSERV];
  if (int gni_ret = getnameinfo(
          (struct sockaddr *)&probe->clientAddr, probe->clientAddrLen, host,
          sizeof(host), port, sizeof(port), NI_NUMERICHOST | NI_NUMERICSERV)) {
    throw runtime_error(to<string>("getnameinfo() failed to get IP/port: ",
                                   gai_strerror(gni_ret)));
  }
  return to<string>(host, ":", port);
}

/*
 * Uses the first UDP/IPv6 socket that can be bound successfully. Since socket
 * option IPV6_V6ONLY is false by default, an IPv6 socket can handle IPv4 probes
 * as well on a dual-stack host.
 */
int initUdpServer(int port) {
  struct addrinfo hints, *serverInfo;
  memset((void *)&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET6;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_PASSIVE;

  int sockFd = kSockFdInvalid;
  if (int gai_ret =
          getaddrinfo(nullptr, to<string>(port).c_str(), &hints, &serverInfo)) {
    throw runtime_error(
        to<string>("getaddrinfo() failed: ", gai_strerror(gai_ret)));
  }
  VLOG(1) << "getaddrinfo() success";

  for (auto iterator = serverInfo; iterator != nullptr;
       iterator = iterator->ai_next) {
    int tmpSockFd;
    if ((tmpSockFd = socket(iterator->ai_family, iterator->ai_socktype,
                            iterator->ai_protocol)) == -1) {
      LOG(WARNING) << "socket() failed: " << strerror(errno);
      continue;
    }
    VLOG(1) << "socket() success";

    int one = 1;
    if (setsockopt(tmpSockFd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) ==
        -1) {
      LOG(WARNING) << "setsocketopt() SO_REUSEPORT failed: "
                   << folly::errnoStr(errno);
    }

    if (setsockopt(tmpSockFd, SOL_SOCKET, SO_TIMESTAMPNS, &one, sizeof(one)) <
        0) {
      LOG(WARNING) << "setsockopt SO_TIMESTAMPNS failed '"
                   << folly::errnoStr(errno);
    }

    int32_t bufferSize = FLAGS_netnorad_socket_buffer_size;
    if (::setsockopt(tmpSockFd, SOL_SOCKET, SO_RCVBUF, &bufferSize,
                     sizeof(bufferSize)) == -1) {
      LOG(ERROR) << "UdpWorker: setsockopt SO_RCVBUF failed '"
                 << folly::errnoStr(errno) << "'";
    }

    if (bind(tmpSockFd, serverInfo->ai_addr, serverInfo->ai_addrlen) != -1) {
      VLOG(1) << "bind() success";
      sockFd = tmpSockFd;
      break;
    }

    LOG(WARNING) << "bind() failed: " << strerror(errno);
    close(tmpSockFd);
  }

  freeaddrinfo(serverInfo);
  if (sockFd == kSockFdInvalid) {
    throw runtime_error("no socket could be bound");
  }
  return sockFd;
}

NoradTargetReceiverThread::NoradTargetReceiverThread(
    int sockFd, shared_ptr<MPMCQueue<unique_ptr<NoradProbe>>> probeQueue)
    : sockFd_(sockFd), probeQueue_(probeQueue) {}

void NoradTargetReceiverThread::receiveProbe(NoradProbe *probe) {
  struct msghdr msg;
  struct iovec entry;

  // control data buffer
  char cbuf[kNoradProbeDataLen];

  memset(&msg, 0, sizeof(msg));

  msg.msg_iov = &entry;
  msg.msg_iovlen = 1;

  entry.iov_base = probe->data;
  entry.iov_len = kNoradProbeDataLen;

  msg.msg_control = cbuf;
  msg.msg_controllen = sizeof(cbuf);

  // prepare to receive either v4 or v6 addresses
  ::memset(&probe->clientAddr, 0, sizeof(probe->clientAddr));
  probe->clientAddrLen = sizeof(probe->clientAddr);

  msg.msg_name = &probe->clientAddr;
  msg.msg_namelen = sizeof(probe->clientAddr);

  // this is a blocking call
  int recvLen = ::recvmsg(sockFd_, &msg, 0);

  if (recvLen == 0) {
    throw runtime_error("recvmsg() returned 0 (unexpected)");
  }
  if (recvLen == -1) {
    throw runtime_error(to<string>("recvfrom() failed: ", strerror(errno)));
  }
  if (recvLen < kNoradProbeDataLen) {
    LOG(ERROR) << "Received " << recvLen << " bytes expected "
               << kNoradProbeDataLen;
    throw runtime_error("recvmsg() truncated probe (unexpected)");
  }

  struct cmsghdr *cmsg{nullptr};
  struct timespec *stamp{nullptr};

  for (cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
    switch (cmsg->cmsg_level) {
    case SOL_SOCKET:
      switch (cmsg->cmsg_type) {
      case SO_TIMESTAMPNS: {
        stamp = (struct timespec *)CMSG_DATA(cmsg);
        break;
      }
      }
      break;
    }
  } // for

  // kernel returned the timestamp
  if (stamp) {
    probe->probeBody.targetRcvdTime =
        htonl(stamp->tv_sec * 1000000 + stamp->tv_nsec / 1000);
  } else {
    FB_LOG_EVERY_MS(INFO, 1000) << "Kernel timestamp not available";

    // use system time to approximate
    probe->probeBody.targetRcvdTime =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
  }
}

void NoradTargetReceiverThread::enqueueProbe(unique_ptr<NoradProbe> &&probe) {
  probeQueue_->blockingWrite(move(probe));
}

void NoradTargetReceiverThread::run() {
  while (1) {
    auto probe = make_unique<NoradProbe>();
    try {
      receiveProbe(probe.get());
    } catch (const runtime_error &e) {
      string clientIpPort;
      try {
        clientIpPort = getIpPortStr(probe.get());
      } catch (const runtime_error &e) {
        clientIpPort = "unknown client";
      }
      LOG(ERROR) << "receiveProbe() from " << clientIpPort
                 << " failed: " << folly::exceptionStr(e);
      continue;
    }
    enqueueProbe(move(probe));
  }

  LOG(INFO) << "Finished run()";
}

NoradTargetSenderThread::NoradTargetSenderThread(
    int sockFd, shared_ptr<MPMCQueue<unique_ptr<NoradProbe>>> probeQueue)
    : sockFd_(sockFd), probeQueue_(probeQueue) {}

unique_ptr<NoradProbe> NoradTargetSenderThread::dequeueProbe() {
  unique_ptr<NoradProbe> probe;
  probeQueue_->blockingRead(probe);
  return probe;
}

void NoradTargetSenderThread::echoProbe(NoradProbe *probe) {
  uint32_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();

  probe->probeBody.targetRespTime = htonl(now);

  FB_LOG_EVERY_MS(INFO, 1000) << folly::sformat(
      "Probe originated at {}, received at {} responded at {}, adjusted by {}",
      ntohl(probe->probeBody.pingerSentTime),
      ntohl(probe->probeBody.targetRcvdTime),
      ntohl(probe->probeBody.targetRespTime),
      ntohl(probe->probeBody.targetRespTime) -
          ntohl(probe->probeBody.targetRcvdTime));

  // prepare the message for sending
  struct msghdr msg;
  struct cmsghdr *cmsg;
  struct iovec entry;

  // control data buffer, hardcoded - we only store tclass there
  char cbuf[256];

  memset(&msg, 0, sizeof(msg));
  memset(cbuf, 0, sizeof(cbuf));
  cmsg = (struct cmsghdr *)cbuf;

  msg.msg_iov = &entry;
  msg.msg_iovlen = 1;

  entry.iov_base = probe->data;
  entry.iov_len = kNoradProbeDataLen;

  // set the ancilliary data - tclass in our case
  // notice that tclass value must be int, hence copy
  int tclass = probe->probeBody.tclass;
  msg.msg_control = cmsg;
  msg.msg_controllen = CMSG_SPACE(sizeof(tclass));

  cmsg->cmsg_len = CMSG_LEN(sizeof(tclass));
  cmsg->cmsg_level = IPPROTO_IPV6;
  cmsg->cmsg_type = IPV6_TCLASS;

  ::memcpy(CMSG_DATA(cmsg), &tclass, sizeof(tclass));

  msg.msg_name = &probe->clientAddr;
  msg.msg_namelen = sizeof(probe->clientAddr);

  // this is a blocking call
  int sendLen = sendmsg(sockFd_, &msg, 0);

  if (sendLen == -1) {
    throw runtime_error(to<string>("sendmsg() error: ", strerror(errno)));
  }
  if (sendLen < kNoradProbeDataLen) {
    throw runtime_error("sendto() didn't send entire datagram (unexpected)");
  }
}

void NoradTargetSenderThread::run() {
  while (1) {
    unique_ptr<NoradProbe> probe = dequeueProbe();
    try {
      echoProbe(probe.get());
    } catch (const runtime_error &e) {
      string clientIpPort;
      try {
        clientIpPort = getIpPortStr(probe.get());
      } catch (const runtime_error &e) {
        clientIpPort = "unknown client";
      }
      LOG(ERROR) << "echoProbe() to " << clientIpPort
                 << " failed: " << folly::exceptionStr(e);
      continue;
    }
  }

  shutdown(sockFd_, SHUT_RDWR);
  LOG(INFO) << "Finished run()";
}
}
} // namespaces
