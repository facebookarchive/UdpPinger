/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "AsyncUDPSocket.h"

#include <folly/io/async/EventBase.h>
#include <folly/Likely.h>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

// Due to the way kernel headers are included, this may or may not be defined.
// Number pulled from 3.10 kernel headers.
#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif

using namespace folly;

namespace facebook {
namespace netnorad {

AsyncUDPSocket::AsyncUDPSocket(EventBase *evb)
    : EventHandler(CHECK_NOTNULL(evb)), eventBase_(evb), fd_(-1),
      readCallback_(nullptr) {
  DCHECK(evb->isInEventBaseThread());
}

AsyncUDPSocket::~AsyncUDPSocket() {
  if (fd_ != -1) {
    close();
  }
}

void AsyncUDPSocket::bind(const folly::SocketAddress &address) {
  int socket = ::socket(address.getFamily(), SOCK_DGRAM, IPPROTO_UDP);
  if (socket == -1) {
    throw AsyncSocketException(AsyncSocketException::NOT_OPEN,
                               "error creating async udp socket", errno);
  }

  auto g = folly::makeGuard([&] { ::close(socket); });

  // put the socket in non-blocking mode
  int ret = fcntl(socket, F_SETFL, O_NONBLOCK);
  if (ret != 0) {
    throw AsyncSocketException(AsyncSocketException::NOT_OPEN,
                               "failed to put socket in non-blocking mode",
                               errno);
  }

  if (reuseAddr_) {
    // put the socket in reuse mode
    int value = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value)) !=
        0) {
      throw AsyncSocketException(AsyncSocketException::NOT_OPEN,
                                 "failed to put socket in reuse mode", errno);
    }
  }

  if (reusePort_) {
    // put the socket in port reuse mode
    int value = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_REUSEPORT, &value, sizeof(value)) !=
        0) {
      ::close(socket);
      throw AsyncSocketException(AsyncSocketException::NOT_OPEN,
                                 "failed to put socket in reuse_port mode",
                                 errno);
    }
  }

  // If we're using IPv6, make sure we don't accept V4-mapped connections
  if (address.getFamily() == AF_INET6) {
    int flag = 1;
    if (::setsockopt(socket, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag))) {
      throw AsyncSocketException(AsyncSocketException::NOT_OPEN,
                                 "Failed to set IPV6_V6ONLY", errno);
    }
  }

  // bind to the address
  sockaddr_storage addrStorage;
  address.getAddress(&addrStorage);
  sockaddr *saddr = reinterpret_cast<sockaddr *>(&addrStorage);
  if (::bind(socket, saddr, address.getActualSize()) != 0) {
    throw AsyncSocketException(
        AsyncSocketException::NOT_OPEN,
        "failed to bind the async udp socket for:" + address.describe(), errno);
  }

  // success
  g.dismiss();
  fd_ = socket;
  ownership_ = FDOwnership::OWNS;

  // attach to EventHandler
  EventHandler::changeHandlerFD(fd_);

  if (address.getPort() != 0) {
    localAddress_ = address;
  } else {
    localAddress_.setFromLocalAddress(fd_);
  }
}

void AsyncUDPSocket::setFD(int fd, FDOwnership ownership) {
  CHECK_EQ(-1, fd_) << "Already bound to another FD";

  fd_ = fd;
  ownership_ = ownership;

  EventHandler::changeHandlerFD(fd_);
  localAddress_.setFromLocalAddress(fd_);
}

ssize_t AsyncUDPSocket::write(const folly::SocketAddress &address,
                              const std::unique_ptr<folly::IOBuf> &buf) {
  // UDP's typical MTU size is 1500, so high number of buffers
  //   really do not make sense. Optimze for buffer chains with
  //   buffers less than 16, which is the highest I can think of
  //   for a real use case.
  iovec vec[16];
  size_t iovec_len = buf->fillIov(vec, sizeof(vec) / sizeof(vec[0]));
  if (UNLIKELY(iovec_len == 0)) {
    buf->coalesce();
    vec[0].iov_base = const_cast<uint8_t *>(buf->data());
    vec[0].iov_len = buf->length();
    iovec_len = 1;
  }

  return writev(address, vec, iovec_len);
}

ssize_t AsyncUDPSocket::writev(const folly::SocketAddress &address,
                               const struct iovec *vec, size_t iovec_len) {
  CHECK_NE(-1, fd_) << "Socket not yet bound";

  sockaddr_storage addrStorage;
  address.getAddress(&addrStorage);

  struct msghdr msg;
  msg.msg_name = reinterpret_cast<void *>(&addrStorage);
  msg.msg_namelen = address.getActualSize();
  msg.msg_iov = const_cast<struct iovec *>(vec);
  msg.msg_iovlen = iovec_len;
  msg.msg_control = nullptr;
  msg.msg_controllen = 0;
  msg.msg_flags = 0;

  return ::sendmsg(fd_, &msg, 0);
}

void AsyncUDPSocket::resumeRead(ReadCallback *cob) {
  CHECK(!readCallback_) << "Another read callback already installed";
  CHECK_NE(-1, fd_) << "UDP server socket not yet bind to an address";

  readCallback_ = CHECK_NOTNULL(cob);
  if (!updateRegistration()) {
    AsyncSocketException ex(AsyncSocketException::NOT_OPEN,
                            "failed to register for accept events");

    readCallback_ = nullptr;
    cob->onReadError(ex);
    return;
  }
}

void AsyncUDPSocket::pauseRead() {
  // It is ok to pause an already paused socket
  readCallback_ = nullptr;
  updateRegistration();
}

void AsyncUDPSocket::close() {
  DCHECK(eventBase_->isInEventBaseThread());

  if (readCallback_) {
    auto cob = readCallback_;
    readCallback_ = nullptr;

    cob->onReadClosed();
  }

  // Unregister any events we are registered for
  unregisterHandler();

  if (fd_ != -1 && ownership_ == FDOwnership::OWNS) {
    ::close(fd_);
  }

  fd_ = -1;
}

void AsyncUDPSocket::handlerReady(uint16_t events) noexcept {
  if (events & EventHandler::READ) {
    DCHECK(readCallback_);
    handleRead();
  }
}

void AsyncUDPSocket::handleRead() noexcept {
  void *buf{nullptr};
  size_t len{0};

  struct msghdr *msg{nullptr};

  readCallback_->getMessageHeader(&msg);

  if (msg == nullptr) {
    AsyncSocketException ex(
        AsyncSocketException::BAD_ARGS,
        "AsyncUDPSocket::getMessageHeader() returned empty header");

    auto cob = readCallback_;
    readCallback_ = nullptr;

    cob->onReadError(ex);
    updateRegistration();
    return;
  }

  ssize_t bytesRead = ::recvmsg(fd_, msg, MSG_DONTWAIT);

  if (bytesRead >= 0) {
    readCallback_->onMessageAvailable(bytesRead);
  } else {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      // No data could be read without blocking the socket
      return;
    }

    AsyncSocketException ex(AsyncSocketException::INTERNAL_ERROR,
                            "::recvmsg() failed", errno);

    // In case of UDP we can continue reading from the socket
    // even if the current request fails. We notify the user
    // so that he can do some logging/stats collection if he wants.
    auto cob = readCallback_;
    readCallback_ = nullptr;

    cob->onReadError(ex);
    updateRegistration();
  }
}

bool AsyncUDPSocket::updateRegistration() noexcept {
  uint16_t flags = NONE;

  if (readCallback_) {
    flags |= READ;
  }

  return registerHandler(flags | PERSIST);
}
}
} // Namespace
