/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include <folly/ScopeGuard.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/AsyncSocketBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/EventBase.h>
#include <folly/SocketAddress.h>

#include <memory>

namespace facebook {
namespace netnorad {

/**
 * UDP socket
 */
class AsyncUDPSocket : public folly::EventHandler {
public:
  enum class FDOwnership { OWNS, SHARED };

  class ReadCallback {
  public:
    /**
     * Invoked when the socket becomes readable and we want buffer
     * to write to. We pass the pointer to a properly filled
     * struct msghdr*, which among others defines the receive
     * and control buffer. See recvmsg(2) for more details.
     *
     */
    virtual void getMessageHeader(struct msghdr **msg) noexcept = 0;

    /**
     * Invoked when a new datagraom is available on the socket. `len`
     * is the number of bytes read. The rest of the metadata is set
     * in the struct msghdr previously passed to the socket.
     */
    virtual void onMessageAvailable(size_t len) noexcept = 0;

    /**
     * Invoked when there is an error reading from the socket.
     *
     * NOTE: Since UDP is connectionless, you can still read from the socket.
     *       But you have to re-register readCallback yourself after
     *       onReadError.
     */
    virtual void
    onReadError(const folly::AsyncSocketException &ex) noexcept = 0;

    /**
     * Invoked when socket is closed and a read callback is registered.
     */
    virtual void onReadClosed() noexcept = 0;

    virtual ~ReadCallback() = default;
  };

  /**
   * Create a new UDP socket that will run in the
   * given eventbase
   */
  explicit AsyncUDPSocket(folly::EventBase *evb);
  ~AsyncUDPSocket();

  /**
   * Returns the address server is listening on
   */
  virtual const folly::SocketAddress &address() const {
    CHECK_NE(-1, fd_) << "Server not yet bound to an address";
    return localAddress_;
  }

  /**
   * Bind the socket to the following address. If port is not
   * set in the `address` an ephemeral port is chosen and you can
   * use `address()` method above to get it after this method successfully
   * returns.
   */
  virtual void bind(const folly::SocketAddress &address);

  /**
   * Use an already bound file descriptor. You can either transfer ownership
   * of this FD by using ownership = FDOwnership::OWNS or share it using
   * FDOwnership::SHARED. In case FD is shared, it will not be `close`d in
   * destructor.
   */
  virtual void setFD(int fd, FDOwnership ownership);

  /**
   * Send the data in buffer to destination. Returns the return code from
   * ::sendmsg.
   */
  virtual ssize_t write(const folly::SocketAddress &address,
                        const std::unique_ptr<folly::IOBuf> &buf);

  /**
   * Send data in iovec to destination. Returns the return code from sendmsg.
   */
  virtual ssize_t writev(const folly::SocketAddress &address,
                         const struct iovec *vec, size_t veclen);

  /**
   * Start reading datagrams
   */
  virtual void resumeRead(ReadCallback *cob);

  /**
   * Pause reading datagrams
   */
  virtual void pauseRead();

  /**
   * Stop listening on the socket.
   */
  virtual void close();

  /**
   * Get internal FD used by this socket
   */
  virtual int getFD() const {
    CHECK_NE(-1, fd_) << "Need to bind before getting FD out";
    return fd_;
  }

  /**
   * Set reuse port mode to call bind() on the same address multiple times
   */
  virtual void setReusePort(bool reusePort) { reusePort_ = reusePort; }

  /**
   * Set SO_REUSEADDR flag on the socket. Default is ON.
   */
  virtual void setReuseAddr(bool reuseAddr) { reuseAddr_ = reuseAddr; }

private:
  AsyncUDPSocket(const AsyncUDPSocket &) = delete;
  AsyncUDPSocket &operator=(const AsyncUDPSocket &) = delete;

  // EventHandler
  void handlerReady(uint16_t events) noexcept;

  void handleRead() noexcept;
  bool updateRegistration() noexcept;

  folly::EventBase *eventBase_;
  folly::SocketAddress localAddress_;

  int fd_;
  FDOwnership ownership_;

  // Non-null only when we are reading
  ReadCallback *readCallback_;

  bool reuseAddr_{true};
  bool reusePort_{false};
};
}
} // Namespace
