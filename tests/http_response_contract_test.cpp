#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <userver/engine/single_use_event.hpp>
#include <userver/utest/http_client.hpp>
#include <userver/utest/utest.hpp>
#include <userver/utils/async.hpp>

#include <servicelib/datasink/http/userver.hpp>

namespace {

using namespace std::chrono_literals;

class Socket final {
 public:
  explicit Socket(int fd) : fd_(fd) {
    if (fd_ < 0) throw std::system_error(errno, std::generic_category());
  }
  ~Socket() { ::close(fd_); }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  int get() const { return fd_; }
 private:
  int fd_;
};

// Deliberately sends headers separately from the body. Unlike an HTTP mock
// returning a complete Response, this fixture can prove the transport boundary.
class HeldHttpServer final {
 public:
  HeldHttpServer(std::string initial, std::string tail, bool hold)
      : listener_(::socket(AF_INET, SOCK_STREAM, 0)),
        initial_(std::move(initial)), tail_(std::move(tail)), hold_(hold) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener_.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener_.get(), 1) != 0) {
      throw std::system_error(errno, std::generic_category());
    }
    socklen_t size = sizeof(address);
    if (::getsockname(listener_.get(), reinterpret_cast<sockaddr*>(&address), &size) != 0) {
      throw std::system_error(errno, std::generic_category());
    }
    port_ = ntohs(address.sin_port);
    worker_ = std::thread([this] {
      try { serve(); } catch (...) { failure_ = std::current_exception(); }
    });
  }
  ~HeldHttpServer() {
    release();
    if (worker_.joinable()) worker_.join();
  }
  std::string url() const { return "http://127.0.0.1:" + std::to_string(port_) + "/"; }
  void release() {
    std::lock_guard lock(mutex_);
    released_ = true;
    changed_.notify_all();
  }
  void finish() {
    release();
    if (worker_.joinable()) worker_.join();
    if (failure_) std::rethrow_exception(failure_);
  }

 private:
  static void sendAll(int fd, const std::string& bytes) {
    std::size_t sent = 0;
    while (sent < bytes.size()) {
      const auto count = ::send(fd, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
      if (count < 0 && errno == EINTR) continue;
      if (count <= 0) throw std::system_error(errno, std::generic_category());
      sent += static_cast<std::size_t>(count);
    }
  }
  void serve() {
    pollfd acceptReady{listener_.get(), POLLIN, 0};
    if (::poll(&acceptReady, 1, 3000) != 1) throw std::runtime_error("no HTTP connection");
    Socket peer(::accept(listener_.get(), nullptr, nullptr));
    std::string request;
    while (request.find("\r\n\r\n") == std::string::npos) {
      pollfd readable{peer.get(), POLLIN, 0};
      if (::poll(&readable, 1, 3000) != 1) throw std::runtime_error("no request headers");
      char buffer[4096];
      const auto size = ::recv(peer.get(), buffer, sizeof(buffer), 0);
      if (size <= 0) throw std::runtime_error("request closed before headers");
      request.append(buffer, static_cast<std::size_t>(size));
    }
    sendAll(peer.get(), initial_);
    if (hold_) {
      std::unique_lock lock(mutex_);
      if (!changed_.wait_for(lock, 3s, [this] { return released_; })) {
        throw std::runtime_error("test did not release HTTP body");
      }
    }
    sendAll(peer.get(), tail_);
  }

  Socket listener_;
  std::string initial_;
  std::string tail_;
  bool hold_;
  unsigned short port_{};
  std::mutex mutex_;
  std::condition_variable changed_;
  bool released_{false};
  std::exception_ptr failure_;
  std::thread worker_;
};

const std::string kHeaders =
    "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n";

// userver's installed streaming API cannot preserve the required error/header
// semantics. The explicitly accepted contract remains a buffered response.
UTEST(HttpResponseContract, AdapterBuffersBodyBeforeReturning) {
  HeldHttpServer server(kHeaders, "body", true);
  auto client = userver::utest::CreateHttpClient();
  servicelib::datasink::http::UserverClient adapter(*client);
  userver::engine::SingleUseEvent responseReady;
  auto task = userver::utils::Async("http-adapter-header-contract", [&] {
    servicelib::datasink::http::Request request;
    request.url = server.url();
    request.timeout = 2s;
    const auto response = adapter.perform(std::move(request));
    EXPECT_EQ(response.status, userver::clients::http::Status::kOk);
    EXPECT_EQ(response.body, "body");
    responseReady.Send();
  });
  const auto status = responseReady.WaitUntil(
      userver::engine::Deadline::FromDuration(300ms));
  server.release();
  task.Get();
  server.finish();
  EXPECT_EQ(status, userver::engine::FutureStatus::kTimeout)
      << "the accepted userver adapter contract buffers the response body";
}

}  // namespace
