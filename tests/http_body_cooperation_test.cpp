#include <userver/clients/http/client.hpp>
#include <userver/clients/http/request.hpp>
#include <userver/clients/http/response.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/utest/http_client.hpp>
#include <userver/utest/utest.hpp>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace {
using namespace std::chrono_literals;

struct Socket final {
  int fd{-1};
  ~Socket() {
    if (fd >= 0) ::close(fd);
  }
};

// Only the peer uses a native thread. The HTTP client and the second test task
// share one userver worker; the peer cannot send the body until that task runs.
class DelayedBodyPeer final {
 public:
  explicit DelayedBodyPeer(userver::engine::SingleConsumerEvent& headersSent)
      : headersSent_(headersSent) {
    listener_.fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_.fd < 0) throw std::runtime_error("socket failed");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener_.fd, reinterpret_cast<sockaddr*>(&address),
               sizeof(address)) != 0 ||
        ::listen(listener_.fd, 1) != 0) {
      throw std::runtime_error("listen failed");
    }
    socklen_t size = sizeof(address);
    if (::getsockname(listener_.fd, reinterpret_cast<sockaddr*>(&address),
                      &size) != 0) {
      throw std::runtime_error("getsockname failed");
    }
    url_ = "http://127.0.0.1:" + std::to_string(ntohs(address.sin_port)) + "/";
    worker_ = std::thread([this] { serve(); });
  }

  ~DelayedBodyPeer() {
    releaseBody();
    if (worker_.joinable()) worker_.join();
  }

  const std::string& url() const { return url_; }

  void releaseBody() {
    {
      std::lock_guard lock(mutex_);
      released_ = true;
    }
    ready_.notify_one();
  }

  void finish() {
    worker_.join();
    if (error_) std::rethrow_exception(error_);
  }

 private:
  static void sendAll(int socket, std::string_view data) {
    while (!data.empty()) {
      const auto sent = ::send(socket, data.data(), data.size(), MSG_NOSIGNAL);
      if (sent <= 0) throw std::runtime_error("send failed");
      data.remove_prefix(static_cast<std::size_t>(sent));
    }
  }

  void serve() noexcept {
    try {
      pollfd event{listener_.fd, POLLIN, 0};
      if (::poll(&event, 1, 5000) <= 0) {
        throw std::runtime_error("accept timeout");
      }
      Socket connection{::accept(listener_.fd, nullptr, nullptr)};
      if (connection.fd < 0) throw std::runtime_error("accept failed");
      const timeval timeout{3, 0};
      if (::setsockopt(connection.fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout)) != 0) {
        throw std::runtime_error("setsockopt failed");
      }
      std::string request;
      while (request.find("\r\n\r\n") == std::string::npos) {
        char buffer[1024];
        const auto received = ::recv(connection.fd, buffer, sizeof(buffer), 0);
        if (received <= 0) throw std::runtime_error("request read failed");
        request.append(buffer, static_cast<std::size_t>(received));
      }
      sendAll(connection.fd,
              "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n");
      headersSent_.Send();
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [this] { return released_; });
      }
      sendAll(connection.fd, "body");
    } catch (...) {
      error_ = std::current_exception();
      headersSent_.Send();
    }
  }

  Socket listener_;
  userver::engine::SingleConsumerEvent& headersSent_;
  std::string url_;
  std::mutex mutex_;
  std::condition_variable ready_;
  bool released_{};
  std::exception_ptr error_;
  std::thread worker_;
};

UTEST_MT(HttpBodyCooperation, BufferedPerformDoesNotBlockOnlyWorker, 1) {
  userver::engine::SingleConsumerEvent headersSent;
  DelayedBodyPeer peer(headersSent);
  auto client = userver::utest::CreateHttpClient();
  auto response = userver::engine::AsyncNoTracing([&] {
    return client->CreateRequest().get(peer.url()).timeout(3s).perform();
  });
  EXPECT_TRUE(headersSent.WaitForEventFor(2s));
  auto progress = userver::engine::AsyncNoTracing([&] {
    // Body is still withheld. A blocking perform() would prevent this task
    // from running until the HTTP request timed out on the single worker.
    const bool waitingForBody = !response.IsFinished();
    peer.releaseBody();
    return waitingForBody;
  });
  EXPECT_TRUE(progress.Get());
  EXPECT_EQ(response.Get()->body(), "body");
  peer.finish();
}
}  // namespace
