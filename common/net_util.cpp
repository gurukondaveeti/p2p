// common/net_util.cpp

#include "net_util.hpp"
#include "common.hpp"

#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>

namespace p2p {

void Socket::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool sendAll(int fd, const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false; // shouldn't happen for send(), but be safe
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool recvAll(int fd, void* data, size_t len) {
    uint8_t* p = static_cast<uint8_t*>(data);
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, p + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false; // peer closed the connection
        got += static_cast<size_t>(n);
    }
    return true;
}

Socket listenOn(const std::string& ip, uint16_t port, int backlog) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return Socket();

    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (ip.empty() || ip == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return Socket();
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return Socket();
    }
    if (::listen(fd, backlog) < 0) {
        ::close(fd);
        return Socket();
    }
    return Socket(fd);
}

Socket connectWithTimeout(const std::string& host, uint16_t port, int timeoutMs) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return Socket();

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        // Not a raw IP -- try resolving it as a hostname.
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || !res) {
            ::close(fd);
            return Socket();
        }
        addr.sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
        ::freeaddrinfo(res);
    }

    // Non-blocking connect so we can bound the wait with select().
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        ::close(fd);
        return Socket();
    }

    if (rc != 0) {
        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(fd, &wfds);
        timeval tv;
        tv.tv_sec = timeoutMs / 1000;
        tv.tv_usec = (timeoutMs % 1000) * 1000;

        rc = ::select(fd + 1, nullptr, &wfds, nullptr, &tv);
        if (rc <= 0) {
            ::close(fd); // timeout or error
            return Socket();
        }
        int soErr = 0;
        socklen_t len = sizeof(soErr);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soErr, &len);
        if (soErr != 0) {
            ::close(fd);
            return Socket();
        }
    }

    // Restore blocking mode -- the rest of the protocol code assumes
    // blocking sockets with recvAll/sendAll doing the looping.
    ::fcntl(fd, F_SETFL, flags);

    int yes = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    return Socket(fd);
}

bool splitHostPort(const std::string& hostPort, std::string& host, uint16_t& port) {
    size_t pos = hostPort.rfind(':');
    if (pos == std::string::npos || pos == 0 || pos == hostPort.size() - 1) return false;
    host = hostPort.substr(0, pos);
    std::string portStr = hostPort.substr(pos + 1);
    for (char c : portStr) {
        if (c < '0' || c > '9') return false;
    }
    int p = std::atoi(portStr.c_str());
    if (p <= 0 || p > 65535) return false;
    port = static_cast<uint16_t>(p);
    return true;
}

std::string detectLocalIp(const std::string& peerHost) {
    // Trick: open a UDP socket and "connect" it (no packets sent for UDP
    // connect) to the peer; the kernel picks the local address/route that
    // would be used, which we can then read back. Falls back to loopback.
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return "127.0.0.1";

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9); // discard port, never actually sent to
    if (::inet_pton(AF_INET, peerHost.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return "127.0.0.1";
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return "127.0.0.1";
    }
    sockaddr_in local{};
    socklen_t len = sizeof(local);
    std::string result = "127.0.0.1";
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &len) == 0) {
        char buf[INET_ADDRSTRLEN];
        if (::inet_ntop(AF_INET, &local.sin_addr, buf, sizeof(buf))) {
            result = buf;
        }
    }
    ::close(fd);
    return result;
}

} // namespace p2p
