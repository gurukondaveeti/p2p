// common/net_util.hpp
//
// Thin wrappers around POSIX sockets: a RAII socket handle, loops that
// guarantee "all bytes sent / all bytes received" (TCP gives no such
// guarantee per call), and connect/listen helpers with sane timeouts.
//
// Nothing here is P2P-specific -- see protocol.hpp for the message framing
// built on top of these primitives.

#pragma once

#include <string>
#include <cstdint>

namespace p2p {

// Owns a socket file descriptor and closes it on destruction. Move-only.
class Socket {
public:
    Socket() : fd_(-1) {}
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket() { close(); }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    int fd() const { return fd_; }
    bool valid() const { return fd_ >= 0; }
    void close();
    int release() { int f = fd_; fd_ = -1; return f; }

private:
    int fd_;
};

// Sends exactly `len` bytes, looping over short writes. Returns false on
// any socket error (including a broken pipe).
bool sendAll(int fd, const void* data, size_t len);

// Receives exactly `len` bytes, looping over short reads. Returns false on
// error *or* on a clean peer disconnect before all bytes arrived -- either
// way the caller should treat the connection as dead.
bool recvAll(int fd, void* data, size_t len);

// Starts listening on ip:port (ip may be "0.0.0.0" to bind all interfaces).
// Returns an invalid Socket on failure (check .valid()).
Socket listenOn(const std::string& ip, uint16_t port, int backlog = 32);

// Connects to host:port with a wall-clock timeout in milliseconds so a dead
// peer (e.g. a downed tracker) fails fast instead of hanging the caller.
// Returns an invalid Socket on failure or timeout.
Socket connectWithTimeout(const std::string& host, uint16_t port, int timeoutMs);

// Splits "ip:port" into its parts. Returns false if malformed.
bool splitHostPort(const std::string& hostPort, std::string& host, uint16_t& port);

// Returns the local IP address this process would use to reach `peerHost`
// (used so a client can tell the tracker/peers a routable address for
// itself rather than hardcoding "127.0.0.1").
std::string detectLocalIp(const std::string& peerHost);

} // namespace p2p
