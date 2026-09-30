// tracker/tracker_sync.cpp

#include "tracker_sync.hpp"
#include "../common/net_util.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <chrono>

namespace p2p {

TrackerSync::TrackerSync(TrackerState& state, uint32_t myNo, uint32_t peerNo,
                          std::string peerSyncIp, uint16_t peerSyncPort, uint16_t mySyncPort)
    : state_(state), myNo_(myNo), peerNo_(peerNo),
      peerSyncIp_(std::move(peerSyncIp)), peerSyncPort_(peerSyncPort), mySyncPort_(mySyncPort) {}

TrackerSync::~TrackerSync() { stop(); }

void TrackerSync::start() {
    running_ = true;
    if (myNo_ < peerNo_) {
        worker_ = std::thread([this] { connectorLoop(); });
    } else {
        worker_ = std::thread([this] { acceptorLoop(); });
    }
}

void TrackerSync::stop() {
    running_ = false;
    int fd = connFd_.exchange(-1);
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR); // unblocks a thread stuck in recv()
    if (worker_.joinable()) worker_.join();
}

void TrackerSync::tuneKeepalive(int fd) {
#ifdef __linux__
    int yes = 1, idle = 3, interval = 2, count = 3;
    ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
#else
    (void)fd;
#endif
}

void TrackerSync::connectorLoop() {
    while (running_) {
        Socket sock = connectWithTimeout(peerSyncIp_, peerSyncPort_, 1500);
        if (!sock.valid()) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }
        P2P_LOG_INFO("tracker-sync: connected to peer tracker at " + peerSyncIp_ + ":" + std::to_string(peerSyncPort_));
        tuneKeepalive(sock.fd());
        runConnection(sock.release()); // blocks until the link drops
        P2P_LOG_WARN("tracker-sync: link to peer tracker lost, will retry");
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void TrackerSync::acceptorLoop() {
    Socket listener = listenOn("0.0.0.0", mySyncPort_);
    if (!listener.valid()) {
        P2P_LOG_ERROR("tracker-sync: failed to bind sync listener on port " + std::to_string(mySyncPort_));
        return;
    }
    P2P_LOG_INFO("tracker-sync: listening for peer tracker on port " + std::to_string(mySyncPort_));

    while (running_) {
        int fd = ::accept(listener.fd(), nullptr, nullptr);
        if (fd < 0) {
            if (!running_) break;
            continue;
        }
        P2P_LOG_INFO("tracker-sync: peer tracker connected");
        tuneKeepalive(fd);
        runConnection(fd); // blocks until the link drops, then we accept() again
        P2P_LOG_WARN("tracker-sync: peer tracker link closed, awaiting reconnect");
    }
}

void TrackerSync::handshakeAndCatchup(int fd) {
    MsgWriter hello;
    hello.putU32(myNo_);
    hello.putU64(state_.lastSeqForOrigin(0));
    hello.putU64(state_.lastSeqForOrigin(1));
    if (!sendMessage(fd, MsgType::SYNC_HELLO, hello)) return;

    Message msg;
    if (!recvMessage(fd, msg) || msg.type != MsgType::SYNC_HELLO) return;

    MsgReader r(msg.payload.data(), msg.payload.size());
    r.getU32(); // peer's own tracker number (already known from config; read to advance cursor)
    uint64_t peerLastSeq[2];
    peerLastSeq[0] = r.getU64();
    peerLastSeq[1] = r.getU64();
    if (!r.ok()) return;

    // Send everything the peer is missing, for BOTH origins -- not just our
    // own -- so a tracker that lost its disk state and restarted empty can
    // be fully rehydrated from whichever peer has more history.
    for (uint32_t origin = 0; origin < 2; ++origin) {
        std::vector<OpRecord> missing = state_.recordsSince(origin, peerLastSeq[origin]);
        for (const auto& rec : missing) {
            if (!sendMessage(fd, MsgType::SYNC_OP, encodeOpRecord(rec))) return;
        }
    }
}

void TrackerSync::runConnection(int fd) {
    {
        std::lock_guard<std::mutex> lock(writeMu_);
        handshakeAndCatchup(fd);
    }
    connFd_.store(fd);

    Message msg;
    while (running_ && recvMessage(fd, msg)) {
        if (msg.type == MsgType::SYNC_OP) {
            OpRecord rec;
            if (decodeOpRecord(msg.payload.data(), msg.payload.size(), rec)) {
                state_.applyRemoteRecord(rec);
            }
        }
        // SYNC_HEARTBEAT and anything else: no action needed, receiving it
        // at all is proof the link is alive.
    }

    connFd_.store(-1);
    ::close(fd);
}

void TrackerSync::replicateAsync(const OpRecord& rec) {
    int fd = connFd_.load();
    if (fd < 0) return; // not connected right now -- next catch-up handshake will cover it
    std::lock_guard<std::mutex> lock(writeMu_);
    // Re-check under the lock: the connection may have dropped between the
    // load above and acquiring the lock.
    if (connFd_.load() != fd) return;
    sendMessage(fd, MsgType::SYNC_OP, encodeOpRecord(rec)); // best-effort
}

} // namespace p2p
