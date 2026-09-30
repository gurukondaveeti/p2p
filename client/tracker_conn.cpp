// client/tracker_conn.cpp

#include "tracker_conn.hpp"
#include "../common/net_util.hpp"
#include "../common/common.hpp"

#include <unistd.h>

namespace p2p {

TrackerConnection::TrackerConnection(std::vector<TrackerAddr> trackers) : trackers_(std::move(trackers)) {}

void TrackerConnection::closeLocked() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

bool TrackerConnection::ensureConnectedLocked() {
    if (fd_ >= 0) return true;

    for (size_t i = 0; i < trackers_.size(); ++i) {
        size_t idx = (currentIdx_ + i) % trackers_.size();
        const TrackerAddr& t = trackers_[idx];
        Socket sock = connectWithTimeout(t.ip, t.port, 1500);
        if (!sock.valid()) continue;

        fd_ = sock.release();
        if (currentIdx_ != idx) {
            P2P_LOG_WARN("tracker-conn: switched to tracker at " + t.ip + ":" + std::to_string(t.port));
        }
        currentIdx_ = idx;

        if (hasCredentials_) {
            // Transparent re-login after failover: replay the credentials
            // this session already authenticated with once, so the user
            // doesn't have to notice their tracker died mid-session.
            MsgWriter w;
            w.putStr(userId_);
            w.putStr(passwordHash_);
            w.putStr(peerIp_);
            w.putU32(peerPort_);
            Message resp;
            if (!sendMessage(fd_, MsgType::LOGIN, w) || !recvMessage(fd_, resp) || resp.type != MsgType::RESP_OK) {
                P2P_LOG_WARN("tracker-conn: reconnected but automatic re-login failed; "
                             "you may need to log in again");
            }
        }
        return true;
    }
    return false;
}

bool TrackerConnection::request(MsgType type, const MsgWriter& payload, Message& response) {
    std::lock_guard<std::mutex> lock(mu_);

    // Two attempts: the first may run over a connection that looks alive
    // but whose peer has actually died (e.g. a crashed tracker whose kernel
    // already closed the socket) -- that failure can surface on the *read*
    // just as easily as the write, since a send into a half-dead socket
    // often still succeeds. Either failure gets the same treatment: drop
    // the connection and let ensureConnectedLocked() retry across every
    // configured tracker (it internally starts from the last-known-good
    // index, so a single retry here is enough to cover full failover).
    for (int attempt = 0; attempt < 2; ++attempt) {
        if (!ensureConnectedLocked()) return false;
        if (sendMessage(fd_, type, payload) && recvMessage(fd_, response)) return true;
        closeLocked();
    }
    return false; // both trackers unreachable -- caller can ask the user to retry
}

void TrackerConnection::onLoginSuccess(const std::string& userId, const std::string& passwordHash,
                                        const std::string& peerIp, uint16_t peerPort) {
    std::lock_guard<std::mutex> lock(mu_);
    hasCredentials_ = true;
    userId_ = userId;
    passwordHash_ = passwordHash;
    peerIp_ = peerIp;
    peerPort_ = peerPort;
}

void TrackerConnection::onLogout() {
    std::lock_guard<std::mutex> lock(mu_);
    hasCredentials_ = false;
    userId_.clear();
    passwordHash_.clear();
}

} // namespace p2p
