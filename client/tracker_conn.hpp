// client/tracker_conn.hpp
//
// Owns the client's single persistent session connection to "a" tracker
// (per the assignment, it doesn't matter which -- both are kept
// consistent). Provides transparent failover: if the active tracker dies
// mid-session, the next request silently reconnects to the other tracker
// and -- if the user was logged in -- replays the login on their behalf,
// so from the REPL's point of view a tracker crash just looks like a brief
// pause rather than a dropped session. This directly implements the
// spec's "the system should continue operating as long as at least one
// tracker remains online" from the client's side of that contract.
//
// All requests are serialized through one mutex: both the interactive REPL
// thread and background threads (a finished download announcing itself as
// a new seed, e.g.) talk to the tracker through the same object.

#pragma once

#include "../common/protocol.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace p2p {

struct TrackerAddr {
    std::string ip;
    uint16_t port;
};

class TrackerConnection {
public:
    explicit TrackerConnection(std::vector<TrackerAddr> trackers);

    // Sends one request and waits for the single response message. Returns
    // false only when no tracker at all could be reached (both down) --
    // that's the one case the assignment says the system may stop serving.
    bool request(MsgType type, const MsgWriter& payload, Message& response);
    bool request(MsgType type, Message& response) { return request(type, MsgWriter{}, response); }

    // The login/logout commands call these so a later failover knows
    // whether (and as whom) to silently re-authenticate.
    void onLoginSuccess(const std::string& userId, const std::string& passwordHash,
                         const std::string& peerIp, uint16_t peerPort);
    void onLogout();

    bool loggedIn() const { std::lock_guard<std::mutex> lock(mu_); return hasCredentials_; }
    std::string userId() const { std::lock_guard<std::mutex> lock(mu_); return userId_; }

private:
    bool ensureConnectedLocked();
    void closeLocked();

    mutable std::mutex mu_;
    int fd_ = -1;
    std::vector<TrackerAddr> trackers_;
    size_t currentIdx_ = 0;

    bool hasCredentials_ = false;
    std::string userId_, passwordHash_, peerIp_;
    uint16_t peerPort_ = 0;
};

} // namespace p2p
