// tracker/tracker_sync.hpp
//
// Manages the single TCP link between the two trackers and drives the
// anti-entropy protocol described in oplog.hpp: on every (re)connection,
// both sides exchange "highest seq I have per origin" (SYNC_HELLO) and then
// each streams the records the other is missing (SYNC_OP). Once caught up,
// newly committed local ops are forwarded live over the same connection.
//
// Connection roles are fixed and deterministic to avoid the two trackers
// racing to both dial each other and ending up with two redundant links:
// the lower-numbered tracker (0) is always the connector (dials out,
// retrying with backoff until it succeeds), and the higher-numbered
// tracker (1) is always the acceptor (listens, accepts one link at a
// time). This works precisely because the assignment fixes the tracker
// count at exactly two.
//
// A dead/partitioned link is detected via tuned TCP keepalive rather than
// a hand-rolled application heartbeat -- simpler, and sufficient since both
// trackers run on the same OS family (Linux) for this assignment.

#pragma once

#include "tracker_state.hpp"

#include <atomic>
#include <mutex>
#include <thread>
#include <string>

namespace p2p {

class TrackerSync {
public:
    TrackerSync(TrackerState& state, uint32_t myNo, uint32_t peerNo,
                std::string peerSyncIp, uint16_t peerSyncPort, uint16_t mySyncPort);
    ~TrackerSync();

    void start();
    void stop();

    // Called right after a local command commits a new OpRecord. Best
    // effort: if the peer link is currently down the record is simply
    // picked up on the next reconnect catch-up (it's already durable in
    // TrackerState's log), so failures here are silently swallowed.
    void replicateAsync(const OpRecord& rec);

    bool isPeerConnected() const { return connFd_.load() >= 0; }

private:
    void connectorLoop();
    void acceptorLoop();
    void runConnection(int fd);
    void handshakeAndCatchup(int fd);
    void tuneKeepalive(int fd);

    TrackerState& state_;
    uint32_t myNo_;
    uint32_t peerNo_;
    std::string peerSyncIp_;
    uint16_t peerSyncPort_;
    uint16_t mySyncPort_;

    std::atomic<int> connFd_{-1};
    std::mutex writeMu_; // serializes writes to connFd_ across many caller threads
    std::atomic<bool> running_{false};
    std::thread worker_;
};

} // namespace p2p
