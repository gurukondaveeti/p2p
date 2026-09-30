// tracker/tracker_server.hpp
//
// The tracker's client-facing side: accepts client TCP connections and
// runs one thread per connection, translating wire messages into
// TrackerState commands/queries and replicating any resulting mutation via
// TrackerSync. A connection carries one login session for its whole
// lifetime (see the .cpp for why).

#pragma once

#include "tracker_state.hpp"
#include "tracker_sync.hpp"

#include <atomic>
#include <mutex>
#include <set>
#include <thread>
#include <string>

namespace p2p {

class TrackerServer {
public:
    TrackerServer(TrackerState& state, TrackerSync& sync, std::string bindIp, uint16_t port);

    // Binds and runs the accept loop on the calling thread until stop() is
    // called from another thread. Returns once fully shut down.
    void run();
    void stop();

private:
    void handleClient(int fd);

    TrackerState& state_;
    TrackerSync& sync_;
    std::string bindIp_;
    uint16_t port_;

    std::atomic<bool> running_{true};
    int listenFd_ = -1;

    std::mutex clientsMu_;
    std::set<int> activeClientFds_;
    std::vector<std::thread> clientThreads_;
};

} // namespace p2p
