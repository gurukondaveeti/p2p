// client/peer_server.hpp
//
// The "seeder" half of every client: listens on this client's own
// IP:PORT (the address it announced to the tracker at login/upload time)
// and answers other peers' bitfield/piece requests by reading straight off
// local disk. Every client runs exactly one of these, regardless of how
// many files it happens to be seeding.

#pragma once

#include "file_registry.hpp"

#include <atomic>
#include <string>
#include <thread>

namespace p2p {

class PeerServer {
public:
    PeerServer(FileRegistry& registry, std::string bindIp, uint16_t port);

    // Starts the accept loop on a background thread. Returns false if the
    // listening socket could not be bound (fatal at startup).
    bool start();
    void stop();

private:
    void acceptLoop(int listenFd);
    void handlePeer(int fd);

    FileRegistry& registry_;
    std::string bindIp_;
    uint16_t port_;
    std::atomic<bool> running_{false};
    int listenFd_ = -1;
    std::thread thread_;
};

} // namespace p2p
