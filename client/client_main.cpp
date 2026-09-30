// client/client_main.cpp
//
// Entry point for the client binary.
//
//   ./client <IP>:<PORT> <tracker_info.txt>
//
// <IP>:<PORT> is this client's own peer-listening address -- the one other
// clients will connect to (via addresses handed out by the tracker) to
// pull pieces from it once it's seeding something. tracker_info.txt is the
// same two-line file the trackers use; the client only needs each line's
// ip:client_port (the sync_port is a tracker-internal detail).

#include "tracker_conn.hpp"
#include "file_registry.hpp"
#include "peer_server.hpp"
#include "downloader.hpp"
#include "commands.hpp"
#include "../common/common.hpp"
#include "../common/net_util.hpp"

#include <fstream>
#include <iostream>

using namespace p2p;

namespace {

bool loadTrackerAddrs(const std::string& path, std::vector<TrackerAddr>& out) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "cannot open tracker info file: " << path << "\n";
        return false;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        size_t p1 = line.find(':');
        size_t p2 = line.find(':', p1 == std::string::npos ? std::string::npos : p1 + 1);
        if (p1 == std::string::npos) continue;
        std::string ip = line.substr(0, p1);
        std::string portStr = (p2 == std::string::npos) ? line.substr(p1 + 1) : line.substr(p1 + 1, p2 - p1 - 1);
        out.push_back({ip, static_cast<uint16_t>(std::atoi(portStr.c_str()))});
    }
    if (out.empty()) {
        std::cerr << "tracker info file has no usable entries\n";
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: " << argv[0] << " <IP>:<PORT> <tracker_info.txt>\n";
        return 1;
    }

    std::string myIp;
    uint16_t myPort;
    if (!splitHostPort(argv[1], myIp, myPort)) {
        std::cerr << "invalid <IP>:<PORT>: " << argv[1] << "\n";
        return 1;
    }

    std::vector<TrackerAddr> trackerAddrs;
    if (!loadTrackerAddrs(argv[2], trackerAddrs)) return 1;

    FileRegistry registry;
    TrackerConnection tracker(trackerAddrs);
    DownloadManager downloads(registry, tracker, myIp, myPort);

    PeerServer peerServer(registry, myIp, myPort);
    if (!peerServer.start()) {
        std::cerr << "fatal: could not start peer server on " << myIp << ":" << myPort << "\n";
        return 1;
    }

    ClientContext ctx{tracker, registry, downloads, myIp, myPort};
    runCommandLoop(ctx);

    // Best-effort clean logout so the tracker doesn't keep stale seeder
    // entries pointing at a peer that's about to disappear.
    if (tracker.loggedIn()) {
        Message resp;
        tracker.request(MsgType::LOGOUT, MsgWriter{}, resp);
    }

    std::cout << "shutting down (waiting for in-flight downloads to finish)...\n";
    downloads.joinAll();
    peerServer.stop();
    return 0;
}
