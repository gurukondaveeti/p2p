// tracker/tracker_main.cpp
//
// Entry point for the tracker binary.
//
//   ./tracker <tracker_info.txt> <tracker_no>
//
// tracker_info.txt has exactly two lines, one per tracker, each of the form
//
//   <ip>:<client_port>:<sync_port>
//
// tracker_no (0 or 1) selects which line describes *this* process; the
// other line describes its peer tracker for replication. <client_port> is
// where user-facing clients connect (matches the ip:port clients are told
// to use); <sync_port> is the private tracker-to-tracker replication link.
//
// Once running, typing `quit` on the tracker's own console shuts it down
// gracefully (per the assignment's "quit - Shutdown tracker" spec).

#include "tracker_state.hpp"
#include "tracker_sync.hpp"
#include "tracker_server.hpp"
#include "../common/common.hpp"
#include "../common/net_util.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

using namespace p2p;

namespace {

struct TrackerConfig {
    std::string ip;
    uint16_t clientPort;
    uint16_t syncPort;
};

bool loadConfig(const std::string& path, TrackerConfig cfg[2]) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "cannot open tracker info file: " << path << "\n";
        return false;
    }
    std::string line;
    int idx = 0;
    while (idx < 2 && std::getline(in, line)) {
        if (line.empty()) continue;
        // format: ip:clientPort:syncPort
        size_t p1 = line.find(':');
        size_t p2 = line.find(':', p1 == std::string::npos ? std::string::npos : p1 + 1);
        if (p1 == std::string::npos || p2 == std::string::npos) {
            std::cerr << "malformed line in tracker info file: " << line << "\n";
            return false;
        }
        cfg[idx].ip = line.substr(0, p1);
        cfg[idx].clientPort = static_cast<uint16_t>(std::atoi(line.substr(p1 + 1, p2 - p1 - 1).c_str()));
        cfg[idx].syncPort = static_cast<uint16_t>(std::atoi(line.substr(p2 + 1).c_str()));
        ++idx;
    }
    if (idx != 2) {
        std::cerr << "tracker info file must have exactly two lines (found " << idx << ")\n";
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: " << argv[0] << " <tracker_info.txt> <tracker_no: 0|1>\n";
        return 1;
    }

    std::string infoPath = argv[1];
    int trackerNo = std::atoi(argv[2]);
    if (trackerNo != 0 && trackerNo != 1) {
        std::cerr << "tracker_no must be 0 or 1\n";
        return 1;
    }

    TrackerConfig cfg[2];
    if (!loadConfig(infoPath, cfg)) return 1;

    uint32_t myNo = static_cast<uint32_t>(trackerNo);
    uint32_t peerNo = 1 - myNo;

    std::string logPath = "tracker" + std::to_string(myNo) + "_oplog.bin";
    TrackerState state(myNo, logPath);
    state.loadFromDisk();

    TrackerSync sync(state, myNo, peerNo, cfg[peerNo].ip, cfg[peerNo].syncPort, cfg[myNo].syncPort);
    sync.start();

    TrackerServer server(state, sync, cfg[myNo].ip, cfg[myNo].clientPort);
    std::thread serverThread([&server] { server.run(); });

    P2P_LOG_INFO("tracker " + std::to_string(myNo) + " up. clients -> " + cfg[myNo].ip + ":" +
                 std::to_string(cfg[myNo].clientPort) + " | peer sync -> " + cfg[peerNo].ip + ":" +
                 std::to_string(cfg[peerNo].syncPort));
    std::cout << "tracker " << myNo << " ready. type 'quit' to shut down, 'status' for a state summary.\n";

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "quit") {
            break;
        } else if (line == "status") {
            std::cout << "peer tracker connected: " << (sync.isPeerConnected() ? "yes" : "no") << "\n";
            auto groups = state.listGroups();
            std::cout << groups.size() << " group(s):\n";
            for (const auto& g : groups) {
                std::cout << "  " << g.id << " (owner=" << g.owner << ", members=" << g.memberCount << ")\n";
            }
        } else if (!line.empty()) {
            std::cout << "unknown command. try 'status' or 'quit'.\n";
        }
    }

    std::cout << "shutting down...\n";
    server.stop();
    sync.stop();
    serverThread.join();
    return 0;
}
