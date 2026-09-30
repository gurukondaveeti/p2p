// client/commands.hpp
//
// Parses lines typed at the client's REPL and dispatches each to the
// tracker connection / downloader / uploader. One function, one place that
// knows the whole command grammar from the assignment spec.

#pragma once

#include "tracker_conn.hpp"
#include "file_registry.hpp"
#include "downloader.hpp"

#include <string>

namespace p2p {

struct ClientContext {
    TrackerConnection& tracker;
    FileRegistry& registry;
    DownloadManager& downloads;
    std::string myIp;
    uint16_t myPort;
};

// Runs the interactive command loop on the calling thread until EOF or an
// `exit`/`quit` command. Returns once the user is done.
void runCommandLoop(ClientContext& ctx);

} // namespace p2p
