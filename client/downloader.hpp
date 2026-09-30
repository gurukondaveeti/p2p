// client/downloader.hpp
//
// Drives one file download from start ("[C]" completion line) to finish:
// fetches metadata + a peer list from the tracker, exchanges bitfields
// with each peer, schedules pieces rarest-first across a small pool of
// worker threads (one persistent connection per peer, reused for many
// piece requests), verifies every piece's SHA1 the instant it arrives,
// and writes it straight to its final offset in the destination file with
// pwrite -- so concurrent workers never contend on a shared file position
// or need a lock around disk I/O.
//
// `download_file` returns to the REPL immediately; the transfer runs on a
// background thread so multiple files (and multiple pieces of the same
// file) genuinely progress concurrently, with `show_downloads` polling
// FileRegistry for live status.

#pragma once

#include "file_registry.hpp"
#include "tracker_conn.hpp"

#include <string>
#include <thread>
#include <vector>
#include <mutex>

namespace p2p {

struct DownloadProgress {
    std::string groupId;
    std::string fileName;
    uint32_t piecesDone;
    uint32_t numPieces;
    FileState state;
    std::string failureReason;
};

class DownloadManager {
public:
    DownloadManager(FileRegistry& registry, TrackerConnection& tracker, std::string myIp, uint16_t myPort);

    // Starts the download on a background thread and returns immediately.
    void startDownload(std::string groupId, std::string fileName, std::string destPath);

    std::vector<DownloadProgress> listProgress();

    // Waits for every in-flight download thread to finish. Call before the
    // process exits so no background thread outlives the objects (tracker
    // connection, registry) it holds references to.
    void joinAll();

private:
    struct PeerConn {
        std::string userId, ip;
        uint16_t port;
        int fd = -1;
        std::vector<uint8_t> bitfield;
    };

    // One attempt at fetching every currently-missing piece of `lf` using
    // `peers`. Returns once every reachable peer's usable pieces have been
    // exhausted (not necessarily once the file is complete -- the caller
    // loops this across a few rounds, re-querying the tracker each time in
    // case new seeders appeared).
    void fetchRound(const std::shared_ptr<LocalFile>& lf, std::vector<PeerConn>& peers);

    void workerThread(const std::shared_ptr<LocalFile>& lf, PeerConn* peer,
                       const std::vector<uint32_t>& rarityOrder,
                       std::vector<uint8_t>& inFlight, std::vector<uint8_t>& retries, std::mutex& schedMu);

    void runDownload(std::string groupId, std::string fileName, std::string destPath);

    FileRegistry& registry_;
    TrackerConnection& tracker_;
    std::string myIp_;
    uint16_t myPort_;

    std::mutex threadsMu_;
    std::vector<std::thread> threads_;
};

} // namespace p2p
