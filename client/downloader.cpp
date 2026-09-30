// client/downloader.cpp

#include "downloader.hpp"
#include "../common/sha1.hpp"
#include "../common/net_util.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <algorithm>
#include <fcntl.h>
#include <unistd.h>

namespace p2p {

DownloadManager::DownloadManager(FileRegistry& registry, TrackerConnection& tracker, std::string myIp, uint16_t myPort)
    : registry_(registry), tracker_(tracker), myIp_(std::move(myIp)), myPort_(myPort) {}

void DownloadManager::startDownload(std::string groupId, std::string fileName, std::string destPath) {
    std::lock_guard<std::mutex> lock(threadsMu_);
    threads_.emplace_back([this, groupId, fileName, destPath]() mutable {
        runDownload(std::move(groupId), std::move(fileName), std::move(destPath));
    });
}

void DownloadManager::joinAll() {
    std::lock_guard<std::mutex> lock(threadsMu_);
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
}

std::vector<DownloadProgress> DownloadManager::listProgress() {
    std::vector<DownloadProgress> out;
    for (auto& lf : registry_.all()) {
        if (!lf->viaDownload) continue;
        DownloadProgress dp;
        dp.groupId = lf->groupId;
        dp.fileName = lf->fileName;
        dp.piecesDone = lf->doneCount();
        dp.numPieces = lf->numPieces;
        dp.state = lf->state;
        dp.failureReason = lf->failureReason;
        out.push_back(std::move(dp));
    }
    return out;
}

void DownloadManager::workerThread(const std::shared_ptr<LocalFile>& lf, PeerConn* peer,
                                    const std::vector<uint32_t>& rarityOrder,
                                    std::vector<uint8_t>& inFlight, std::vector<uint8_t>& retries,
                                    std::mutex& schedMu) {
    while (true) {
        uint32_t chosen = UINT32_MAX;
        {
            std::lock_guard<std::mutex> lock(schedMu);
            for (uint32_t idx : rarityOrder) {
                if (lf->hasPiece(idx) || inFlight[idx] || retries[idx] >= kMaxPieceRetries) continue;
                bool peerHas = (idx / 8 < peer->bitfield.size()) && ((peer->bitfield[idx / 8] >> (idx % 8)) & 1);
                if (!peerHas) continue;
                chosen = idx;
                inFlight[idx] = 1;
                break;
            }
        }
        if (chosen == UINT32_MAX) break; // this peer has nothing left we still need right now

        bool success = false;
        bool connectionDead = false;

        MsgWriter req;
        req.putStr(lf->fileSha1);
        req.putU32(chosen);
        if (!sendMessage(peer->fd, MsgType::PIECE_REQUEST, req)) {
            connectionDead = true;
        } else {
            Message resp;
            if (!recvMessage(peer->fd, resp) || resp.type != MsgType::PIECE_RESPONSE) {
                connectionDead = true; // peer closed the link, or explicitly said "don't have it"
            } else {
                MsgReader r(resp.payload.data(), resp.payload.size());
                uint32_t gotIdx = r.getU32();
                uint32_t dataLen = 0;
                const uint8_t* data = r.getBytesRef(dataLen);
                if (r.ok() && gotIdx == chosen && data != nullptr) {
                    if (Sha1::hexOf(data, dataLen) == lf->pieceSha1[chosen]) {
                        int fd = ::open(lf->diskPath.c_str(), O_WRONLY);
                        if (fd >= 0) {
                            uint64_t offset = static_cast<uint64_t>(chosen) * kPieceSize;
                            ssize_t written = ::pwrite(fd, data, dataLen, static_cast<off_t>(offset));
                            ::close(fd);
                            success = (written == static_cast<ssize_t>(dataLen));
                            if (success) lf->markPiece(chosen); // only now is this piece "done"
                        }
                    }
                    // else: corrupted piece -- discarded, will be retried
                    // from this or another peer on a later pass.
                }
            }
        }

        {
            std::lock_guard<std::mutex> lock(schedMu);
            inFlight[chosen] = 0;
            if (!success) ++retries[chosen];
        }
        if (connectionDead) break;
    }
}

void DownloadManager::fetchRound(const std::shared_ptr<LocalFile>& lf, std::vector<PeerConn>& peers) {
    uint32_t numPieces = lf->numPieces;

    // Rarity among this round's peers: pieces held by fewer peers are
    // scheduled first, so the swarm doesn't waste time all fetching a
    // common piece while a scarce one goes untouched (classic BitTorrent
    // rarest-first heuristic -- maximizes eventual parallelism and keeps a
    // single peer's departure from stranding a piece nobody else has).
    std::vector<uint32_t> rarity(numPieces, 0);
    for (auto& p : peers) {
        for (uint32_t i = 0; i < numPieces; ++i) {
            if (i / 8 < p.bitfield.size() && ((p.bitfield[i / 8] >> (i % 8)) & 1)) ++rarity[i];
        }
    }
    std::vector<uint32_t> order;
    order.reserve(numPieces);
    for (uint32_t i = 0; i < numPieces; ++i) {
        if (!lf->hasPiece(i)) order.push_back(i);
    }
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        if (rarity[a] != rarity[b]) return rarity[a] < rarity[b];
        return a < b;
    });

    std::vector<uint8_t> inFlight(numPieces, 0);
    std::vector<uint8_t> retries(numPieces, 0);
    std::mutex schedMu;

    std::vector<std::thread> workers;
    workers.reserve(peers.size());
    for (auto& p : peers) {
        workers.emplace_back(&DownloadManager::workerThread, this, lf, &p, std::cref(order),
                              std::ref(inFlight), std::ref(retries), std::ref(schedMu));
    }
    for (auto& t : workers) t.join();
}

void DownloadManager::runDownload(std::string groupId, std::string fileName, std::string destPath) {
    const std::string tag = "[" + groupId + "] " + fileName;

    MsgWriter infoReq;
    infoReq.putStr(groupId);
    infoReq.putStr(fileName);
    Message resp;
    if (!tracker_.request(MsgType::DOWNLOAD_FILE, infoReq, resp)) {
        printLine("download failed: " + tag + " -- tracker unreachable");
        return;
    }
    if (resp.type == MsgType::RESP_ERR) {
        MsgReader er(resp.payload.data(), resp.payload.size());
        printLine("download failed: " + tag + " -- " + er.getStr());
        return;
    }
    if (resp.type != MsgType::DOWNLOAD_FILE_RESP) {
        printLine("download failed: " + tag + " -- unexpected tracker response");
        return;
    }

    MsgReader r(resp.payload.data(), resp.payload.size());
    std::string fileSha1 = r.getStr();
    uint64_t fileSize = r.getU64();
    uint32_t numPieces = r.getU32();
    std::vector<std::string> pieceHashes;
    pieceHashes.reserve(numPieces);
    for (uint32_t i = 0; i < numPieces && r.ok(); ++i) pieceHashes.push_back(r.getStr());
    uint32_t peerCount = r.getU32();
    struct RawPeer { std::string userId, ip; uint16_t port; };
    std::vector<RawPeer> peersToTry;
    for (uint32_t i = 0; i < peerCount && r.ok(); ++i) {
        RawPeer p;
        p.userId = r.getStr();
        p.ip = r.getStr();
        p.port = static_cast<uint16_t>(r.getU32());
        peersToTry.push_back(std::move(p));
    }
    if (!r.ok()) {
        printLine("download failed: " + tag + " -- malformed tracker response");
        return;
    }
    if (peersToTry.empty()) {
        printLine("download failed: " + tag + " -- no peers currently sharing this file");
        return;
    }

    int fd = ::open(destPath.c_str(), O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        printLine("download failed: " + tag + " -- cannot create destination file '" + destPath + "'");
        return;
    }
    if (::ftruncate(fd, static_cast<off_t>(fileSize)) != 0) {
        ::close(fd);
        printLine("download failed: " + tag + " -- cannot allocate " + std::to_string(fileSize) + " bytes at destination");
        return;
    }
    ::close(fd);

    auto lf = registry_.create(fileSha1, fileName, groupId, fileSize, pieceHashes, destPath, FileState::DOWNLOADING);
    lf->viaDownload = true;

    constexpr int kMaxRounds = 4;
    for (int round = 0; round < kMaxRounds && lf->doneCount() < numPieces; ++round) {
        if (round > 0) {
            // Refresh the peer list -- the swarm may have changed since
            // the last round (new seeders finished downloading, others
            // logged out).
            Message resp2;
            if (tracker_.request(MsgType::DOWNLOAD_FILE, infoReq, resp2) && resp2.type == MsgType::DOWNLOAD_FILE_RESP) {
                MsgReader r2(resp2.payload.data(), resp2.payload.size());
                r2.getStr();
                r2.getU64();
                uint32_t np2 = r2.getU32();
                for (uint32_t i = 0; i < np2 && r2.ok(); ++i) r2.getStr();
                uint32_t pc2 = r2.getU32();
                peersToTry.clear();
                for (uint32_t i = 0; i < pc2 && r2.ok(); ++i) {
                    RawPeer p;
                    p.userId = r2.getStr();
                    p.ip = r2.getStr();
                    p.port = static_cast<uint16_t>(r2.getU32());
                    peersToTry.push_back(std::move(p));
                }
            }
        }

        std::vector<PeerConn> peers;
        for (const auto& rp : peersToTry) {
            Socket sock = connectWithTimeout(rp.ip, rp.port, 2000);
            if (!sock.valid()) continue;
            int pfd = sock.release();

            MsgWriter bw;
            bw.putStr(fileSha1);
            if (!sendMessage(pfd, MsgType::BITFIELD_REQUEST, bw)) { ::close(pfd); continue; }
            Message bresp;
            if (!recvMessage(pfd, bresp) || bresp.type != MsgType::BITFIELD_RESPONSE) { ::close(pfd); continue; }
            MsgReader br(bresp.payload.data(), bresp.payload.size());
            br.getU32(); // peer's numPieces -- already known from tracker metadata, just skip it
            std::vector<uint8_t> bits = br.getBytes();
            if (!br.ok()) { ::close(pfd); continue; }

            PeerConn pc;
            pc.userId = rp.userId;
            pc.ip = rp.ip;
            pc.port = rp.port;
            pc.fd = pfd;
            pc.bitfield = std::move(bits);
            peers.push_back(std::move(pc));
            if (peers.size() >= static_cast<size_t>(kMaxParallelPeersPerDownload)) break;
        }

        if (!peers.empty()) fetchRound(lf, peers);
        for (auto& p : peers) {
            if (p.fd >= 0) ::close(p.fd);
        }
    }

    if (lf->doneCount() < numPieces) {
        lf->state = FileState::FAILED;
        lf->failureReason = "only obtained " + std::to_string(lf->doneCount()) + "/" + std::to_string(numPieces) +
                             " pieces from available peers";
        printLine("download incomplete: " + tag + " -- " + lf->failureReason);
        return;
    }

    std::string wholeFileHash;
    if (!Sha1::hexOfFile(destPath, wholeFileHash) || wholeFileHash != fileSha1) {
        lf->state = FileState::FAILED;
        lf->failureReason = "final whole-file hash mismatch after piece-level verification";
        printLine("download failed: " + tag + " -- " + lf->failureReason);
        return;
    }

    lf->state = FileState::SEEDING; // this client is now a full seeder for the file
    printLine("[C] [" + groupId + "] " + fileName);

    MsgWriter seedReq;
    seedReq.putStr(groupId);
    seedReq.putStr(fileName);
    Message seedResp;
    tracker_.request(MsgType::REGISTER_SEED, seedReq, seedResp); // best-effort announce
}

} // namespace p2p
