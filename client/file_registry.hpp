// client/file_registry.hpp
//
// A client-local, in-memory record of every file this client knows about
// on disk: files it has uploaded/is seeding, and files it is currently
// downloading (partial) or has finished downloading. This is the shared
// state between three otherwise-independent pieces of the client:
//
//   - uploader.cpp:   creates entries when `upload_file` runs
//   - downloader.cpp: creates/updates entries as pieces arrive
//   - peer_server.cpp: reads entries (bitfield + on-disk path) to answer
//                       other peers' BITFIELD_REQUEST / PIECE_REQUEST
//
// Keyed by fileSha1, which is content-addressed and therefore a natural,
// collision-free key for "the bytes of this file", independent of which
// group/name a peer happens to know it by.

#pragma once

#include "../common/common.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace p2p {

enum class FileState { SEEDING, DOWNLOADING, FAILED };

struct LocalFile {
    std::string fileSha1;
    std::string fileName;
    std::string groupId;
    uint64_t fileSize = 0;
    uint32_t numPieces = 0;
    std::vector<std::string> pieceSha1;
    std::string diskPath;

    std::mutex bitLock;
    std::vector<uint8_t> bitfield; // 1 bit per piece, set = present locally
    uint32_t piecesDone = 0;       // redundant with popcount(bitfield) but O(1) to read for progress
    FileState state = FileState::DOWNLOADING;
    std::string failureReason;
    bool viaDownload = false; // true if this entry originated from `download_file` (vs. `upload_file`)

    uint32_t doneCount() {
        std::lock_guard<std::mutex> lock(bitLock);
        return piecesDone;
    }

    bool hasPiece(uint32_t idx) {
        std::lock_guard<std::mutex> lock(bitLock);
        return (bitfield[idx / 8] >> (idx % 8)) & 1;
    }
    void markPiece(uint32_t idx) {
        std::lock_guard<std::mutex> lock(bitLock);
        uint8_t& byte = bitfield[idx / 8];
        uint8_t mask = 1 << (idx % 8);
        if (!(byte & mask)) {
            byte |= mask;
            ++piecesDone;
        }
    }
    std::vector<uint8_t> snapshotBitfield() {
        std::lock_guard<std::mutex> lock(bitLock);
        return bitfield;
    }
};

class FileRegistry {
public:
    std::shared_ptr<LocalFile> create(const std::string& fileSha1, const std::string& fileName,
                                       const std::string& groupId, uint64_t fileSize,
                                       const std::vector<std::string>& pieceSha1, const std::string& diskPath,
                                       FileState initialState);

    std::shared_ptr<LocalFile> find(const std::string& fileSha1);
    // Convenience for peer_server / commands that only know (group,name).
    std::shared_ptr<LocalFile> findByGroupAndName(const std::string& groupId, const std::string& fileName);

    std::vector<std::shared_ptr<LocalFile>> all();

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<LocalFile>> byHash_;
};

} // namespace p2p
