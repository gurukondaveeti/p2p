// client/uploader.cpp

#include "uploader.hpp"
#include "../common/sha1.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <cstdio>
#include <sys/stat.h>
#include <vector>

namespace p2p {

namespace {
std::string baseName(const std::string& path) {
    size_t pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}
} // namespace

bool uploadFile(FileRegistry& registry, TrackerConnection& tracker, const std::string& myIp, uint16_t myPort,
                 const std::string& groupId, const std::string& filePath, std::string& outMessage) {
    struct stat st;
    if (::stat(filePath.c_str(), &st) != 0) {
        outMessage = "cannot access file: " + filePath;
        return false;
    }
    uint64_t fileSize = static_cast<uint64_t>(st.st_size);
    if (fileSize > kMaxFileSize) {
        outMessage = "file exceeds the 1GB limit";
        return false;
    }

    std::FILE* f = std::fopen(filePath.c_str(), "rb");
    if (!f) {
        outMessage = "cannot open file: " + filePath;
        return false;
    }

    uint32_t numPieces = numPiecesFor(fileSize);
    std::vector<std::string> pieceHashes;
    pieceHashes.reserve(numPieces);

    Sha1 wholeFileHasher;
    std::vector<uint8_t> buf(kPieceSize);
    bool readError = false;

    for (uint32_t i = 0; i < numPieces; ++i) {
        uint64_t want = pieceLength(fileSize, i, numPieces);
        size_t got = want > 0 ? std::fread(buf.data(), 1, want, f) : 0;
        if (got != want) { readError = true; break; }
        pieceHashes.push_back(Sha1::hexOf(buf.data(), got));
        wholeFileHasher.update(buf.data(), got);
    }
    std::fclose(f);

    if (readError) {
        outMessage = "error reading file while hashing pieces";
        return false;
    }

    uint8_t digest[20];
    wholeFileHasher.finalize(digest);
    std::string fileSha1 = Sha1::toHex(digest);
    std::string fileName = baseName(filePath);

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(fileName);
    w.putStr(fileSha1);
    w.putU64(fileSize);
    w.putU32(numPieces);
    for (const auto& h : pieceHashes) w.putStr(h);

    Message resp;
    if (!tracker.request(MsgType::UPLOAD_FILE, w, resp)) {
        outMessage = "tracker unreachable";
        return false;
    }
    MsgReader r(resp.payload.data(), resp.payload.size());
    std::string serverMsg = r.getStr();
    if (resp.type != MsgType::RESP_OK) {
        outMessage = serverMsg.empty() ? "upload rejected by tracker" : serverMsg;
        return false;
    }

    auto lf = registry.create(fileSha1, fileName, groupId, fileSize, pieceHashes, filePath, FileState::SEEDING);
    for (uint32_t i = 0; i < numPieces; ++i) lf->markPiece(i); // we already have every piece on disk

    (void)myIp;
    (void)myPort;
    outMessage = "shared '" + fileName + "' with group " + groupId + " (" + std::to_string(numPieces) +
                 " piece(s), sha1=" + fileSha1.substr(0, 10) + "...)";
    return true;
}

} // namespace p2p
