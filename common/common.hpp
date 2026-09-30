// common/common.hpp
//
// Shared constants and small utilities used by both the tracker and the
// client. Keeping these in one place means the piece size, limits, and
// logging format can never drift between the two binaries.

#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <string>
#include <mutex>

namespace p2p {

// ---- Protocol-visible constants -------------------------------------------
// These numbers are part of the wire contract: both trackers and every
// client must agree on them, so they live here instead of being duplicated.

constexpr uint64_t kPieceSize = 512 * 1024;          // 512 KB, per the spec
constexpr uint64_t kMaxFileSize = 1ull * 1024 * 1024 * 1024; // 1 GB soft cap
constexpr int kShaDigestBytes = 20;                  // raw SHA1 digest size
constexpr int kShaHexLen = 40;                       // hex string length

// Number of peer connections a single download will fan out across at once.
constexpr int kMaxParallelPeersPerDownload = 4;

// How many times a single piece may be retried (from different peers) before
// the whole download is declared failed.
constexpr int kMaxPieceRetries = 6;

// ---- Logging ----------------------------------------------------------
// A tiny thread-safe logger. Not a logging framework -- just enough to get
// readable, timestamped, interleaved output from many threads without them
// stepping on each other mid-line.

inline std::mutex& logMutex() {
    static std::mutex m;
    return m;
}

inline void logLine(const char* tag, const std::string& msg) {
    std::lock_guard<std::mutex> lock(logMutex());
    std::time_t t = std::time(nullptr);
    char timebuf[16];
    std::strftime(timebuf, sizeof(timebuf), "%H:%M:%S", std::localtime(&t));
    std::fprintf(stderr, "[%s] [%s] %s\n", timebuf, tag, msg.c_str());
    std::fflush(stderr);
}

#define P2P_LOG_INFO(msg)  ::p2p::logLine("INFO",  (msg))
#define P2P_LOG_WARN(msg)  ::p2p::logLine("WARN",  (msg))
#define P2P_LOG_ERROR(msg) ::p2p::logLine("ERROR", (msg))

// A second, separate mutex for user-facing stdout output (status lines,
// command results). Kept apart from the stderr debug logger above so a
// background download thread's "[C] [group] file" completion line never
// gets interleaved mid-character with the REPL's own output.
inline std::mutex& stdoutMutex() {
    static std::mutex m;
    return m;
}

inline void printLine(const std::string& s) {
    std::lock_guard<std::mutex> lock(stdoutMutex());
    std::fputs(s.c_str(), stdout);
    std::fputc('\n', stdout);
    std::fflush(stdout);
}

// ---- Small helpers -------------------------------------------------------

// Number of pieces a file of `size` bytes is split into, given kPieceSize.
inline uint32_t numPiecesFor(uint64_t size) {
    if (size == 0) return 1; // zero-byte files still get one (empty) piece
    return static_cast<uint32_t>((size + kPieceSize - 1) / kPieceSize);
}

// Size in bytes of a specific piece index (the last piece may be short).
inline uint64_t pieceLength(uint64_t fileSize, uint32_t pieceIndex, uint32_t numPieces) {
    if (pieceIndex + 1 < numPieces) return kPieceSize;
    uint64_t consumed = static_cast<uint64_t>(pieceIndex) * kPieceSize;
    return fileSize > consumed ? fileSize - consumed : 0;
}

inline uint32_t bitfieldBytes(uint32_t numPieces) {
    return (numPieces + 7) / 8;
}

} // namespace p2p
