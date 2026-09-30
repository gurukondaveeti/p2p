// common/protocol.hpp
//
// The custom application-layer protocol used for all three kinds of
// communication in the system (client<->tracker, client<->client,
// tracker<->tracker). Design goals, in order:
//
//   1. Self-describing framing that never needs to guess a payload's
//      length or scan for a delimiter -- every message is a fixed 12-byte
//      header (magic, type, flags, payload length) followed by exactly
//      that many payload bytes. recvMessage() loops on the socket until it
//      has the whole header and the whole payload (see net_util::recvAll),
//      so partial reads/writes from TCP are a non-issue.
//   2. A payload is a sequence of TLV-ish fields (length-prefixed strings,
//      length-prefixed byte blobs, and fixed-width integers) built with
//      MsgWriter and read back with MsgReader. This avoids any delimiter/
//      escaping problems -- important because piece data is arbitrary
//      binary and file/user names are attacker- or typo-controlled input.
//   3. MsgReader is defensive: every get*() bounds-checks against the
//      remaining buffer and sets an internal error flag instead of
//      reading out of bounds, so a malformed or truncated message from a
//      buggy/malicious peer cannot crash the process -- it just fails the
//      request with an error response.
//
// All integers on the wire are big-endian (network byte order).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace p2p {

constexpr uint32_t kProtoMagic = 0x50325046; // "P2PF"

enum class MsgType : uint16_t {
    // ---- Client <-> Tracker: account & session ----
    CREATE_USER = 1,
    LOGIN,
    LOGOUT,

    // ---- Client <-> Tracker: groups ----
    CREATE_GROUP,
    JOIN_GROUP,
    LEAVE_GROUP,
    LIST_GROUPS,
    LIST_GROUPS_RESP,
    LIST_REQUESTS,
    LIST_REQUESTS_RESP,
    ACCEPT_REQUEST,

    // ---- Client <-> Tracker: files ----
    UPLOAD_FILE,
    LIST_FILES,
    LIST_FILES_RESP,
    DOWNLOAD_FILE,
    DOWNLOAD_FILE_RESP,
    STOP_SHARE,
    REGISTER_SEED,

    // ---- Generic tracker responses ----
    RESP_OK,
    RESP_ERR,

    // ---- Client <-> Client (peer wire protocol) ----
    BITFIELD_REQUEST,
    BITFIELD_RESPONSE,
    PIECE_REQUEST,
    PIECE_RESPONSE,
    PIECE_NOTFOUND,

    // ---- Tracker <-> Tracker (replication) ----
    SYNC_HELLO,        // announce {trackerNo, lastSeq[0], lastSeq[1]} on connect
    SYNC_OP,           // one replicated operation (see tracker/oplog.hpp)
    SYNC_HEARTBEAT,    // keepalive so a dead link is noticed quickly
};

const char* msgTypeName(MsgType t);

// ---------------------------------------------------------------------
// MsgWriter: builds a payload field by field.
// ---------------------------------------------------------------------
class MsgWriter {
public:
    void putU8(uint8_t v);
    void putU32(uint32_t v);
    void putU64(uint64_t v);
    void putStr(const std::string& s);                  // u16 len + bytes
    void putBytes(const void* data, uint32_t len);       // u32 len + bytes

    const std::vector<uint8_t>& buffer() const { return buf_; }

private:
    std::vector<uint8_t> buf_;
};

// ---------------------------------------------------------------------
// MsgReader: reads fields back out in the order they were written.
// Every accessor checks bounds; call ok() after a batch of reads (or after
// each one) to detect a truncated/malformed payload.
// ---------------------------------------------------------------------
class MsgReader {
public:
    MsgReader(const uint8_t* data, size_t len) : data_(data), len_(len), pos_(0), ok_(true) {}

    uint8_t getU8();
    uint32_t getU32();
    uint64_t getU64();
    std::string getStr();
    std::vector<uint8_t> getBytes();
    // Zero-copy variant for large payloads (piece data) -- valid only as
    // long as the underlying buffer outlives it.
    const uint8_t* getBytesRef(uint32_t& outLen);

    bool ok() const { return ok_; }
    size_t remaining() const { return pos_ <= len_ ? len_ - pos_ : 0; }

private:
    bool require(size_t n);

    const uint8_t* data_;
    size_t len_;
    size_t pos_;
    bool ok_;
};

// ---------------------------------------------------------------------
// Whole-message send/receive over a connected TCP socket.
// ---------------------------------------------------------------------
struct Message {
    MsgType type;
    std::vector<uint8_t> payload;
};

bool sendMessage(int fd, MsgType type, const std::vector<uint8_t>& payload);
bool sendMessage(int fd, MsgType type, const MsgWriter& writer);
// Convenience for messages with no payload at all.
inline bool sendMessage(int fd, MsgType type) { return sendMessage(fd, type, std::vector<uint8_t>()); }

// Returns false on any I/O error or clean disconnect (caller should treat
// the connection as closed either way).
bool recvMessage(int fd, Message& out, uint32_t maxPayload = 64u * 1024 * 1024);

} // namespace p2p
