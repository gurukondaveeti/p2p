// common/protocol.cpp

#include "protocol.hpp"
#include "net_util.hpp"

#include <arpa/inet.h>
#include <cstring>

namespace p2p {

const char* msgTypeName(MsgType t) {
    switch (t) {
        case MsgType::CREATE_USER: return "CREATE_USER";
        case MsgType::LOGIN: return "LOGIN";
        case MsgType::LOGOUT: return "LOGOUT";
        case MsgType::CREATE_GROUP: return "CREATE_GROUP";
        case MsgType::JOIN_GROUP: return "JOIN_GROUP";
        case MsgType::LEAVE_GROUP: return "LEAVE_GROUP";
        case MsgType::LIST_GROUPS: return "LIST_GROUPS";
        case MsgType::LIST_GROUPS_RESP: return "LIST_GROUPS_RESP";
        case MsgType::LIST_REQUESTS: return "LIST_REQUESTS";
        case MsgType::LIST_REQUESTS_RESP: return "LIST_REQUESTS_RESP";
        case MsgType::ACCEPT_REQUEST: return "ACCEPT_REQUEST";
        case MsgType::UPLOAD_FILE: return "UPLOAD_FILE";
        case MsgType::LIST_FILES: return "LIST_FILES";
        case MsgType::LIST_FILES_RESP: return "LIST_FILES_RESP";
        case MsgType::DOWNLOAD_FILE: return "DOWNLOAD_FILE";
        case MsgType::DOWNLOAD_FILE_RESP: return "DOWNLOAD_FILE_RESP";
        case MsgType::STOP_SHARE: return "STOP_SHARE";
        case MsgType::REGISTER_SEED: return "REGISTER_SEED";
        case MsgType::RESP_OK: return "RESP_OK";
        case MsgType::RESP_ERR: return "RESP_ERR";
        case MsgType::BITFIELD_REQUEST: return "BITFIELD_REQUEST";
        case MsgType::BITFIELD_RESPONSE: return "BITFIELD_RESPONSE";
        case MsgType::PIECE_REQUEST: return "PIECE_REQUEST";
        case MsgType::PIECE_RESPONSE: return "PIECE_RESPONSE";
        case MsgType::PIECE_NOTFOUND: return "PIECE_NOTFOUND";
        case MsgType::SYNC_HELLO: return "SYNC_HELLO";
        case MsgType::SYNC_OP: return "SYNC_OP";
        case MsgType::SYNC_HEARTBEAT: return "SYNC_HEARTBEAT";
    }
    return "UNKNOWN";
}

// ---- MsgWriter -------------------------------------------------------

void MsgWriter::putU8(uint8_t v) { buf_.push_back(v); }

void MsgWriter::putU32(uint32_t v) {
    uint32_t be = htonl(v);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&be);
    buf_.insert(buf_.end(), p, p + 4);
}

void MsgWriter::putU64(uint64_t v) {
    putU32(static_cast<uint32_t>(v >> 32));
    putU32(static_cast<uint32_t>(v & 0xFFFFFFFFu));
}

void MsgWriter::putStr(const std::string& s) {
    uint16_t len = static_cast<uint16_t>(s.size() > 0xFFFF ? 0xFFFF : s.size());
    uint16_t be = htons(len);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&be);
    buf_.insert(buf_.end(), p, p + 2);
    buf_.insert(buf_.end(), s.begin(), s.begin() + len);
}

void MsgWriter::putBytes(const void* data, uint32_t len) {
    putU32(len);
    const uint8_t* p = static_cast<const uint8_t*>(data);
    buf_.insert(buf_.end(), p, p + len);
}

// ---- MsgReader -------------------------------------------------------

bool MsgReader::require(size_t n) {
    if (!ok_ || pos_ + n > len_) {
        ok_ = false;
        return false;
    }
    return true;
}

uint8_t MsgReader::getU8() {
    if (!require(1)) return 0;
    return data_[pos_++];
}

uint32_t MsgReader::getU32() {
    if (!require(4)) return 0;
    uint32_t be;
    std::memcpy(&be, data_ + pos_, 4);
    pos_ += 4;
    return ntohl(be);
}

uint64_t MsgReader::getU64() {
    uint64_t hi = getU32();
    uint64_t lo = getU32();
    return (hi << 32) | lo;
}

std::string MsgReader::getStr() {
    if (!require(2)) return "";
    uint16_t be;
    std::memcpy(&be, data_ + pos_, 2);
    pos_ += 2;
    uint16_t slen = ntohs(be);
    if (!require(slen)) return "";
    std::string s(reinterpret_cast<const char*>(data_ + pos_), slen);
    pos_ += slen;
    return s;
}

std::vector<uint8_t> MsgReader::getBytes() {
    uint32_t blen;
    if (!require(4)) return {};
    std::memcpy(&blen, data_ + pos_, 4);
    pos_ += 4;
    blen = ntohl(blen);
    if (!require(blen)) return {};
    std::vector<uint8_t> v(data_ + pos_, data_ + pos_ + blen);
    pos_ += blen;
    return v;
}

const uint8_t* MsgReader::getBytesRef(uint32_t& outLen) {
    if (!require(4)) { outLen = 0; return nullptr; }
    uint32_t blen;
    std::memcpy(&blen, data_ + pos_, 4);
    pos_ += 4;
    blen = ntohl(blen);
    if (!require(blen)) { outLen = 0; return nullptr; }
    const uint8_t* ptr = data_ + pos_;
    pos_ += blen;
    outLen = blen;
    return ptr;
}

// ---- wire framing ------------------------------------------------------

namespace {
struct WireHeader {
    uint32_t magic;
    uint16_t type;
    uint16_t flags;
    uint32_t length;
};
} // namespace

bool sendMessage(int fd, MsgType type, const std::vector<uint8_t>& payload) {
    WireHeader hdr;
    hdr.magic = htonl(kProtoMagic);
    hdr.type = htons(static_cast<uint16_t>(type));
    hdr.flags = 0;
    hdr.length = htonl(static_cast<uint32_t>(payload.size()));

    if (!sendAll(fd, &hdr, sizeof(hdr))) return false;
    if (!payload.empty() && !sendAll(fd, payload.data(), payload.size())) return false;
    return true;
}

bool sendMessage(int fd, MsgType type, const MsgWriter& writer) {
    return sendMessage(fd, type, writer.buffer());
}

bool recvMessage(int fd, Message& out, uint32_t maxPayload) {
    WireHeader hdr;
    if (!recvAll(fd, &hdr, sizeof(hdr))) return false;

    uint32_t magic = ntohl(hdr.magic);
    if (magic != kProtoMagic) return false; // desynced stream / garbage peer

    uint32_t length = ntohl(hdr.length);
    if (length > maxPayload) return false; // guard against a bogus huge length

    out.type = static_cast<MsgType>(ntohs(hdr.type));
    out.payload.resize(length);
    if (length > 0 && !recvAll(fd, out.payload.data(), length)) return false;
    return true;
}

} // namespace p2p
