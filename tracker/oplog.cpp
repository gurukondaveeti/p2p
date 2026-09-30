// tracker/oplog.cpp

#include "oplog.hpp"
#include "../common/protocol.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>

namespace p2p {

const char* opTypeName(OpType t) {
    switch (t) {
        case OpType::CREATE_USER: return "CREATE_USER";
        case OpType::CREATE_GROUP: return "CREATE_GROUP";
        case OpType::JOIN_REQUEST: return "JOIN_REQUEST";
        case OpType::ACCEPT_REQUEST: return "ACCEPT_REQUEST";
        case OpType::LEAVE_GROUP: return "LEAVE_GROUP";
        case OpType::ADD_FILE: return "ADD_FILE";
        case OpType::REGISTER_SEED: return "REGISTER_SEED";
        case OpType::REMOVE_SEED: return "REMOVE_SEED";
    }
    return "UNKNOWN";
}

std::vector<uint8_t> encodeOpRecord(const OpRecord& rec) {
    MsgWriter w;
    w.putU32(rec.originTracker);
    w.putU64(rec.seq);
    w.putU8(static_cast<uint8_t>(rec.opType));
    w.putBytes(rec.fields.data(), static_cast<uint32_t>(rec.fields.size()));
    return w.buffer();
}

bool decodeOpRecord(const uint8_t* data, size_t len, OpRecord& out) {
    MsgReader r(data, len);
    out.originTracker = r.getU32();
    out.seq = r.getU64();
    out.opType = static_cast<OpType>(r.getU8());
    out.fields = r.getBytes();
    return r.ok();
}

bool appendRecordToFile(std::FILE* f, const OpRecord& rec) {
    if (!f) return false;
    std::vector<uint8_t> encoded = encodeOpRecord(rec);
    uint32_t lenBe = htonl(static_cast<uint32_t>(encoded.size()));
    if (std::fwrite(&lenBe, sizeof(lenBe), 1, f) != 1) return false;
    if (!encoded.empty() && std::fwrite(encoded.data(), 1, encoded.size(), f) != encoded.size()) return false;
    std::fflush(f);
    ::fsync(fileno(f)); // durability: survive a crash immediately after this call
    return true;
}

bool readAllRecordsFromFile(const std::string& path, std::vector<OpRecord>& out) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return true; // no log yet -- that's fine, state starts empty

    while (true) {
        uint32_t lenBe;
        size_t n = std::fread(&lenBe, sizeof(lenBe), 1, f);
        if (n != 1) break; // EOF (or a trailing partial write from a crash -- stop cleanly)
        uint32_t len = ntohl(lenBe);

        std::vector<uint8_t> buf(len);
        if (len > 0 && std::fread(buf.data(), 1, len, f) != len) {
            break; // truncated last record (crash mid-append) -- ignore it
        }
        OpRecord rec;
        if (!decodeOpRecord(buf.data(), buf.size(), rec)) break;
        out.push_back(std::move(rec));
    }
    std::fclose(f);
    return true;
}

} // namespace p2p
