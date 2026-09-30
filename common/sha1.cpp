// common/sha1.cpp
//
// Straightforward, unoptimized SHA-1 (RFC 3174). Processes data in 64-byte
// blocks so it can hash a 1GB file with a few KB of memory (see
// hexOfFile), which matters given the assignment's "handle large files
// without excessive memory usage" requirement.

#include "sha1.hpp"

#include <cstdio>
#include <cstring>

namespace p2p {

namespace {
inline uint32_t rotl32(uint32_t x, int n) { return (x << n) | (x >> (32 - n)); }
} // namespace

void Sha1::reset() {
    h_[0] = 0x67452301;
    h_[1] = 0xEFCDAB89;
    h_[2] = 0x98BADCFE;
    h_[3] = 0x10325476;
    h_[4] = 0xC3D2E1F0;
    totalLenBits_ = 0;
    bufferLen_ = 0;
}

void Sha1::processBlock(const uint8_t block[64]) {
    uint32_t w[80];
    for (int i = 0; i < 16; ++i) {
        w[i] = (uint32_t(block[i * 4]) << 24) | (uint32_t(block[i * 4 + 1]) << 16) |
               (uint32_t(block[i * 4 + 2]) << 8) | (uint32_t(block[i * 4 + 3]));
    }
    for (int i = 16; i < 80; ++i) {
        w[i] = rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }

    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];

    for (int i = 0; i < 80; ++i) {
        uint32_t f, k;
        if (i < 20) {
            f = (b & c) | ((~b) & d);
            k = 0x5A827999;
        } else if (i < 40) {
            f = b ^ c ^ d;
            k = 0x6ED9EBA1;
        } else if (i < 60) {
            f = (b & c) | (b & d) | (c & d);
            k = 0x8F1BBCDC;
        } else {
            f = b ^ c ^ d;
            k = 0xCA62C1D6;
        }
        uint32_t temp = rotl32(a, 5) + f + e + k + w[i];
        e = d;
        d = c;
        c = rotl32(b, 30);
        b = a;
        a = temp;
    }

    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
}

void Sha1::update(const uint8_t* data, size_t len) {
    totalLenBits_ += static_cast<uint64_t>(len) * 8;

    // Fill any partial block left over from a previous call first.
    if (bufferLen_ > 0) {
        size_t need = 64 - bufferLen_;
        size_t take = len < need ? len : need;
        std::memcpy(buffer_ + bufferLen_, data, take);
        bufferLen_ += take;
        data += take;
        len -= take;
        if (bufferLen_ == 64) {
            processBlock(buffer_);
            bufferLen_ = 0;
        }
    }

    // Process full 64-byte blocks directly from the caller's buffer.
    while (len >= 64) {
        processBlock(data);
        data += 64;
        len -= 64;
    }

    // Stash the remainder for next time (or for finalize()).
    if (len > 0) {
        std::memcpy(buffer_, data, len);
        bufferLen_ = len;
    }
}

void Sha1::finalize(uint8_t out[20]) {
    uint64_t bitLen = totalLenBits_;

    // Padding: a single 0x80 byte, then zeros, then the 64-bit bit-length,
    // such that the total length is a multiple of 64 bytes.
    uint8_t pad = 0x80;
    update(&pad, 1);

    uint8_t zero = 0x00;
    while (bufferLen_ != 56) {
        update(&zero, 1);
    }

    uint8_t lenBytes[8];
    for (int i = 0; i < 8; ++i) {
        lenBytes[i] = static_cast<uint8_t>(bitLen >> (56 - 8 * i));
    }
    // Append length directly, bypassing update()'s bit-counting since the
    // length field itself must not be counted.
    std::memcpy(buffer_ + bufferLen_, lenBytes, 8);
    processBlock(buffer_);
    bufferLen_ = 0;

    for (int i = 0; i < 5; ++i) {
        out[i * 4]     = static_cast<uint8_t>(h_[i] >> 24);
        out[i * 4 + 1] = static_cast<uint8_t>(h_[i] >> 16);
        out[i * 4 + 2] = static_cast<uint8_t>(h_[i] >> 8);
        out[i * 4 + 3] = static_cast<uint8_t>(h_[i]);
    }
}

std::string Sha1::toHex(const uint8_t digest[20]) {
    static const char* hexChars = "0123456789abcdef";
    std::string out;
    out.resize(40);
    for (int i = 0; i < 20; ++i) {
        out[i * 2]     = hexChars[(digest[i] >> 4) & 0xF];
        out[i * 2 + 1] = hexChars[digest[i] & 0xF];
    }
    return out;
}

std::string Sha1::hexOf(const uint8_t* data, size_t len) {
    Sha1 s;
    s.update(data, len);
    uint8_t digest[20];
    s.finalize(digest);
    return toHex(digest);
}

bool Sha1::hexOfFile(const std::string& path, std::string& outHex) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return false;

    Sha1 s;
    uint8_t buf[64 * 1024];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) {
        s.update(buf, n);
    }
    bool err = std::ferror(f) != 0;
    std::fclose(f);
    if (err) return false;

    uint8_t digest[20];
    s.finalize(digest);
    outHex = toHex(digest);
    return true;
}

} // namespace p2p
