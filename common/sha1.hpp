// common/sha1.hpp
//
// A small, self-contained SHA-1 implementation (per RFC 3174).
//
// Why hand-rolled instead of linking OpenSSL: the assignment forbids
// "high-level libraries" and the intent is clearly to make us implement our
// own integrity checking rather than call a library function. This is also
// deliberately *not* used for anything security-critical in the adversarial
// sense (we use it for piece/file integrity, and -- with an honestly-stated
// caveat in the README -- for password storage). SHA1 is cryptographically
// broken for collision resistance but is exactly what the spec asks for.

#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

namespace p2p {

class Sha1 {
public:
    Sha1() { reset(); }

    void reset();
    void update(const uint8_t* data, size_t len);
    void update(const std::string& s) { update(reinterpret_cast<const uint8_t*>(s.data()), s.size()); }

    // Finalizes the hash (destructive -- call once) and writes 20 raw bytes.
    void finalize(uint8_t out[20]);

    // Convenience: hash a whole in-memory buffer to a lowercase hex string.
    static std::string hexOf(const uint8_t* data, size_t len);
    static std::string hexOf(const std::string& s) {
        return hexOf(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }
    static std::string toHex(const uint8_t digest[20]);

    // Hash a file on disk in streaming fashion (constant memory), used for
    // whole-file integrity verification of files up to 1GB.
    // Returns false if the file could not be opened.
    static bool hexOfFile(const std::string& path, std::string& outHex);

private:
    void processBlock(const uint8_t block[64]);

    uint32_t h_[5];
    uint64_t totalLenBits_;
    uint8_t buffer_[64];
    size_t bufferLen_;
};

} // namespace p2p
