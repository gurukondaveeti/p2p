// tracker/entities.hpp
//
// Plain data held by the tracker. No logic lives here -- see
// tracker_state.hpp for the operations that read/mutate these under lock.

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <set>

namespace p2p {

// A user account. Passwords are never stored in plaintext -- only a SHA1
// hash. (Honest caveat, documented in the README: SHA1 with no per-user
// salt is not how you'd store passwords in a real system; it satisfies the
// assignment's integrity-hashing spirit but isn't a production KDF.)
struct User {
    std::string id;
    std::string passwordHash;
    uint32_t creatorOrigin = 0; // which tracker first created this record
    uint64_t creatorSeq = 0;    // ...and at what point in its op log
};

struct Group {
    std::string id;
    std::string owner;
    std::set<std::string> members;            // includes the owner
    std::vector<std::string> pendingRequests;  // join requests awaiting owner
    uint32_t creatorOrigin = 0;
    uint64_t creatorSeq = 0;
};

// A peer known to (possibly) hold some or all pieces of a file. The tracker
// itself never verifies this claim -- piece-level truth is established
// peer-to-peer via bitfield exchange and per-piece SHA1 checks.
struct SeederInfo {
    std::string userId;
    std::string ip;
    uint16_t port = 0;

    bool operator<(const SeederInfo& o) const { return userId < o.userId; }
};

struct SharedFile {
    std::string fileSha1;      // primary key -- content-addressed
    std::string groupId;
    std::string fileName;
    uint64_t fileSize = 0;
    uint32_t numPieces = 0;
    std::vector<std::string> pieceSha1; // hex, one per piece
    std::vector<SeederInfo> seeders;    // de-duplicated by userId
    uint32_t creatorOrigin = 0;
    uint64_t creatorSeq = 0;
};

} // namespace p2p
