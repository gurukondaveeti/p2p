// tracker/oplog.hpp
//
// The replicated operation log that underlies tracker synchronization.
//
// Every durable state change (not session state like "who is logged in
// right now" -- just durable facts: accounts, groups, file/seeder
// metadata) is captured as one OpRecord tagged with WHO produced it
// (originTracker: 0 or 1) and WHEN in that tracker's own local history
// (seq: a per-origin monotonically increasing counter, i.e. a Lamport-style
// logical clock). This is deliberately simple write-ahead-log/event-sourcing
// design rather than a full consensus protocol (Raft/Paxos):
//
//   - Only two trackers ever exist, so there's no quorum/leader-election
//     problem to solve -- both are always "primary" for writes they see
//     directly from a client.
//   - Applying a record is idempotent (keyed by the record's natural
//     primary key, e.g. user id / group id / file hash), so replaying the
//     same record twice -- which WILL happen, e.g. a record already
//     applied locally is also fetched back during a full catch-up sync --
//     is harmless.
//   - The one genuine conflict case is two trackers independently creating
//     an entity with the *same* key while partitioned from each other
//     (e.g. both accept `create_group g1` from different clients while the
//     inter-tracker link is down). We resolve this deterministically with
//     Last-Writer-Wins on the (seq, origin) pair -- the same pattern used
//     by Cassandra/Riak/CRDT registers -- so both trackers converge to the
//     identical winner once they exchange logs, regardless of which one
//     applied first. See TrackerState::applyRecord.
//
// The log is also the recovery mechanism for both failure modes the
// assignment asks for:
//   - Process restart: on startup a tracker replays its own on-disk log
//     (kOpLogFile) to rebuild in-memory state before accepting connections.
//   - Network partition heals: on (re)connecting, trackers exchange
//     "what's your highest seq per origin?" (SYNC_HELLO) and then each
//     streams the records the other is missing (SYNC_OP) -- classic
//     anti-entropy / gossip catch-up.

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <cstdio>

namespace p2p {

enum class OpType : uint8_t {
    CREATE_USER = 1,
    CREATE_GROUP,
    JOIN_REQUEST,     // a user asked to join a group (-> pendingRequests)
    ACCEPT_REQUEST,   // owner accepted a pending request (-> members)
    LEAVE_GROUP,      // a member left (or an owner's group is untouched -- see tracker_state)
    ADD_FILE,         // a file was shared into a group, with the uploader as first seeder
    REGISTER_SEED,    // a peer announces it now has (some/all of) a file
    REMOVE_SEED,      // a peer stopped sharing / logged out
};

const char* opTypeName(OpType t);

struct OpRecord {
    uint32_t originTracker = 0; // 0 or 1
    uint64_t seq = 0;           // monotonic per-origin
    OpType opType = OpType::CREATE_USER;
    std::vector<uint8_t> fields; // opType-specific payload, MsgWriter-encoded
};

// Serializes one record to a self-delimited byte blob (u32 len prefix +
// origin/seq/type/fields), suitable both for on-disk persistence and for
// embedding as a SYNC_OP message payload.
std::vector<uint8_t> encodeOpRecord(const OpRecord& rec);
bool decodeOpRecord(const uint8_t* data, size_t len, OpRecord& out);

// Appends one record to an open append-mode log file, flushing so a crash
// right after this call still has the record durable on disk.
bool appendRecordToFile(std::FILE* f, const OpRecord& rec);

// Reads every record from a log file from the start (used at startup to
// rebuild state). Returns true even for an empty/nonexistent file.
bool readAllRecordsFromFile(const std::string& path, std::vector<OpRecord>& out);

} // namespace p2p
