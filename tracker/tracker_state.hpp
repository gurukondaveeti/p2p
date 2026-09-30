// tracker/tracker_state.hpp
//
// The tracker's durable state (accounts, groups, shared-file metadata) plus
// the replicated operation log that backs it. See oplog.hpp for the design
// rationale of the replication/recovery scheme.
//
// Locking model: a single std::shared_mutex guards everything. Queries take
// a shared (read) lock; every mutation takes a unique (write) lock, builds
// an OpRecord, applies it, appends it to the in-memory log and to disk, all
// atomically with respect to other threads. This is coarse-grained but the
// assignment's scale (a handful of users/groups/files, up to 3 clients) and
// state size make a single lock the simplest correct choice -- there is no
// hot-path contention to optimize away, and fine-grained per-entity locking
// would only add deadlock risk for no measurable benefit here.
//
// State-changing entry points ("commands") always run the check ("is this
// allowed?") and the mutation atomically under the same write lock, so two
// racing clients can never both win a check that only one of them should
// (e.g. two `create_group g1` calls landing at the same instant on the same
// tracker cannot both succeed).

#pragma once

#include "entities.hpp"
#include "oplog.hpp"

#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>

namespace p2p {

struct GroupSummary {
    std::string id;
    std::string owner;
    size_t memberCount;
};

struct FileSummary {
    std::string fileName;
    uint64_t fileSize;
    uint32_t numPieces;
    std::string fileSha1;
    size_t seederCount;
};

struct DownloadInfo {
    std::string fileSha1;
    uint64_t fileSize;
    uint32_t numPieces;
    std::vector<std::string> pieceSha1;
    std::vector<SeederInfo> peers;
};

// Result of a command: whether it was allowed, a human-readable message for
// the client, and -- if it mutated durable state -- the OpRecord that was
// generated so the caller (tracker_server) can hand it to TrackerSync for
// live replication to the peer tracker (outside of TrackerState's lock).
struct CommandResult {
    bool success = false;
    std::string message;
    bool producedRecord = false;
    OpRecord record;
};

class TrackerState {
public:
    TrackerState(uint32_t myTrackerNo, std::string logPath);

    // Replays the on-disk log (if any) to rebuild in-memory state. Call
    // once at startup before serving any client or sync connections.
    void loadFromDisk();

    // ---- Commands (mutating) ----
    CommandResult createUser(const std::string& userId, const std::string& passwordHash);
    CommandResult createGroup(const std::string& groupId, const std::string& ownerId);
    CommandResult joinGroup(const std::string& groupId, const std::string& userId);
    CommandResult leaveGroup(const std::string& groupId, const std::string& userId);
    CommandResult acceptRequest(const std::string& groupId, const std::string& ownerId, const std::string& targetUserId);
    CommandResult addFile(const std::string& groupId, const std::string& fileName, uint64_t fileSize,
                           const std::string& fileSha1, const std::vector<std::string>& pieceHashes,
                           const std::string& uploaderId, const std::string& uploaderIp, uint16_t uploaderPort);
    CommandResult registerSeed(const std::string& groupId, const std::string& fileName,
                                const std::string& userId, const std::string& ip, uint16_t port);
    CommandResult stopShare(const std::string& groupId, const std::string& fileName, const std::string& userId);

    // Drops `userId` from every file's seeder list (used on logout). Each
    // removal is its own replicated op -- the caller must forward each
    // returned record to TrackerSync so peers hear about the departure too.
    std::vector<OpRecord> removeUserFromAllSeeding(const std::string& userId);

    // ---- Queries (read-only) ----
    bool checkCredentials(const std::string& userId, const std::string& passwordHash, std::string& errMsg) const;
    bool userExists(const std::string& userId) const;
    std::vector<GroupSummary> listGroups() const;
    CommandResult listRequests(const std::string& groupId, const std::string& requesterId, std::vector<std::string>& out) const;
    CommandResult listFiles(const std::string& groupId, const std::string& requesterId, std::vector<FileSummary>& out) const;
    CommandResult getDownloadInfo(const std::string& groupId, const std::string& fileName,
                                   const std::string& requesterId, DownloadInfo& out) const;

    // ---- Replication support (used by TrackerSync) ----
    uint32_t myTrackerNo() const { return myTrackerNo_; }
    uint64_t lastSeqForOrigin(uint32_t origin) const;
    // All records this tracker holds for `origin` with seq > afterSeq, in
    // order -- i.e. "what the peer is missing", for anti-entropy catch-up.
    std::vector<OpRecord> recordsSince(uint32_t origin, uint64_t afterSeq) const;
    // Applies a record received live (SYNC_OP) or during catch-up from the
    // peer tracker. Idempotent: a record whose seq we've already applied
    // for that origin is silently ignored.
    void applyRemoteRecord(const OpRecord& rec);

private:
    static std::string fileKey(const std::string& groupId, const std::string& fileName) {
        return groupId + "\x1f" + fileName;
    }

    // Applies a record's effect to users_/groups_/files_. Assumes the write
    // lock is already held. Used for local ops, remote ops, and disk replay
    // alike -- the single place that knows how to interpret each OpType.
    void applyLocked(const OpRecord& rec);

    // Builds a fresh local OpRecord (origin = myTrackerNo_, seq = next),
    // applies it, appends it to the log and to disk. Assumes the write lock
    // is already held. Returns the record for the caller to replicate.
    OpRecord commitLocked(OpType type, std::vector<uint8_t> fields);

    mutable std::shared_mutex mu_;
    uint32_t myTrackerNo_;
    uint64_t nextSeq_ = 1;
    uint64_t lastSeq_[2] = {0, 0};
    std::vector<OpRecord> log_;
    std::string logPath_;
    std::FILE* logFile_ = nullptr;

    std::unordered_map<std::string, User> users_;
    std::unordered_map<std::string, Group> groups_;
    std::unordered_map<std::string, SharedFile> files_; // key = fileKey(groupId, fileName)
};

} // namespace p2p
