// tracker/tracker_state.cpp

#include "tracker_state.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <algorithm>

namespace p2p {

namespace {
// Last-Writer-Wins tie-break used whenever two trackers independently create
// an entity with the same key while partitioned. "Earlier" wins so both
// sides converge to the identical winner regardless of application order:
// smaller seq wins; a tie (impossible in practice, since seq is only ever
// compared between different origins) falls back to smaller origin.
bool isEarlier(uint32_t origin, uint64_t seq, uint32_t existingOrigin, uint64_t existingSeq) {
    if (seq != existingSeq) return seq < existingSeq;
    return origin < existingOrigin;
}
} // namespace

TrackerState::TrackerState(uint32_t myTrackerNo, std::string logPath)
    : myTrackerNo_(myTrackerNo), logPath_(std::move(logPath)) {}

void TrackerState::loadFromDisk() {
    std::vector<OpRecord> records;
    readAllRecordsFromFile(logPath_, records);

    std::unique_lock<std::shared_mutex> lock(mu_);
    for (const auto& rec : records) {
        applyLocked(rec);
        log_.push_back(rec);
        if (rec.seq > lastSeq_[rec.originTracker]) lastSeq_[rec.originTracker] = rec.seq;
    }
    nextSeq_ = lastSeq_[myTrackerNo_] + 1;

    // Reopen the log in append mode for future commits. Using "ab" (not
    // "wb") is essential -- it preserves everything we just replayed.
    logFile_ = std::fopen(logPath_.c_str(), "ab");
    if (!logFile_) {
        P2P_LOG_ERROR("failed to open oplog file for append: " + logPath_);
    }
    P2P_LOG_INFO("replayed " + std::to_string(records.size()) + " op-log record(s) from " + logPath_);
}

OpRecord TrackerState::commitLocked(OpType type, std::vector<uint8_t> fields) {
    OpRecord rec;
    rec.originTracker = myTrackerNo_;
    rec.seq = nextSeq_++;
    rec.opType = type;
    rec.fields = std::move(fields);

    applyLocked(rec);
    log_.push_back(rec);
    lastSeq_[myTrackerNo_] = rec.seq;
    if (logFile_) appendRecordToFile(logFile_, rec);
    return rec;
}

void TrackerState::applyLocked(const OpRecord& rec) {
    MsgReader r(rec.fields.data(), rec.fields.size());

    switch (rec.opType) {
        case OpType::CREATE_USER: {
            std::string id = r.getStr();
            std::string passHash = r.getStr();
            if (!r.ok()) return;

            auto it = users_.find(id);
            if (it == users_.end()) {
                User u;
                u.id = id;
                u.passwordHash = passHash;
                u.creatorOrigin = rec.originTracker;
                u.creatorSeq = rec.seq;
                users_.emplace(id, std::move(u));
            } else if (isEarlier(rec.originTracker, rec.seq, it->second.creatorOrigin, it->second.creatorSeq)) {
                it->second.passwordHash = passHash;
                it->second.creatorOrigin = rec.originTracker;
                it->second.creatorSeq = rec.seq;
            }
            break;
        }
        case OpType::CREATE_GROUP: {
            std::string id = r.getStr();
            std::string owner = r.getStr();
            if (!r.ok()) return;

            auto it = groups_.find(id);
            if (it == groups_.end()) {
                Group g;
                g.id = id;
                g.owner = owner;
                g.members.insert(owner);
                g.creatorOrigin = rec.originTracker;
                g.creatorSeq = rec.seq;
                groups_.emplace(id, std::move(g));
            } else if (isEarlier(rec.originTracker, rec.seq, it->second.creatorOrigin, it->second.creatorSeq)) {
                it->second.owner = owner;
                it->second.members.clear();
                it->second.members.insert(owner);
                it->second.pendingRequests.clear();
                it->second.creatorOrigin = rec.originTracker;
                it->second.creatorSeq = rec.seq;
            }
            break;
        }
        case OpType::JOIN_REQUEST: {
            std::string groupId = r.getStr();
            std::string userId = r.getStr();
            if (!r.ok()) return;
            auto it = groups_.find(groupId);
            if (it == groups_.end()) return; // group vanished (e.g. LWW loss) -- ignore defensively
            Group& g = it->second;
            if (g.members.count(userId)) return; // already a member, nothing to do
            if (std::find(g.pendingRequests.begin(), g.pendingRequests.end(), userId) != g.pendingRequests.end())
                return; // idempotent re-apply
            g.pendingRequests.push_back(userId);
            break;
        }
        case OpType::ACCEPT_REQUEST: {
            std::string groupId = r.getStr();
            std::string userId = r.getStr();
            if (!r.ok()) return;
            auto it = groups_.find(groupId);
            if (it == groups_.end()) return;
            Group& g = it->second;
            auto pit = std::find(g.pendingRequests.begin(), g.pendingRequests.end(), userId);
            if (pit != g.pendingRequests.end()) g.pendingRequests.erase(pit);
            g.members.insert(userId); // idempotent regardless
            break;
        }
        case OpType::LEAVE_GROUP: {
            std::string groupId = r.getStr();
            std::string userId = r.getStr();
            if (!r.ok()) return;
            auto it = groups_.find(groupId);
            if (it == groups_.end()) return;
            it->second.members.erase(userId);
            if (it->second.members.empty()) groups_.erase(it); // auto-cleanup an emptied group
            break;
        }
        case OpType::ADD_FILE: {
            std::string groupId = r.getStr();
            std::string fileName = r.getStr();
            std::string fileSha1 = r.getStr();
            uint64_t fileSize = r.getU64();
            uint32_t numPieces = r.getU32();
            std::vector<std::string> pieces;
            pieces.reserve(numPieces);
            for (uint32_t i = 0; i < numPieces && r.ok(); ++i) pieces.push_back(r.getStr());
            std::string uploaderId = r.getStr();
            std::string uploaderIp = r.getStr();
            uint16_t uploaderPort = static_cast<uint16_t>(r.getU32());
            if (!r.ok()) return;

            std::string key = fileKey(groupId, fileName);
            auto it = files_.find(key);
            if (it == files_.end()) {
                SharedFile f;
                f.fileSha1 = fileSha1;
                f.groupId = groupId;
                f.fileName = fileName;
                f.fileSize = fileSize;
                f.numPieces = numPieces;
                f.pieceSha1 = std::move(pieces);
                f.seeders.push_back(SeederInfo{uploaderId, uploaderIp, uploaderPort});
                f.creatorOrigin = rec.originTracker;
                f.creatorSeq = rec.seq;
                files_.emplace(key, std::move(f));
            } else if (it->second.fileSha1 == fileSha1) {
                // Re-announcement of identical content (possibly by another
                // member, or a replay) -- just add/update the seeder.
                auto sit = std::find_if(it->second.seeders.begin(), it->second.seeders.end(),
                                         [&](const SeederInfo& s) { return s.userId == uploaderId; });
                if (sit == it->second.seeders.end())
                    it->second.seeders.push_back(SeederInfo{uploaderId, uploaderIp, uploaderPort});
                else
                    *sit = SeederInfo{uploaderId, uploaderIp, uploaderPort};
            } else if (isEarlier(rec.originTracker, rec.seq, it->second.creatorOrigin, it->second.creatorSeq)) {
                // Genuine name collision with different content, created
                // concurrently on both trackers during a partition. LWW.
                it->second.fileSha1 = fileSha1;
                it->second.fileSize = fileSize;
                it->second.numPieces = numPieces;
                it->second.pieceSha1 = std::move(pieces);
                it->second.seeders.clear();
                it->second.seeders.push_back(SeederInfo{uploaderId, uploaderIp, uploaderPort});
                it->second.creatorOrigin = rec.originTracker;
                it->second.creatorSeq = rec.seq;
            }
            break;
        }
        case OpType::REGISTER_SEED: {
            std::string groupId = r.getStr();
            std::string fileName = r.getStr();
            std::string userId = r.getStr();
            std::string ip = r.getStr();
            uint16_t port = static_cast<uint16_t>(r.getU32());
            if (!r.ok()) return;
            auto it = files_.find(fileKey(groupId, fileName));
            if (it == files_.end()) return;
            auto sit = std::find_if(it->second.seeders.begin(), it->second.seeders.end(),
                                     [&](const SeederInfo& s) { return s.userId == userId; });
            if (sit == it->second.seeders.end())
                it->second.seeders.push_back(SeederInfo{userId, ip, port});
            else
                *sit = SeederInfo{userId, ip, port};
            break;
        }
        case OpType::REMOVE_SEED: {
            std::string groupId = r.getStr();
            std::string fileName = r.getStr();
            std::string userId = r.getStr();
            if (!r.ok()) return;
            auto it = files_.find(fileKey(groupId, fileName));
            if (it == files_.end()) return;
            auto& seeders = it->second.seeders;
            seeders.erase(std::remove_if(seeders.begin(), seeders.end(),
                                          [&](const SeederInfo& s) { return s.userId == userId; }),
                          seeders.end());
            break;
        }
    }
}

// ---- Commands ----------------------------------------------------------

CommandResult TrackerState::createUser(const std::string& userId, const std::string& passwordHash) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (users_.count(userId)) return {false, "user '" + userId + "' already exists", false, {}};

    MsgWriter w;
    w.putStr(userId);
    w.putStr(passwordHash);
    OpRecord rec = commitLocked(OpType::CREATE_USER, w.buffer());
    return {true, "user created", true, rec};
}

CommandResult TrackerState::createGroup(const std::string& groupId, const std::string& ownerId) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (groups_.count(groupId)) return {false, "group '" + groupId + "' already exists", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(ownerId);
    OpRecord rec = commitLocked(OpType::CREATE_GROUP, w.buffer());
    return {true, "group created", true, rec};
}

CommandResult TrackerState::joinGroup(const std::string& groupId, const std::string& userId) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto it = groups_.find(groupId);
    if (it == groups_.end()) return {false, "no such group", false, {}};
    if (it->second.members.count(userId)) return {false, "already a member of this group", false, {}};
    if (std::find(it->second.pendingRequests.begin(), it->second.pendingRequests.end(), userId) !=
        it->second.pendingRequests.end())
        return {false, "join request already pending", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(userId);
    OpRecord rec = commitLocked(OpType::JOIN_REQUEST, w.buffer());
    return {true, "join request submitted, awaiting owner approval", true, rec};
}

CommandResult TrackerState::leaveGroup(const std::string& groupId, const std::string& userId) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto it = groups_.find(groupId);
    if (it == groups_.end()) return {false, "no such group", false, {}};
    if (!it->second.members.count(userId)) return {false, "you are not a member of this group", false, {}};
    if (userId == it->second.owner && it->second.members.size() > 1)
        return {false, "owner cannot leave while other members remain", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(userId);
    OpRecord rec = commitLocked(OpType::LEAVE_GROUP, w.buffer());
    return {true, "left group", true, rec};
}

CommandResult TrackerState::acceptRequest(const std::string& groupId, const std::string& ownerId,
                                           const std::string& targetUserId) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto it = groups_.find(groupId);
    if (it == groups_.end()) return {false, "no such group", false, {}};
    if (it->second.owner != ownerId) return {false, "only the group owner can accept requests", false, {}};
    if (std::find(it->second.pendingRequests.begin(), it->second.pendingRequests.end(), targetUserId) ==
        it->second.pendingRequests.end())
        return {false, "no pending request from that user", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(targetUserId);
    OpRecord rec = commitLocked(OpType::ACCEPT_REQUEST, w.buffer());
    return {true, "request accepted", true, rec};
}

CommandResult TrackerState::addFile(const std::string& groupId, const std::string& fileName, uint64_t fileSize,
                                     const std::string& fileSha1, const std::vector<std::string>& pieceHashes,
                                     const std::string& uploaderId, const std::string& uploaderIp,
                                     uint16_t uploaderPort) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto git = groups_.find(groupId);
    if (git == groups_.end()) return {false, "no such group", false, {}};
    if (!git->second.members.count(uploaderId)) return {false, "you are not a member of this group", false, {}};

    auto fit = files_.find(fileKey(groupId, fileName));
    if (fit != files_.end() && fit->second.fileSha1 != fileSha1)
        return {false, "a different file named '" + fileName + "' already exists in this group", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(fileName);
    w.putStr(fileSha1);
    w.putU64(fileSize);
    w.putU32(static_cast<uint32_t>(pieceHashes.size()));
    for (const auto& h : pieceHashes) w.putStr(h);
    w.putStr(uploaderId);
    w.putStr(uploaderIp);
    w.putU32(uploaderPort);
    OpRecord rec = commitLocked(OpType::ADD_FILE, w.buffer());
    return {true, "file shared with group", true, rec};
}

CommandResult TrackerState::registerSeed(const std::string& groupId, const std::string& fileName,
                                          const std::string& userId, const std::string& ip, uint16_t port) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto fit = files_.find(fileKey(groupId, fileName));
    if (fit == files_.end()) return {false, "no such file in group", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(fileName);
    w.putStr(userId);
    w.putStr(ip);
    w.putU32(port);
    OpRecord rec = commitLocked(OpType::REGISTER_SEED, w.buffer());
    return {true, "registered as seed", true, rec};
}

CommandResult TrackerState::stopShare(const std::string& groupId, const std::string& fileName,
                                       const std::string& userId) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto fit = files_.find(fileKey(groupId, fileName));
    if (fit == files_.end()) return {false, "no such file in group", false, {}};
    bool isSeeding = std::any_of(fit->second.seeders.begin(), fit->second.seeders.end(),
                                  [&](const SeederInfo& s) { return s.userId == userId; });
    if (!isSeeding) return {false, "you are not sharing this file", false, {}};

    MsgWriter w;
    w.putStr(groupId);
    w.putStr(fileName);
    w.putStr(userId);
    OpRecord rec = commitLocked(OpType::REMOVE_SEED, w.buffer());
    return {true, "stopped sharing", true, rec};
}

std::vector<OpRecord> TrackerState::removeUserFromAllSeeding(const std::string& userId) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    // Snapshot keys first -- commitLocked doesn't touch files_'s key set,
    // but iterating and mutating unrelated map entries at once is fragile
    // to reason about, so keep it simple and explicit.
    std::vector<std::pair<std::string, std::string>> targets; // (groupId, fileName)
    for (auto& [key, file] : files_) {
        bool seeding = std::any_of(file.seeders.begin(), file.seeders.end(),
                                    [&](const SeederInfo& s) { return s.userId == userId; });
        if (seeding) targets.emplace_back(file.groupId, file.fileName);
    }
    std::vector<OpRecord> produced;
    for (auto& [groupId, fileName] : targets) {
        MsgWriter w;
        w.putStr(groupId);
        w.putStr(fileName);
        w.putStr(userId);
        produced.push_back(commitLocked(OpType::REMOVE_SEED, w.buffer()));
    }
    return produced;
}

// ---- Queries ------------------------------------------------------------

bool TrackerState::checkCredentials(const std::string& userId, const std::string& passwordHash,
                                     std::string& errMsg) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = users_.find(userId);
    if (it == users_.end()) { errMsg = "no such user"; return false; }
    if (it->second.passwordHash != passwordHash) { errMsg = "incorrect password"; return false; }
    return true;
}

bool TrackerState::userExists(const std::string& userId) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return users_.count(userId) != 0;
}

std::vector<GroupSummary> TrackerState::listGroups() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<GroupSummary> out;
    out.reserve(groups_.size());
    for (const auto& [id, g] : groups_) out.push_back({g.id, g.owner, g.members.size()});
    std::sort(out.begin(), out.end(), [](const GroupSummary& a, const GroupSummary& b) { return a.id < b.id; });
    return out;
}

CommandResult TrackerState::listRequests(const std::string& groupId, const std::string& requesterId,
                                          std::vector<std::string>& out) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = groups_.find(groupId);
    if (it == groups_.end()) return {false, "no such group", false, {}};
    if (it->second.owner != requesterId) return {false, "only the group owner can list requests", false, {}};
    out = it->second.pendingRequests;
    return {true, "ok", false, {}};
}

CommandResult TrackerState::listFiles(const std::string& groupId, const std::string& requesterId,
                                       std::vector<FileSummary>& out) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto git = groups_.find(groupId);
    if (git == groups_.end()) return {false, "no such group", false, {}};
    if (!git->second.members.count(requesterId)) return {false, "you are not a member of this group", false, {}};

    for (const auto& [key, f] : files_) {
        if (f.groupId != groupId) continue;
        out.push_back({f.fileName, f.fileSize, f.numPieces, f.fileSha1, f.seeders.size()});
    }
    std::sort(out.begin(), out.end(), [](const FileSummary& a, const FileSummary& b) { return a.fileName < b.fileName; });
    return {true, "ok", false, {}};
}

CommandResult TrackerState::getDownloadInfo(const std::string& groupId, const std::string& fileName,
                                             const std::string& requesterId, DownloadInfo& out) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto git = groups_.find(groupId);
    if (git == groups_.end()) return {false, "no such group", false, {}};
    if (!git->second.members.count(requesterId)) return {false, "you are not a member of this group", false, {}};

    auto fit = files_.find(fileKey(groupId, fileName));
    if (fit == files_.end()) return {false, "no such file in group", false, {}};

    out.fileSha1 = fit->second.fileSha1;
    out.fileSize = fit->second.fileSize;
    out.numPieces = fit->second.numPieces;
    out.pieceSha1 = fit->second.pieceSha1;
    for (const auto& s : fit->second.seeders) {
        if (s.userId == requesterId) continue; // no point downloading from yourself
        out.peers.push_back(s);
    }
    return {true, "ok", false, {}};
}

// ---- Replication support -------------------------------------------------

uint64_t TrackerState::lastSeqForOrigin(uint32_t origin) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return lastSeq_[origin];
}

std::vector<OpRecord> TrackerState::recordsSince(uint32_t origin, uint64_t afterSeq) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<OpRecord> out;
    for (const auto& rec : log_) {
        if (rec.originTracker == origin && rec.seq > afterSeq) out.push_back(rec);
    }
    return out;
}

void TrackerState::applyRemoteRecord(const OpRecord& rec) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (rec.seq <= lastSeq_[rec.originTracker]) return; // already applied -- idempotent skip
    applyLocked(rec);
    log_.push_back(rec);
    lastSeq_[rec.originTracker] = rec.seq;
    if (logFile_) appendRecordToFile(logFile_, rec);
}

} // namespace p2p
