// tracker/tracker_server.cpp

#include "tracker_server.hpp"
#include "../common/net_util.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>

namespace p2p {

namespace {
// One client's session state, local to its handler thread -- no locking
// needed since only that thread ever touches it. Durable facts (who owns
// which group, etc.) live in TrackerState; this is purely "who is this
// socket logged in as right now".
struct Session {
    bool loggedIn = false;
    std::string userId;
    std::string peerIp;
    uint16_t peerPort = 0;
};

void sendOk(int fd, const std::string& msg) {
    MsgWriter w;
    w.putStr(msg);
    sendMessage(fd, MsgType::RESP_OK, w);
}

void sendErr(int fd, const std::string& msg) {
    MsgWriter w;
    w.putStr(msg);
    sendMessage(fd, MsgType::RESP_ERR, w);
}

void sendResult(int fd, const CommandResult& r) {
    if (r.success) sendOk(fd, r.message);
    else sendErr(fd, r.message);
}
} // namespace

TrackerServer::TrackerServer(TrackerState& state, TrackerSync& sync, std::string bindIp, uint16_t port)
    : state_(state), sync_(sync), bindIp_(std::move(bindIp)), port_(port) {}

void TrackerServer::stop() {
    running_ = false;
    // Closing the listening fd out from under the blocked accept() call in
    // run() (running on a different thread) is the standard way to unblock
    // it on Linux -- accept() returns with EBADF and the loop exits.
    if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }

    {
        std::lock_guard<std::mutex> lock(clientsMu_);
        for (int fd : activeClientFds_) ::shutdown(fd, SHUT_RDWR);
    }
    for (auto& t : clientThreads_) {
        if (t.joinable()) t.join();
    }
}

void TrackerServer::run() {
    Socket listener = listenOn(bindIp_, port_);
    if (!listener.valid()) {
        P2P_LOG_ERROR("tracker: failed to bind client listener on " + bindIp_ + ":" + std::to_string(port_));
        return;
    }
    listenFd_ = listener.release(); // this TrackerServer now solely owns the fd; stop() closes it
    P2P_LOG_INFO("tracker: listening for clients on " + bindIp_ + ":" + std::to_string(port_));

    while (running_) {
        int fd = ::accept(listenFd_, nullptr, nullptr);
        if (fd < 0) {
            if (!running_) break;
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(clientsMu_);
            activeClientFds_.insert(fd);
        }
        clientThreads_.emplace_back([this, fd] { handleClient(fd); });
    }
}

void TrackerServer::handleClient(int fd) {
    Session session;
    Message msg;

    while (recvMessage(fd, msg)) {
        MsgReader r(msg.payload.data(), msg.payload.size());

        switch (msg.type) {
            case MsgType::CREATE_USER: {
                std::string userId = r.getStr();
                std::string passHash = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.createUser(userId, passHash);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }
            case MsgType::LOGIN: {
                std::string userId = r.getStr();
                std::string passHash = r.getStr();
                std::string peerIp = r.getStr();
                uint16_t peerPort = static_cast<uint16_t>(r.getU32());
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                if (session.loggedIn) { sendErr(fd, "already logged in on this connection"); break; }

                std::string err;
                if (!state_.checkCredentials(userId, passHash, err)) { sendErr(fd, err); break; }
                session.loggedIn = true;
                session.userId = userId;
                session.peerIp = peerIp;
                session.peerPort = peerPort;
                sendOk(fd, "login successful");
                P2P_LOG_INFO("tracker: '" + userId + "' logged in from " + peerIp + ":" + std::to_string(peerPort));
                break;
            }
            case MsgType::LOGOUT: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                for (const auto& rec : state_.removeUserFromAllSeeding(session.userId)) sync_.replicateAsync(rec);
                sendOk(fd, "logged out");
                P2P_LOG_INFO("tracker: '" + session.userId + "' logged out");
                session = Session{};
                break;
            }

            case MsgType::CREATE_GROUP: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.createGroup(groupId, session.userId);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }
            case MsgType::JOIN_GROUP: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.joinGroup(groupId, session.userId);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }
            case MsgType::LEAVE_GROUP: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.leaveGroup(groupId, session.userId);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }
            case MsgType::LIST_GROUPS: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                auto groups = state_.listGroups();
                MsgWriter w;
                w.putU32(static_cast<uint32_t>(groups.size()));
                for (const auto& g : groups) {
                    w.putStr(g.id);
                    w.putStr(g.owner);
                    w.putU32(static_cast<uint32_t>(g.memberCount));
                }
                sendMessage(fd, MsgType::LIST_GROUPS_RESP, w);
                break;
            }
            case MsgType::LIST_REQUESTS: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                std::vector<std::string> pending;
                CommandResult res = state_.listRequests(groupId, session.userId, pending);
                if (!res.success) { sendErr(fd, res.message); break; }
                MsgWriter w;
                w.putU32(static_cast<uint32_t>(pending.size()));
                for (const auto& u : pending) w.putStr(u);
                sendMessage(fd, MsgType::LIST_REQUESTS_RESP, w);
                break;
            }
            case MsgType::ACCEPT_REQUEST: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                std::string targetUser = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.acceptRequest(groupId, session.userId, targetUser);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }

            case MsgType::UPLOAD_FILE: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                std::string fileName = r.getStr();
                std::string fileSha1 = r.getStr();
                uint64_t fileSize = r.getU64();
                uint32_t numPieces = r.getU32();
                std::vector<std::string> pieces;
                pieces.reserve(numPieces);
                for (uint32_t i = 0; i < numPieces && r.ok(); ++i) pieces.push_back(r.getStr());
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.addFile(groupId, fileName, fileSize, fileSha1, pieces,
                                                    session.userId, session.peerIp, session.peerPort);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }
            case MsgType::LIST_FILES: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                std::vector<FileSummary> files;
                CommandResult res = state_.listFiles(groupId, session.userId, files);
                if (!res.success) { sendErr(fd, res.message); break; }
                MsgWriter w;
                w.putU32(static_cast<uint32_t>(files.size()));
                for (const auto& f : files) {
                    w.putStr(f.fileName);
                    w.putU64(f.fileSize);
                    w.putU32(f.numPieces);
                    w.putStr(f.fileSha1);
                    w.putU32(static_cast<uint32_t>(f.seederCount));
                }
                sendMessage(fd, MsgType::LIST_FILES_RESP, w);
                break;
            }
            case MsgType::DOWNLOAD_FILE: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                std::string fileName = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                DownloadInfo info;
                CommandResult res = state_.getDownloadInfo(groupId, fileName, session.userId, info);
                if (!res.success) { sendErr(fd, res.message); break; }
                MsgWriter w;
                w.putStr(info.fileSha1);
                w.putU64(info.fileSize);
                w.putU32(info.numPieces);
                for (const auto& h : info.pieceSha1) w.putStr(h);
                w.putU32(static_cast<uint32_t>(info.peers.size()));
                for (const auto& p : info.peers) {
                    w.putStr(p.userId);
                    w.putStr(p.ip);
                    w.putU32(p.port);
                }
                sendMessage(fd, MsgType::DOWNLOAD_FILE_RESP, w);
                break;
            }
            case MsgType::STOP_SHARE: {
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                std::string fileName = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.stopShare(groupId, fileName, session.userId);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }
            case MsgType::REGISTER_SEED: {
                // Internal message: the client sends this once a download
                // finishes, announcing itself as a new full seeder. Not a
                // user-typed command.
                if (!session.loggedIn) { sendErr(fd, "not logged in"); break; }
                std::string groupId = r.getStr();
                std::string fileName = r.getStr();
                if (!r.ok()) { sendErr(fd, "malformed request"); break; }
                CommandResult res = state_.registerSeed(groupId, fileName, session.userId,
                                                          session.peerIp, session.peerPort);
                if (res.producedRecord) sync_.replicateAsync(res.record);
                sendResult(fd, res);
                break;
            }

            default:
                sendErr(fd, "unknown or unexpected message type");
                break;
        }
    }

    // Connection dropped (client crashed, network blip, or a clean close
    // without an explicit `logout`). Treat it exactly like a logout so we
    // never leave a stale seeder pointing at a peer that isn't listening
    // anymore.
    if (session.loggedIn) {
        for (const auto& rec : state_.removeUserFromAllSeeding(session.userId)) sync_.replicateAsync(rec);
        P2P_LOG_INFO("tracker: '" + session.userId + "' disconnected, cleaned up its seeder entries");
    }

    {
        std::lock_guard<std::mutex> lock(clientsMu_);
        activeClientFds_.erase(fd);
    }
    ::close(fd);
}

} // namespace p2p
