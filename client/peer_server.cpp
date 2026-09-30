// client/peer_server.cpp

#include "peer_server.hpp"
#include "../common/net_util.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>

namespace p2p {

PeerServer::PeerServer(FileRegistry& registry, std::string bindIp, uint16_t port)
    : registry_(registry), bindIp_(std::move(bindIp)), port_(port) {}

bool PeerServer::start() {
    Socket listener = listenOn(bindIp_, port_);
    if (!listener.valid()) {
        P2P_LOG_ERROR("peer-server: failed to bind on " + bindIp_ + ":" + std::to_string(port_));
        return false;
    }
    listenFd_ = listener.release();
    running_ = true;
    thread_ = std::thread([this] { acceptLoop(listenFd_); });
    P2P_LOG_INFO("peer-server: serving pieces on " + bindIp_ + ":" + std::to_string(port_));
    return true;
}

void PeerServer::stop() {
    running_ = false;
    if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }
    if (thread_.joinable()) thread_.join();
}

void PeerServer::acceptLoop(int listenFd) {
    while (running_) {
        int fd = ::accept(listenFd, nullptr, nullptr);
        if (fd < 0) {
            if (!running_) break;
            continue;
        }
        std::thread(&PeerServer::handlePeer, this, fd).detach();
    }
}

void PeerServer::handlePeer(int fd) {
    Message msg;
    while (recvMessage(fd, msg)) {
        MsgReader r(msg.payload.data(), msg.payload.size());

        if (msg.type == MsgType::BITFIELD_REQUEST) {
            std::string fileSha1 = r.getStr();
            if (!r.ok()) { sendMessage(fd, MsgType::PIECE_NOTFOUND); continue; }

            auto lf = registry_.find(fileSha1);
            if (!lf) { sendMessage(fd, MsgType::PIECE_NOTFOUND); continue; }

            std::vector<uint8_t> bits = lf->snapshotBitfield();
            MsgWriter w;
            w.putU32(lf->numPieces);
            w.putBytes(bits.data(), static_cast<uint32_t>(bits.size()));
            sendMessage(fd, MsgType::BITFIELD_RESPONSE, w);

        } else if (msg.type == MsgType::PIECE_REQUEST) {
            std::string fileSha1 = r.getStr();
            uint32_t pieceIdx = r.getU32();
            if (!r.ok()) { sendMessage(fd, MsgType::PIECE_NOTFOUND); continue; }

            auto lf = registry_.find(fileSha1);
            if (!lf || pieceIdx >= lf->numPieces || !lf->hasPiece(pieceIdx)) {
                sendMessage(fd, MsgType::PIECE_NOTFOUND);
                continue;
            }

            uint64_t offset = static_cast<uint64_t>(pieceIdx) * kPieceSize;
            uint64_t len = pieceLength(lf->fileSize, pieceIdx, lf->numPieces);

            // Open fresh + pread + close per request: no shared mutable fd
            // state, so many peers can be served the same file concurrently
            // with zero locking around the I/O itself.
            int fileFd = ::open(lf->diskPath.c_str(), O_RDONLY);
            if (fileFd < 0) { sendMessage(fd, MsgType::PIECE_NOTFOUND); continue; }

            std::vector<uint8_t> buf(len);
            ssize_t n = ::pread(fileFd, buf.data(), len, static_cast<off_t>(offset));
            ::close(fileFd);
            if (n < 0 || static_cast<uint64_t>(n) != len) {
                sendMessage(fd, MsgType::PIECE_NOTFOUND);
                continue;
            }

            MsgWriter w;
            w.putU32(pieceIdx);
            w.putBytes(buf.data(), static_cast<uint32_t>(buf.size()));
            sendMessage(fd, MsgType::PIECE_RESPONSE, w);

        } else {
            sendMessage(fd, MsgType::PIECE_NOTFOUND);
        }
    }
    ::close(fd);
}

} // namespace p2p
