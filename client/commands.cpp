// client/commands.cpp

#include "commands.hpp"
#include "uploader.hpp"
#include "../common/sha1.hpp"
#include "../common/protocol.hpp"
#include "../common/common.hpp"

#include <iostream>
#include <sstream>
#include <vector>

namespace p2p {

namespace {

std::vector<std::string> tokenize(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string tok;
    while (iss >> tok) tokens.push_back(tok);
    return tokens;
}

// Sends `type` with `payload` and prints the tracker's OK/ERR message
// verbatim. Used for every command that just wants a pass/fail + reason.
void doSimpleRequest(ClientContext& ctx, MsgType type, const MsgWriter& payload) {
    Message resp;
    if (!ctx.tracker.request(type, payload, resp)) {
        std::cout << "error: could not reach any tracker\n";
        return;
    }
    MsgReader r(resp.payload.data(), resp.payload.size());
    std::string msg = r.getStr();
    std::cout << (resp.type == MsgType::RESP_OK ? "OK: " : "ERROR: ") << msg << "\n";
}

bool requireLogin(ClientContext& ctx) {
    if (!ctx.tracker.loggedIn()) {
        std::cout << "you must log in first (login <user_id> <password>)\n";
        return false;
    }
    return true;
}

void cmdCreateUser(ClientContext& ctx, const std::vector<std::string>& t) {
    if (t.size() != 3) { std::cout << "usage: create_user <user_id> <password>\n"; return; }
    MsgWriter w;
    w.putStr(t[1]);
    w.putStr(Sha1::hexOf(t[2])); // password never leaves the client in plaintext
    doSimpleRequest(ctx, MsgType::CREATE_USER, w);
}

void cmdLogin(ClientContext& ctx, const std::vector<std::string>& t) {
    if (t.size() != 3) { std::cout << "usage: login <user_id> <password>\n"; return; }
    if (ctx.tracker.loggedIn()) { std::cout << "already logged in as '" << ctx.tracker.userId() << "'\n"; return; }

    std::string passHash = Sha1::hexOf(t[2]);
    MsgWriter w;
    w.putStr(t[1]);
    w.putStr(passHash);
    w.putStr(ctx.myIp);
    w.putU32(ctx.myPort);

    Message resp;
    if (!ctx.tracker.request(MsgType::LOGIN, w, resp)) { std::cout << "error: could not reach any tracker\n"; return; }
    MsgReader r(resp.payload.data(), resp.payload.size());
    std::string msg = r.getStr();
    if (resp.type == MsgType::RESP_OK) {
        ctx.tracker.onLoginSuccess(t[1], passHash, ctx.myIp, ctx.myPort);
        std::cout << "OK: " << msg << "\n";
    } else {
        std::cout << "ERROR: " << msg << "\n";
    }
}

void cmdLogout(ClientContext& ctx) {
    if (!requireLogin(ctx)) return;
    Message resp;
    if (!ctx.tracker.request(MsgType::LOGOUT, MsgWriter{}, resp)) { std::cout << "error: could not reach any tracker\n"; return; }
    MsgReader r(resp.payload.data(), resp.payload.size());
    std::cout << (resp.type == MsgType::RESP_OK ? "OK: " : "ERROR: ") << r.getStr() << "\n";
    if (resp.type == MsgType::RESP_OK) ctx.tracker.onLogout();
}

void cmdListGroups(ClientContext& ctx) {
    if (!requireLogin(ctx)) return;
    Message resp;
    if (!ctx.tracker.request(MsgType::LIST_GROUPS, resp)) { std::cout << "error: could not reach any tracker\n"; return; }
    MsgReader r(resp.payload.data(), resp.payload.size());
    uint32_t count = r.getU32();
    std::cout << count << " group(s):\n";
    for (uint32_t i = 0; i < count && r.ok(); ++i) {
        std::string id = r.getStr();
        std::string owner = r.getStr();
        uint32_t members = r.getU32();
        std::cout << "  " << id << "  (owner=" << owner << ", members=" << members << ")\n";
    }
}

void cmdListRequests(ClientContext& ctx, const std::vector<std::string>& t) {
    if (!requireLogin(ctx)) return;
    if (t.size() != 2) { std::cout << "usage: list_requests <group_id>\n"; return; }
    MsgWriter w;
    w.putStr(t[1]);
    Message resp;
    if (!ctx.tracker.request(MsgType::LIST_REQUESTS, w, resp)) { std::cout << "error: could not reach any tracker\n"; return; }
    if (resp.type == MsgType::RESP_ERR) {
        MsgReader er(resp.payload.data(), resp.payload.size());
        std::cout << "ERROR: " << er.getStr() << "\n";
        return;
    }
    MsgReader r(resp.payload.data(), resp.payload.size());
    uint32_t count = r.getU32();
    std::cout << count << " pending request(s):\n";
    for (uint32_t i = 0; i < count && r.ok(); ++i) std::cout << "  " << r.getStr() << "\n";
}

void cmdListFiles(ClientContext& ctx, const std::vector<std::string>& t) {
    if (!requireLogin(ctx)) return;
    if (t.size() != 2) { std::cout << "usage: list_files <group_id>\n"; return; }
    MsgWriter w;
    w.putStr(t[1]);
    Message resp;
    if (!ctx.tracker.request(MsgType::LIST_FILES, w, resp)) { std::cout << "error: could not reach any tracker\n"; return; }
    if (resp.type == MsgType::RESP_ERR) {
        MsgReader er(resp.payload.data(), resp.payload.size());
        std::cout << "ERROR: " << er.getStr() << "\n";
        return;
    }
    MsgReader r(resp.payload.data(), resp.payload.size());
    uint32_t count = r.getU32();
    std::cout << count << " file(s):\n";
    for (uint32_t i = 0; i < count && r.ok(); ++i) {
        std::string name = r.getStr();
        uint64_t size = r.getU64();
        uint32_t pieces = r.getU32();
        std::string sha1 = r.getStr();
        uint32_t seeders = r.getU32();
        std::cout << "  " << name << "  size=" << size << "B  pieces=" << pieces
                  << "  seeders=" << seeders << "  sha1=" << sha1.substr(0, 10) << "...\n";
    }
}

void cmdShowDownloads(ClientContext& ctx) {
    auto list = ctx.downloads.listProgress();
    if (list.empty()) { std::cout << "no downloads started this session\n"; return; }
    for (const auto& dp : list) {
        if (dp.state == FileState::SEEDING) {
            std::cout << "[C] [" << dp.groupId << "] " << dp.fileName << "\n";
        } else if (dp.state == FileState::FAILED) {
            std::cout << "[FAILED] [" << dp.groupId << "] " << dp.fileName << " -- " << dp.failureReason << "\n";
        } else {
            int pct = dp.numPieces ? static_cast<int>(100ull * dp.piecesDone / dp.numPieces) : 0;
            std::cout << "[" << dp.piecesDone << "/" << dp.numPieces << "] [" << dp.groupId << "] "
                      << dp.fileName << " (" << pct << "%)\n";
        }
    }
}

} // namespace

void runCommandLoop(ClientContext& ctx) {
    std::cout << "p2p-client ready. type 'help' for commands.\n";
    std::string line;
    while (true) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) break;
        auto t = tokenize(line);
        if (t.empty()) continue;
        const std::string& cmd = t[0];

        if (cmd == "exit" || cmd == "quit") {
            break;
        } else if (cmd == "help") {
            std::cout <<
                "create_user <user_id> <password>\n"
                "login <user_id> <password>\n"
                "create_group <group_id>\n"
                "join_group <group_id>\n"
                "leave_group <group_id>\n"
                "list_groups\n"
                "list_requests <group_id>\n"
                "accept_request <group_id> <user_id>\n"
                "logout\n"
                "upload_file <group_id> <file_path>\n"
                "list_files <group_id>\n"
                "download_file <group_id> <file_name> <destination_path>\n"
                "show_downloads\n"
                "stop_share <group_id> <file_name>\n"
                "exit\n";
        } else if (cmd == "create_user") {
            cmdCreateUser(ctx, t);
        } else if (cmd == "login") {
            cmdLogin(ctx, t);
        } else if (cmd == "logout") {
            cmdLogout(ctx);
        } else if (cmd == "create_group") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 2) { std::cout << "usage: create_group <group_id>\n"; continue; }
            MsgWriter w; w.putStr(t[1]);
            doSimpleRequest(ctx, MsgType::CREATE_GROUP, w);
        } else if (cmd == "join_group") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 2) { std::cout << "usage: join_group <group_id>\n"; continue; }
            MsgWriter w; w.putStr(t[1]);
            doSimpleRequest(ctx, MsgType::JOIN_GROUP, w);
        } else if (cmd == "leave_group") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 2) { std::cout << "usage: leave_group <group_id>\n"; continue; }
            MsgWriter w; w.putStr(t[1]);
            doSimpleRequest(ctx, MsgType::LEAVE_GROUP, w);
        } else if (cmd == "list_groups") {
            cmdListGroups(ctx);
        } else if (cmd == "list_requests") {
            cmdListRequests(ctx, t);
        } else if (cmd == "accept_request") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 3) { std::cout << "usage: accept_request <group_id> <user_id>\n"; continue; }
            MsgWriter w; w.putStr(t[1]); w.putStr(t[2]);
            doSimpleRequest(ctx, MsgType::ACCEPT_REQUEST, w);
        } else if (cmd == "upload_file") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 3) { std::cout << "usage: upload_file <group_id> <file_path>\n"; continue; }
            std::string msg;
            bool ok = uploadFile(ctx.registry, ctx.tracker, ctx.myIp, ctx.myPort, t[1], t[2], msg);
            std::cout << (ok ? "OK: " : "ERROR: ") << msg << "\n";
        } else if (cmd == "list_files") {
            cmdListFiles(ctx, t);
        } else if (cmd == "download_file") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 4) { std::cout << "usage: download_file <group_id> <file_name> <destination_path>\n"; continue; }
            ctx.downloads.startDownload(t[1], t[2], t[3]);
            std::cout << "download started in background: [" << t[1] << "] " << t[2] << " -> " << t[3]
                      << " (see show_downloads)\n";
        } else if (cmd == "show_downloads") {
            cmdShowDownloads(ctx);
        } else if (cmd == "stop_share") {
            if (!requireLogin(ctx)) continue;
            if (t.size() != 3) { std::cout << "usage: stop_share <group_id> <file_name>\n"; continue; }
            MsgWriter w; w.putStr(t[1]); w.putStr(t[2]);
            doSimpleRequest(ctx, MsgType::STOP_SHARE, w);
        } else {
            std::cout << "unknown command '" << cmd << "'. type 'help' for the list.\n";
        }
    }
}

} // namespace p2p
