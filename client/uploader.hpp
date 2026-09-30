// client/uploader.hpp
//
// Implements `upload_file`: chunks a file into kPieceSize pieces in a
// single streaming pass (never holding more than one piece in memory, so
// a 1GB file costs ~512KB of RAM to hash), computes each piece's SHA1 plus
// a whole-file SHA1, registers the metadata with the tracker, and marks
// the file fully present in FileRegistry so PeerServer can start serving
// it to other members immediately.

#pragma once

#include "file_registry.hpp"
#include "tracker_conn.hpp"

#include <cstdint>
#include <string>

namespace p2p {

bool uploadFile(FileRegistry& registry, TrackerConnection& tracker, const std::string& myIp, uint16_t myPort,
                 const std::string& groupId, const std::string& filePath, std::string& outMessage);

} // namespace p2p
