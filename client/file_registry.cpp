// client/file_registry.cpp

#include "file_registry.hpp"

namespace p2p {

std::shared_ptr<LocalFile> FileRegistry::create(const std::string& fileSha1, const std::string& fileName,
                                                  const std::string& groupId, uint64_t fileSize,
                                                  const std::vector<std::string>& pieceSha1,
                                                  const std::string& diskPath, FileState initialState) {
    auto lf = std::make_shared<LocalFile>();
    lf->fileSha1 = fileSha1;
    lf->fileName = fileName;
    lf->groupId = groupId;
    lf->fileSize = fileSize;
    lf->numPieces = static_cast<uint32_t>(pieceSha1.size());
    lf->pieceSha1 = pieceSha1;
    lf->diskPath = diskPath;
    lf->bitfield.assign(bitfieldBytes(lf->numPieces), 0);
    lf->state = initialState;

    std::lock_guard<std::mutex> lock(mu_);
    byHash_[fileSha1] = lf;
    return lf;
}

std::shared_ptr<LocalFile> FileRegistry::find(const std::string& fileSha1) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = byHash_.find(fileSha1);
    return it == byHash_.end() ? nullptr : it->second;
}

std::shared_ptr<LocalFile> FileRegistry::findByGroupAndName(const std::string& groupId, const std::string& fileName) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [hash, lf] : byHash_) {
        if (lf->groupId == groupId && lf->fileName == fileName) return lf;
    }
    return nullptr;
}

std::vector<std::shared_ptr<LocalFile>> FileRegistry::all() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::shared_ptr<LocalFile>> out;
    out.reserve(byHash_.size());
    for (auto& [hash, lf] : byHash_) out.push_back(lf);
    return out;
}

} // namespace p2p
