#include "backend/store/hash_tree.h"

#include <cstddef>
#include <fstream>
#include <span>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/common/page_id.h"
#include "common/byte_util.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::store {

void HashTree::RegisterPage(PageId id) {
  // Make sure the data structure is aware of the existence of this page.
  TrackNumPages(id);
}

void HashTree::UpdateHash(PageId id, std::span<const std::byte> page) {
  UpdateHash(id, carmen::GetHash(hasher_, page));
}

void HashTree::UpdateHash(PageId id, const Hash& hash) {
  TrackNumPages(id);
  GetMutableHash(0, id) = hash;
  dirty_pages_.erase(id);
  dirty_level_one_positions_.insert(id / branching_factor_);
}

void HashTree::MarkDirty(PageId page) {
  TrackNumPages(page);
  dirty_pages_.insert(page);
}

Hash HashTree::GetHash() {
  // If there are no pages, the full hash is zero by definition.
  if (num_pages_ == 0) {
    return Hash{};
  }

  // If nothing has changed in the meanwhile, return the last result.
  if (dirty_pages_.empty() && dirty_level_one_positions_.empty()) {
    return hashes_.back()[0];
  }

  // Update hashes of dirty pages.
  absl::flat_hash_set<int> dirty_parent;
  std::swap(dirty_level_one_positions_, dirty_parent);
  for (PageId i : dirty_pages_) {
    auto data = page_source_->GetPageData(i);
    GetMutableHash(0, i) = carmen::GetHash(hasher_, data);
    dirty_parent.insert(i / branching_factor_);
  }
  dirty_pages_.clear();

  // If there is only one page, the full hash is that page's hash.
  if (num_pages_ == 1) {
    return GetMutableHash(0, 0);
  }

  // Perform hash aggregation.
  for (std::size_t level = 1;; level++) {
    // Gets the parent before the children because the fetching of the parent
    // may resize the hash list while the fetch for the children will not.
    std::vector<Hash>& parent = GetHashes(level);
    const std::vector<Hash>& children = GetHashes(level - 1);

    absl::flat_hash_set<int> new_dirty;
    for (int parent_pos : dirty_parent) {
      auto data = std::as_bytes(std::span<const Hash>(children).subspan(
          parent_pos * branching_factor_, branching_factor_));
      GetMutableHash(level, parent_pos) = carmen::GetHash(hasher_, data);
      new_dirty.insert(parent_pos / branching_factor_);
    }

    if (children.size() <= branching_factor_) {
      return parent[0];
    }

    std::swap(dirty_parent, new_dirty);
  }
}

namespace {

std::size_t GetPaddedSize(std::size_t min_size, std::size_t block_size) {
  if (min_size % block_size == 0) {
    return min_size;
  }
  return ((min_size / block_size) + 1) * block_size;
}

}  // namespace

std::vector<Hash>& HashTree::GetHashes(std::size_t level) {
  if (level >= hashes_.size()) {
    hashes_.resize(level + 1);
  }
  return hashes_[level];
}

Hash& HashTree::GetMutableHash(std::size_t level, std::size_t pos) {
  auto& level_hashes = GetHashes(level);
  if (pos >= level_hashes.size()) {
    level_hashes.resize(GetPaddedSize(pos + 1, branching_factor_));
  }
  return level_hashes[pos];
}

void HashTree::TrackNumPages(PageId page) {
  if (page < num_pages_) {
    return;
  }

  // All new pages need to be considered dirty.
  for (auto cur = num_pages_; cur <= page; cur++) {
    dirty_pages_.insert(cur);
  }
  num_pages_ = page + 1;
}

absl::Status HashTree::SaveToFile(const std::filesystem::path& file) {
  // The following information is stored in the file:
  //  - the branching factor (4 byte, little endian)
  //  - the number of pages (4 byte, little endian)
  //  - the aggregated hash
  //  - the hash of each page
  static_assert(std::endian::native == std::endian::little,
                "Big endian architectures not yet supported.");

  std::uint32_t branching_factor = branching_factor_;
  std::uint32_t num_pages = num_pages_;
  auto hash = GetHash();

  std::fstream out(file, std::ios::binary | std::ios::out);
  if (!out) {
    return absl::InternalError(
        absl::StrFormat("Could not open file %s for writing.", file));
  }

  auto write_scalar = [&](auto& data) -> absl::Status {
    out.write(reinterpret_cast<const char*>(&data), sizeof(data));
    if (out.good()) return absl::OkStatus();
    return absl::InternalError(
        absl::StrFormat("Could not write to file %s.", file));
  };

  RETURN_IF_ERROR(write_scalar(branching_factor));
  RETURN_IF_ERROR(write_scalar(num_pages));
  RETURN_IF_ERROR(write_scalar(hash));
  for (std::size_t i = 0; i < num_pages_; i++) {
    const auto& hash = hashes_[0][i];
    RETURN_IF_ERROR(write_scalar(hash));
  }

  out.close();
  if (!out.good()) {
    return absl::InternalError(
        absl::StrFormat("Could not close file %s.", file));
  }

  return absl::OkStatus();
}

absl::Status HashTree::LoadFromFile(const std::filesystem::path& file) {
  std::fstream in(file, std::ios::binary | std::ios::in);

  // Fail if the file could not be opened.
  if (!in) {
    return absl::InternalError(
        absl::StrFormat("Could not open file %s for reading.", file));
  }

  // Check the minimum file length of 4 + 4 + 32 byte.
  in.seekg(0, std::ios::end);
  if (!in.good()) {
    return absl::InternalError(
        absl::StrFormat("Could not seek to end of file %s.", file));
  }

  auto size = in.tellg();
  if (size < 0) {
    return absl::InternalError(
        absl::StrFormat("Could not read position in file %s.", file));
  }

  if (size < 40) {
    return absl::InternalError(absl::StrFormat(
        "File %s is too short. Needed 40, got %d bytes.", file, size));
  }

  auto read_scalar = [&](auto& data) -> absl::Status {
    in.read(reinterpret_cast<char*>(&data), sizeof(data));
    if (in.good()) return absl::OkStatus();
    return absl::InternalError(
        absl::StrFormat("Could not read from file %s.", file));
  };

  in.seekg(0, std::ios::beg);
  if (!in.good()) {
    return absl::InternalError(
        absl::StrFormat("Could not seek to beginning of file %s.", file));
  }

  // Load the branching factor.
  std::uint32_t branching_factor;
  RETURN_IF_ERROR(read_scalar(branching_factor));
  if (branching_factor_ != branching_factor) {
    return absl::InternalError(
        absl::StrFormat("Branching factor mismatch. Expected %d, got %d.",
                        branching_factor_, branching_factor));
  }

  // Load the number of pages.
  std::uint32_t num_pages;
  RETURN_IF_ERROR(read_scalar(num_pages));
  if (size != 40 + num_pages * 32) {
    return absl::InternalError(
        absl::StrFormat("File %s has wrong size. Expected %d, got %d bytes.",
                        file, 40 + num_pages * 32, size));
  }
  num_pages_ = num_pages;

  // Load the global hash.
  Hash file_hash;
  RETURN_IF_ERROR(read_scalar(file_hash));

  // Read the page hashes.
  hashes_.clear();
  if (num_pages > 0) {
    std::vector<Hash> page_hashes;
    page_hashes.resize(GetPaddedSize(num_pages, branching_factor));
    in.read(reinterpret_cast<char*>(page_hashes.data()),
            sizeof(Hash) * num_pages);
    if (!in.good()) {
      return absl::InternalError(
          absl::StrFormat("Could not read hashes from file %s.", file));
    }
    hashes_.push_back(std::move(page_hashes));
  }

  in.close();
  if (!in.good()) {
    return absl::InternalError(
        absl::StrFormat("Could not close file %s.", file));
  }

  // Update hash information.
  dirty_pages_.clear();
  dirty_level_one_positions_.clear();
  for (std::size_t i = 0; i < num_pages; i += branching_factor_) {
    dirty_level_one_positions_.insert(i / branching_factor);
  }

  // Recompute hashes.
  auto hash = GetHash();

  if (hash != file_hash) {
    std::stringstream ss;
    ss << "Unable to verify hash:\n - in file:  " << file_hash
       << "\n - restored: " << hash << "\n";
    return absl::InternalError(ss.str());
  }

  return absl::OkStatus();
}

absl::Status HashTree::SaveToLevelDb(LevelDb& leveldb) {
  RETURN_IF_ERROR(
      leveldb.Add({"ht_branching_factor", AsChars(branching_factor_)}));
  RETURN_IF_ERROR(leveldb.Add({"ht_num_pages", AsChars(num_pages_)}));
  RETURN_IF_ERROR(leveldb.Add({"ht_hash", AsChars(GetHash())}));

  for (std::size_t i = 0; i < num_pages_; i++) {
    RETURN_IF_ERROR(
        leveldb.Add({"ht_page_" + std::to_string(i), AsChars(hashes_[0][i])}));
  }

  return absl::OkStatus();
}

absl::Status HashTree::SaveToLevelDb(const std::filesystem::path& file) {
  // The following information is stored in the leveldb:
  //  - the branching factor (4 byte, little endian)
  //  - the number of pages (4 byte, little endian)
  //  - the aggregated hash
  //  - the hash of each page
  static_assert(std::endian::native == std::endian::little,
                "Big endian architectures not yet supported.");

  ASSIGN_OR_RETURN(auto db, LevelDb::Open(file, /*create_if_missing=*/true));
  return SaveToLevelDb(db);
}

absl::Status HashTree::LoadFromLevelDb(const LevelDb& leveldb) {
  // Load the branching factor.
  ASSIGN_OR_RETURN(auto result, leveldb.Get("ht_branching_factor"));
  ASSIGN_OR_RETURN(auto branching_factor,
                   FromChars<decltype(branching_factor_)>(result));
  if (branching_factor != branching_factor_)
    return absl::InternalError("Invalid branching factor in leveldb file.");

  // Load the number of pages.
  ASSIGN_OR_RETURN(result, leveldb.Get("ht_num_pages"));
  ASSIGN_OR_RETURN(num_pages_, FromChars<decltype(num_pages_)>(result));

  // Load the global hash.
  ASSIGN_OR_RETURN(result, leveldb.Get("ht_hash"));
  ASSIGN_OR_RETURN(auto file_hash, FromChars<Hash>(result));

  // Read the page hashes.
  hashes_.clear();
  if (num_pages_ > 0) {
    std::vector<Hash> page_hashes;
    page_hashes.resize(GetPaddedSize(num_pages_, branching_factor_));
    for (std::size_t i = 0; i < num_pages_; i++) {
      ASSIGN_OR_RETURN(result, leveldb.Get("ht_page_" + std::to_string(i)));
      ASSIGN_OR_RETURN(page_hashes[i], FromChars<Hash>(result));
    }
    hashes_.push_back(std::move(page_hashes));
  }

  // Update hash information.
  dirty_pages_.clear();
  dirty_level_one_positions_.clear();
  for (std::size_t i = 0; i < num_pages_; i += branching_factor_) {
    dirty_level_one_positions_.insert(i / branching_factor);
  }

  // Recompute hashes.
  auto hash = GetHash();

  if (hash != file_hash) {
    std::stringstream ss;
    ss << "Unable to verify hash:\n - in leveldb:  " << file_hash
       << "\n - restored: " << hash;
    return absl::InternalError(ss.str());
  }

  return absl::OkStatus();
}

absl::Status HashTree::LoadFromLevelDb(const std::filesystem::path& file) {
  ASSIGN_OR_RETURN(auto db, LevelDb::Open(file, /*create_if_missing=*/false));
  return LoadFromLevelDb(db);
}

MemoryFootprint HashTree::GetMemoryFootprint() const {
  int i = 0;
  MemoryFootprint hashsize;
  for (const auto& hashes : hashes_) {
    hashsize.Add(absl::StrFormat("level-%d", i++), SizeOf(hashes));
  }

  MemoryFootprint res(*this);
  res.Add("hashes", std::move(hashsize));
  res.Add("dirty_pages", SizeOf(dirty_pages_));
  res.Add("dirty_level_one_positions", SizeOf(dirty_level_one_positions_));
  return res;
}

}  // namespace carmen::backend::store
