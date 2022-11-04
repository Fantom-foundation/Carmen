#include "backend/store/hash_tree.h"

#include <cstddef>
#include <fstream>
#include <span>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/level_db.h"
#include "backend/common/page_id.h"
#include "common/hash.h"
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

void HashTree::SaveToFile(std::filesystem::path file) {
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
  out.write(reinterpret_cast<const char*>(&branching_factor),
            sizeof(branching_factor));
  out.write(reinterpret_cast<const char*>(&num_pages), sizeof(num_pages));
  out.write(reinterpret_cast<const char*>(&hash), sizeof(hash));
  for (std::size_t i = 0; i < num_pages_; i++) {
    const auto& hash = hashes_[0][i];
    out.write(reinterpret_cast<const char*>(&hash), sizeof(hash));
  }
}

bool HashTree::LoadFromFile(std::filesystem::path file) {
  // TODO: introduce absl::Status to report errors
  std::fstream in(file, std::ios::binary | std::ios::in);

  // Fail if the file could not be opened.
  if (!in) return false;

  // Check the minimum file length of 4 + 4 + 32 byte.
  in.seekg(0, std::ios::end);
  auto size = in.tellg();
  if (size < 40) {
    std::cout << "File to short - needed 40, got " << size << " byte\n";
    return false;
  }

  // Load the branching factor.
  in.seekg(0, std::ios::beg);
  std::uint32_t branching_factor;
  in.read(reinterpret_cast<char*>(&branching_factor), sizeof(branching_factor));
  if (branching_factor_ != branching_factor) {
    std::cout << "File has wrong branching factor - expected "
              << branching_factor_ << ", got " << branching_factor << "\n";
    return false;
  }

  // Load the number of pages.
  std::uint32_t num_pages;
  in.read(reinterpret_cast<char*>(&num_pages), sizeof(num_pages));
  if (size != 40 + num_pages * 32) {
    std::cout << "File has wrong size - for " << num_pages << " a size of "
              << (40 + num_pages * 32) << " would be needed, but it has "
              << size << " byte\n";
    return false;
  }
  num_pages_ = num_pages;

  // Load the global hash.
  Hash file_hash;
  in.read(reinterpret_cast<char*>(&file_hash), sizeof(file_hash));

  // Read the page hashes.
  hashes_.clear();
  if (num_pages > 0) {
    std::vector<Hash> page_hashes;
    page_hashes.resize(GetPaddedSize(num_pages, branching_factor));
    in.read(reinterpret_cast<char*>(page_hashes.data()),
            sizeof(Hash) * num_pages);
    hashes_.push_back(std::move(page_hashes));
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
    std::cout << "Unable to verify hash:\n - in file:  " << file_hash
              << "\n - restored: " << hash << "\n";
    return false;
  }

  return true;
}

template <typename T>
std::span<const char> AsRawData(const T& value) {
  auto bytes = std::as_bytes(std::span<const T>(&value, 1));
  return {reinterpret_cast<const char*>(bytes.data()), sizeof(T)};
}

absl::Status HashTree::SaveToLevelDB(const std::filesystem::path& file) {
  // The following information is stored in the leveldb:
  //  - the branching factor (4 byte, little endian)
  //  - the number of pages (4 byte, little endian)
  //  - the aggregated hash
  //  - the hash of each page
  static_assert(std::endian::native == std::endian::little,
                "Big endian architectures not yet supported.");

  auto db = LevelDB::Open(file, /*create_if_missing=*/true);
  if (!db.ok()) return db.status();

  RETURN_IF_ERROR(
      (*db).Add({"ht_branching_factor", AsRawData(branching_factor_)}));
  RETURN_IF_ERROR((*db).Add({"ht_num_pages", AsRawData(num_pages_)}));
  RETURN_IF_ERROR((*db).Add({"ht_hash", AsRawData(GetHash())}));

  for (std::size_t i = 0; i < num_pages_; i++) {
    RETURN_IF_ERROR(
        (*db).Add({"ht_page_" + std::to_string(i), AsRawData(hashes_[0][i])}));
  }

  return absl::OkStatus();
}

template <typename T>
absl::StatusOr<T> ParseRawData(std::span<const char> data) {
  if (data.size() != sizeof(T)) return absl::InternalError("Invalid data size");
  return *reinterpret_cast<const T*>(data.data());
}

absl::Status HashTree::LoadFromLevelDB(const std::filesystem::path& file) {
  auto db = LevelDB::Open(file, /*create_if_missing=*/false);
  if (!db.ok()) return db.status();

  // Load the branching factor.
  ASSIGN_OR_RETURN(auto result, (*db).Get("ht_branching_factor"));
  ASSIGN_OR_RETURN(auto branching_factor,
                   ParseRawData<decltype(branching_factor_)>(result));
  if (branching_factor != branching_factor_)
    return absl::InternalError("Invalid branching factor in leveldb file.");

  // Load the number of pages.
  ASSIGN_OR_RETURN(result, (*db).Get("ht_num_pages"));
  ASSIGN_OR_RETURN(num_pages_, ParseRawData<decltype(num_pages_)>(result));

  // Load the global hash.
  ASSIGN_OR_RETURN(result, (*db).Get("ht_hash"));
  ASSIGN_OR_RETURN(auto file_hash, ParseRawData<Hash>(result));

  // Read the page hashes.
  hashes_.clear();
  if (num_pages_ > 0) {
    std::vector<Hash> page_hashes;
    page_hashes.resize(GetPaddedSize(num_pages_, branching_factor_));
    for (std::size_t i = 0; i < num_pages_; i++) {
      ASSIGN_OR_RETURN(result, (*db).Get("ht_page_" + std::to_string(i)));
      ASSIGN_OR_RETURN(page_hashes[i], ParseRawData<Hash>(result));
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
       << "\n - restored: " << hash << "\n";
    return absl::InternalError(ss.str());
  }

  return absl::OkStatus();
}

}  // namespace carmen::backend::store
