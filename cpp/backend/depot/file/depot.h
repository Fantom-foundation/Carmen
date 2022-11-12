#pragma once

#include <concepts>
#include <fstream>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/store/hash_tree.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::depot {

// In memory implementation of a Depot.
template <std::integral K>
class FileDepot {
 public:
  // The type of the depot key.
  using key_type = K;

  // Creates a new FileDepot using the provided directory path, branching factor
  // and number of boxes per group for hash computation.
  FileDepot(const std::filesystem::path& directory, std::size_t hash_branching_factor = 32,
                std::size_t num_hash_boxes = 4)
      : num_hash_boxes_(num_hash_boxes),
        hash_file_(directory / "hash.dat"),
        offset_file_(directory / "offset.dat"),
        data_file_(directory / "data.dat"),
        hashes_(std::make_unique<PageProvider>(offset_file_, data_file_, num_hash_boxes_),
                hash_branching_factor) {
        if (std::filesystem::exists(hash_file_)) {
          hashes_.LoadFromFile(hash_file_);
        }
  }

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    std::fstream dfs(data_file_, std::ios::out | std::ios::binary);
    if (!dfs.is_open()) return absl::InternalError("Failed to open data file");

    // Move to the end of the file and get the position.
    dfs.seekp(0, std::ios::end);
    auto offset = dfs.tellp();
    if (offset == -1) {
      dfs.close();
      return absl::InternalError("Failed to seek to end of data file");
    }

    // Write data to the end of the file.
    dfs.write(reinterpret_cast<const char*>(data.data()), data.size());
    dfs.close();

    std::fstream ofs(offset_file_, std::ios::out | std::ios::binary);
    if (!ofs.is_open()) return absl::InternalError("Failed to open offset file");
    auto position = GetBoxPosition(key);
    ofs.seekp(position, std::ios::beg);
    Offset offset_value = offset;
    ofs.write(reinterpret_cast<const char*>(&offset_value), sizeof(offset_value));
    ofs.seekp(position + sizeof(Offset), std::ios::beg);
    Size size = data.size();
    ofs.write(reinterpret_cast<const char*>(&size), sizeof(size));
    ofs.close();

    hashes_.MarkDirty(GetBoxHashGroup(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    if (key >= boxes_->size()) {
      return absl::NotFoundError("Key not found");
    }
    return (*boxes_)[key];
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Ignored, since depot is not backed by disk storage.
  absl::Status Flush() { return absl::OkStatus(); }

  // Ignored, since depot does not maintain any resources.
  absl::Status Close() { return absl::OkStatus(); }

 private:
  using Offset = std::uint64_t;
  using Size = std::uint32_t;

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / num_hash_boxes_;
  }

  // Get position of the given key in the data file.
  std::size_t GetBoxPosition(const K& key) const {
    return key * (sizeof(Offset) + sizeof(Size));
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      // calculate start and end of the hash group
      auto start = boxes_.begin() + id * num_hash_boxes_;
      auto end =
          boxes_.begin() +
          std::min(id * num_hash_boxes_ + num_hash_boxes_, boxes_.size());

      if (start >= end) return empty;

      // calculate the size of the hash group
      std::size_t len = 0;
      for (auto it = start; it != end; ++it) {
        len += it->size();
      }

      page_data_.resize(len);
      std::size_t pos = 0;
      for (auto it = start; it != end; ++it) {
        if (it->empty()) continue;
        std::memcpy(page_data_.data() + pos, it->data(), it->size());
        pos += it->size();
      }

      return {page_data_.data(), len};
    }

    explicit PageProvider(const std::filesystem::path& offset_file, const std::filesystem::path& data_file, std::size_t num_hash_boxes)
        : offset_file_(offset_file),
          data_file_(data_file),
          num_hash_boxes_(num_hash_boxes) {}

   private:
    const std::filesystem::path& offset_file_;
    const std::filesystem::path& data_file_;
    std::size_t num_hash_boxes_;
    std::vector<std::byte> page_data_;
  };

  // The name of the file to save hashes to.
  std::filesystem::path hash_file_;

  // The name of the file to save offsets to.
  std::filesystem::path offset_file_;

  // The name of the file to save data to.
  std::filesystem::path data_file_;

  // The amount of boxes that will be grouped into a single hashing group.
  const std::size_t num_hash_boxes_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;

  // Temporary storage for the result of Get().
  mutable std::vector<std::byte> get_data_;
};

}  // namespace carmen::backend::depot
