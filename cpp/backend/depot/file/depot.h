#pragma once

#include <concepts>
#include <fstream>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/store/hash_tree.h"
#include "common/hash.h"
#include "common/type.h"
#include "common/status_util.h"

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
        hashes_(std::make_unique<PageProvider>(data_file_, num_hash_boxes_, std::bind(&FileDepot::GetBoxOffsetAndSize, this, std::placeholders::_1)),
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
    ASSIGN_OR_RETURN(auto metadata, GetBoxOffsetAndSize(key));
    if (metadata.second == 0) return absl::NotFoundError("Key not found");

    std::ifstream fs(data_file_, std::ios::in | std::ios::binary);
    if (!fs.is_open()) return absl::InternalError("Failed to open data file");

    get_data_.resize(metadata.second);

    // seek to position in data file
    if (!fs.seekg(metadata.first, std::ios::beg)) {
      fs.close();
      return absl::InternalError("Failed to seek to offset");
    }

    // read actual data
    fs.read(get_data_.data(), metadata.second);
    fs.close();
    if (!fs.good()) return absl::InternalError("Failed to read data");

    return std::span<const std::byte>(reinterpret_cast<const std::byte*>(get_data_.data()), metadata.second);
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to disk.
  absl::Status Flush() {
    hashes_.SaveToFile(hash_file_);
    return absl::OkStatus();
  }

  // Close the depot.
  absl::Status Close() {
    Flush();
    return absl::OkStatus();
  }

 private:
  using Offset = std::uint64_t;
  using Size = std::uint32_t;

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / num_hash_boxes_;
  }

  // Get position of the given key in the offset file.
  std::size_t GetBoxPosition(const K& key) const {
    return key * (sizeof(Offset) + sizeof(Size));
  }

  // Get offset and size for given key from the offset file into the data file.
  absl::StatusOr<std::pair<Offset, Size>> GetBoxOffsetAndSize(const K& key) const {
    std::fstream ofs(offset_file_, std::ios::in | std::ios::binary);
    if (!ofs.is_open()) return absl::InternalError("Failed to open offset file");

    // Seek to the position of the key.
    if (!ofs.seekg(GetBoxPosition(key), std::ios::beg)) {
      ofs.close();
      return absl::InternalError("Failed to seek to position in offset file");
    }

    // Read offset and size.
    std::array<char, sizeof(Offset) + sizeof(Size)> data{};

    ofs.read(data.data(), data.max_size());
    ofs.close();

    if (ofs.eof()) {
      return absl::NotFoundError("Key not found");
    }

    if (ofs.fail()) {
      return absl::InternalError("Failed to read offset and size");
    }

    return std::pair<Offset, Size> {*reinterpret_cast<const Offset*>(&data),
                                    *reinterpret_cast<const Size*>(&data + sizeof(Offset))};
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      auto static empty = std::array<std::byte, 0>{};
      // calculate start and end of the hash group
      auto start =  id * num_hash_boxes_;
      auto end = start + num_hash_boxes_ - 1;

      if (start > end) return empty;

      std::vector<std::pair<Offset, Size>> metadata(num_hash_boxes_);
      for (K i = 0; start + i <= end; ++i) {
        auto meta = metadata_extractor_(i);
        if (!meta.ok()) return empty;
        metadata[i] = *meta;
      }

      std::ifstream fs(data_file_, std::ios::in | std::ios::binary);
      if (!fs.is_open()) return empty;

      std::size_t len = 0;
      for (std::size_t i = 0; i < num_hash_boxes_; ++i) {
        if (!fs.seekg(metadata[i].first, std::ios::beg)) {
          fs.close();
          return empty;
        }

        page_data_.resize(len + metadata[i].second);

        fs.read(page_data_.data() + len, metadata[i].second);
        if (fs.fail()) {
          fs.close();
          return empty;
        }

        len += metadata[i].second;
      }

      return {reinterpret_cast<const std::byte*>(page_data_.data()), len};
    }

    PageProvider(const std::filesystem::path& data_file, std::size_t num_hash_boxes, std::function<absl::StatusOr<std::pair<Offset, Size>>(const K&)> metadata_extractor)
        : data_file_(data_file),
          num_hash_boxes_(num_hash_boxes),
          metadata_extractor_(metadata_extractor) {}

   private:
    const std::filesystem::path& data_file_;
    const std::size_t num_hash_boxes_;
    std::vector<char> page_data_;
    std::function<absl::StatusOr<std::pair<Offset, Size>>(const K&)> metadata_extractor_;
  };

  // The amount of boxes that will be grouped into a single hashing group.
  const std::size_t num_hash_boxes_;

  // The name of the file to save hashes to.
  std::filesystem::path hash_file_;

  // The name of the file to save offsets to. It is used to get positions
  // of the data in the data file.
  std::filesystem::path offset_file_;

  // The name of the file to save data to. It is used to get the actual data.
  std::filesystem::path data_file_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;

  // Temporary storage for the result of Get().
  mutable std::vector<char> get_data_;
};

}  // namespace carmen::backend::depot
