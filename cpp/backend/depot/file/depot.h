#pragma once

#include <concepts>
#include <fstream>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/file.h"
#include "backend/store/hash_tree.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::depot {

// File Depot implementation. The depot consists of 3 files:
// - data.dat: contains the actual data - append only
// - offset.dat: contains the offset and size of each key in data.dat
// - hashes.dat: contains the hash tree of the depot
template <std::integral K>
class FileDepot {
 public:
  // The type of the depot key.
  using key_type = K;

  // Creates a new FileDepot using the provided context and directory path.
  static absl::StatusOr<FileDepot> Open(Context&,
                                        const std::filesystem::path& path) {
    return Open(path);
  }

  // Creates a new FileDepot using the provided directory path, branching factor
  // and number of items per group for hash computation.
  static absl::StatusOr<FileDepot> Open(const std::filesystem::path& path,
                                        std::size_t hash_branching_factor = 32,
                                        std::size_t hash_box_size = 4) {
    // Make sure the parent directory exists.
    if (!CreateDirectory(path)) {
      return absl::InternalError(
          absl::StrFormat("Unable to create parent directory %s", path));
    }

    auto offset_file = path / "offset.dat";
    auto data_file = path / "data.dat";

    // Opening the file write-only first creates the file in case it does not
    // exist.
    if (!std::filesystem::exists(offset_file)) {
      std::fstream fs(offset_file, std::ios::binary | std::ios::out);
      fs.close();
    }
    if (!std::filesystem::exists(data_file)) {
      std::fstream fs(data_file, std::ios::binary | std::ios::out);
      fs.close();
    }

    // Open the files for reading and writing.
    std::fstream offset_fs(offset_file,
                           std::ios::binary | std::ios::in | std::ios::out);
    if (!offset_fs.is_open())
      return absl::InternalError("Failed to open offset file.");
    std::fstream data_fs(data_file,
                         std::ios::binary | std::ios::in | std::ios::out);
    if (!data_fs.is_open()) {
      offset_fs.close();
      return absl::InternalError("Failed to open data file.");
    }

    auto depot =
        FileDepot(path / "hash.dat", std::move(offset_fs), std::move(data_fs),
                  hash_branching_factor, hash_box_size);

    // Load the hash tree from the file.
    if (std::filesystem::exists(depot.hash_file_)) {
      RETURN_IF_ERROR(depot.hashes_.LoadFromFile(depot.hash_file_));
    }

    return depot;
  }

  // Supports instances to be moved.
  FileDepot(FileDepot&&) noexcept = default;

  // Depot is closed when the instance is destroyed.
  ~FileDepot() { Close().IgnoreError(); }

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    // clear the error state
    data_fs_->clear();
    offset_fs_->clear();

    // Move to the end of the file and get the position.
    data_fs_->seekp(0, std::ios::end);
    auto eof_pos = data_fs_->tellp();
    if (eof_pos == -1)
      return absl::InternalError("Failed to get offset in data file");

    // Write data to the end of the file.
    data_fs_->write(reinterpret_cast<const char*>(data.data()), data.size());
    if (!data_fs_->good())
      return absl::InternalError("Failed to write data to data file");

    // Move to the position of the key in the offset file.
    offset_fs_->seekp(GetOffsetPosition(key), std::ios::beg);

    // Prepare data to write to the offset file.
    OffsetAndSize write_data{static_cast<Offset>(eof_pos),
                             static_cast<Size>(data.size())};

    // Write data to the offset file.
    offset_fs_->write(reinterpret_cast<const char*>(&write_data),
                      sizeof(write_data));
    if (!offset_fs_->good())
      return absl::InternalError("Failed to write size to offset file");

    hashes_.MarkDirty(GetBoxHashGroup(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. The data is valid
  // until the next call to this function. If no values has been previously
  // set using the Set(..) function above, not found status is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    ASSIGN_OR_RETURN(auto metadata, GetOffsetAndSize(key, *offset_fs_));
    if (metadata.size == 0) return std::span<const std::byte>();

    // clear the error state
    data_fs_->clear();

    // prepare the buffer
    get_data_.resize(metadata.size);

    // seek to position in data file
    data_fs_->seekg(metadata.offset, std::ios::beg);

    // read actual data
    data_fs_->read(get_data_.data(), metadata.size);
    if (!data_fs_->good()) return absl::InternalError("Failed to read data");

    return std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(get_data_.data()), metadata.size);
  }

  // Retrieves the size of data associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::uint32_t> GetSize(const K& key) const {
    ASSIGN_OR_RETURN(auto metadata, GetOffsetAndSize(key, *offset_fs_));
    if (metadata.size == 0) return absl::NotFoundError("Key not found");
    return metadata.size;
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to disk.
  absl::Status Flush() {
    if ((data_fs_ && data_fs_->is_open()) &&
        (offset_fs_ && offset_fs_->is_open())) {
      RETURN_IF_ERROR(hashes_.SaveToFile(hash_file_));
      data_fs_->flush();
      offset_fs_->flush();
    }
    return absl::OkStatus();
  }

  // Close the depot.
  absl::Status Close() {
    if ((data_fs_ && data_fs_->is_open()) &&
        (offset_fs_ && offset_fs_->is_open())) {
      RETURN_IF_ERROR(Flush());
      data_fs_->close();
      offset_fs_->close();
    }
    return absl::OkStatus();
  }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("hashes", hashes_);
    res.Add("buffer", SizeOf(get_data_));
    return res;
  }

 private:
  using Offset = std::uint64_t;
  using Size = std::uint32_t;

  // The offset and size of each entry in the data file stored in offset file.
  struct OffsetAndSize {
    Offset offset = 0;
    Size size = 0;
  } ABSL_ATTRIBUTE_PACKED;

  FileDepot(std::filesystem::path hash_file, std::fstream offset_fs,
            std::fstream data_fs, std::size_t hash_branching_factor,
            std::size_t hash_box_size)
      : hash_box_size_(hash_box_size),
        hash_file_(std::move(hash_file)),
        offset_fs_(std::make_unique<std::fstream>(std::move(offset_fs))),
        data_fs_(std::make_unique<std::fstream>(std::move(data_fs))),
        hashes_(std::make_unique<PageProvider>(*data_fs_, *offset_fs_,
                                               hash_box_size_),
                hash_branching_factor) {
    assert(hash_box_size_ > 0 && "hash_box_size must be > 0");
  }

  // Get position of the given key in the offset file.
  static std::size_t GetOffsetPosition(const K& key) {
    return key * sizeof(OffsetAndSize);
  }

  // Get offset and size for given key from the offset file into the data file.
  static absl::StatusOr<OffsetAndSize> GetOffsetAndSize(
      const K& key, std::fstream& offset_fs) {
    // clear the error state
    offset_fs.clear();

    // Seek to the position of the key.
    offset_fs.seekg(GetOffsetPosition(key), std::ios::beg);

    // Read offset and size.
    OffsetAndSize metadata;
    offset_fs.read(reinterpret_cast<char*>(&metadata), sizeof(metadata));

    if (offset_fs.eof()) return absl::NotFoundError("Key not found");
    if (offset_fs.fail())
      return absl::InternalError("Failed to read offset and size");

    return metadata;
  }

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / hash_box_size_;
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    PageProvider(std::fstream& data_fs, std::fstream& offset_fs,
                 std::size_t hash_box_size)
        : data_fs_(data_fs),
          offset_fs_(offset_fs),
          hash_box_size_(hash_box_size) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      const auto empty = std::span<const std::byte>();
      std::vector<OffsetAndSize> offset_buffer(hash_box_size_);
      const std::size_t lengths_size = hash_box_size_ * sizeof(Size);

      // read all offsets and sizes for the hash group
      offset_fs_.clear();
      offset_fs_.seekg(GetOffsetPosition(id * hash_box_size_), std::ios::beg);
      offset_fs_.read(reinterpret_cast<char*>(offset_buffer.data()),
                      hash_box_size_ * sizeof(OffsetAndSize));
      // TODO: Do proper error handling
      if (!offset_fs_.eof() && offset_fs_.fail()) {
        return empty;
      }

      // set lengths to zero default value
      if (page_data_.size() < lengths_size) {
        page_data_.resize(lengths_size);
      }
      std::fill_n(page_data_.begin(), lengths_size, 0);

      // parse offsets and sizes
      std::size_t total_length = 0;
      std::size_t start = 0;
      auto is_fragmented = false;
      for (std::size_t i = 0; i < hash_box_size_; ++i) {
        if (offset_buffer[i].size == 0) continue;
        if (total_length == 0) {
          start = offset_buffer[i].offset;
        } else if (start + total_length != offset_buffer[i].offset) {
          is_fragmented = true;
        }
        total_length += offset_buffer[i].size;
        // set length for this key
        reinterpret_cast<Size*>(page_data_.data())[i] = offset_buffer[i].size;
      }

      if (total_length == 0) {
        return {reinterpret_cast<const std::byte*>(page_data_.data()),
                lengths_size};
      }

      // add lengths size to total length and prepare buffer
      total_length += lengths_size;
      if (page_data_.size() < total_length) {
        page_data_.resize(total_length);
      }

      data_fs_.clear();

      // fast path for non-fragmented data
      if (!is_fragmented) {
        data_fs_.seekg(start, std::ios::beg);
        data_fs_.read(page_data_.data() + lengths_size,
                      total_length - lengths_size);
        if (!data_fs_.good()) {
          // TODO: Add error handling
          return empty;
        }
        return {reinterpret_cast<const std::byte*>(page_data_.data()),
                total_length};
      }

      // slow path for fragmented data
      std::size_t position = 0;
      for (std::size_t i = 0; i < hash_box_size_; ++i) {
        if (offset_buffer[i].size == 0) continue;
        data_fs_.seekg(offset_buffer[i].offset, std::ios::beg);
        data_fs_.read(page_data_.data() + lengths_size + position,
                      offset_buffer[i].size);
        if (!data_fs_.good()) {
          // TODO: Add error handling
          return empty;
        }
        position += offset_buffer[i].size;
      }

      return {reinterpret_cast<const std::byte*>(page_data_.data()),
              total_length};
    }

   private:
    std::fstream& data_fs_;
    std::fstream& offset_fs_;
    const std::size_t hash_box_size_;
    std::vector<char> page_data_;
  };

  // The amount of items that will be grouped into a single hashing group.
  const std::size_t hash_box_size_;

  // The name of the file to save hashes to.
  std::filesystem::path hash_file_;

  // It is used to get positions of the data in the data file.
  std::unique_ptr<std::fstream> offset_fs_;

  // It is used to get the actual data.
  std::unique_ptr<std::fstream> data_fs_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;

  // Temporary storage for the result of Get().
  mutable std::vector<char> get_data_;
};

}  // namespace carmen::backend::depot
