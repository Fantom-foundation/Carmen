/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#pragma once

#include <concepts>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/file.h"
#include "backend/store/hash_tree.h"
#include "backend/structure.h"
#include "common/fstream.h"
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
    RETURN_IF_ERROR(CreateDirectory(path));

    auto offset_file = path / "offset.dat";
    auto data_file = path / "data.dat";

    // Create files for data and offsets.
    RETURN_IF_ERROR(CreateFile(offset_file));
    RETURN_IF_ERROR(CreateFile(data_file));

    ASSIGN_OR_RETURN(
        auto offset_fs,
        FStream::Open(offset_file,
                      std::ios::binary | std::ios::in | std::ios::out));

    ASSIGN_OR_RETURN(auto data_fs,
                     FStream::Open(data_file, std::ios::binary | std::ios::in |
                                                  std::ios::out));

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
  ~FileDepot() {
    if (auto status = Close(); !status.ok()) {
      std::cout << "WARNING: Failed to close Depot, " << status << std::endl;
    }
  }

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    // Move to the end of the file and get the position.
    RETURN_IF_ERROR(data_fs_->Seekp(0, std::ios::end));
    ASSIGN_OR_RETURN(auto eof_pos, data_fs_->Tellp());

    // Write data to the end of the file.
    RETURN_IF_ERROR(data_fs_->Write(data));

    // Move to the position of the key in the offset file.
    RETURN_IF_ERROR(offset_fs_->Seekp(GetOffsetPosition(key), std::ios::beg));

    // Prepare data to write to the offset file.
    OffsetAndSize write_data{static_cast<Offset>(eof_pos),
                             static_cast<Size>(data.size())};

    // Write data to the offset file.
    RETURN_IF_ERROR(offset_fs_->Write(write_data));

    hashes_.MarkDirty(GetBoxHashGroup(key));

    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. The data is valid
  // until the next call to this function. If no values has been previously
  // set using the Set(..) function above, not found status is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    ASSIGN_OR_RETURN(auto metadata, GetOffsetAndSize(key));
    if (metadata.size == 0) return std::span<const std::byte>();

    // prepare the buffer
    get_data_.resize(metadata.size);

    // seek to position in data file
    RETURN_IF_ERROR(data_fs_->Seekg(metadata.offset, std::ios::beg));

    // read actual data
    RETURN_IF_ERROR(data_fs_->Read(std::span(get_data_.data(), metadata.size)));

    return get_data_;
  }

  // Retrieves the size of data associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::uint32_t> GetSize(const K& key) const {
    ASSIGN_OR_RETURN(auto metadata, GetOffsetAndSize(key));
    if (metadata.size == 0) return absl::NotFoundError("Key not found");
    auto res = metadata.size;
    return res;
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to disk.
  absl::Status Flush() {
    if (data_fs_ && data_fs_->IsOpen() && offset_fs_ && offset_fs_->IsOpen()) {
      RETURN_IF_ERROR(hashes_.SaveToFile(hash_file_));
      RETURN_IF_ERROR(data_fs_->Flush());
      RETURN_IF_ERROR(offset_fs_->Flush());
    }
    return absl::OkStatus();
  }

  // Close the depot.
  absl::Status Close() {
    if (data_fs_ && data_fs_->IsOpen() && offset_fs_ && offset_fs_->IsOpen()) {
      RETURN_IF_ERROR(Flush());
      RETURN_IF_ERROR(data_fs_->Close());
      RETURN_IF_ERROR(offset_fs_->Close());
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

  FileDepot(std::filesystem::path hash_file, FStream offset_fs, FStream data_fs,
            std::size_t hash_branching_factor, std::size_t hash_box_size)
      : hash_box_size_(hash_box_size),
        hash_file_(std::move(hash_file)),
        offset_fs_(std::make_unique<FStream>(std::move(offset_fs))),
        data_fs_(std::make_unique<FStream>(std::move(data_fs))),
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
  absl::StatusOr<OffsetAndSize> GetOffsetAndSize(const K& key) const {
    // Seek to the position of the key.
    RETURN_IF_ERROR(offset_fs_->Seekg(GetOffsetPosition(key), std::ios::beg));

    // Read offset and size.
    OffsetAndSize metadata;
    ASSIGN_OR_RETURN(auto length,
                     offset_fs_->ReadUntilEof(std::span(&metadata, 1)));

    // If no data were read, then entry does not exist.
    if (length == 0) return absl::NotFoundError("Key not found");

    return metadata;
  }

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / hash_box_size_;
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    PageProvider(FStream& data_fs, FStream& offset_fs,
                 std::size_t hash_box_size)
        : data_fs_(data_fs),
          offset_fs_(offset_fs),
          hash_box_size_(hash_box_size) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    absl::StatusOr<std::span<const std::byte>> GetPageData(PageId id) override {
      std::vector<OffsetAndSize> offset_buffer(hash_box_size_);
      const std::size_t lengths_size = hash_box_size_ * sizeof(Size);

      // read all offsets and sizes for the hash group
      RETURN_IF_ERROR(offset_fs_.Seekg(GetOffsetPosition(id * hash_box_size_),
                                       std::ios::beg));
      RETURN_IF_ERROR(offset_fs_.ReadUntilEof(
          std::span(offset_buffer.data(), hash_box_size_)));

      // set lengths to zero default value
      page_data_.resize(lengths_size);
      std::fill_n(page_data_.begin(), lengths_size, std::byte{0});

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
        return page_data_;
      }

      // add lengths size to total length and prepare buffer
      total_length += lengths_size;
      page_data_.resize(total_length);

      // fast path for non-fragmented data
      if (!is_fragmented) {
        RETURN_IF_ERROR(data_fs_.Seekg(start, std::ios::beg));
        RETURN_IF_ERROR(data_fs_.Read(std::span(
            page_data_.data() + lengths_size, total_length - lengths_size)));
        return page_data_;
      }

      // slow path for fragmented data
      std::size_t position = 0;
      for (std::size_t i = 0; i < hash_box_size_; ++i) {
        if (offset_buffer[i].size == 0) continue;
        RETURN_IF_ERROR(data_fs_.Seekg(offset_buffer[i].offset, std::ios::beg));
        RETURN_IF_ERROR(
            data_fs_.Read(std::span(page_data_.data() + lengths_size + position,
                                    offset_buffer[i].size)));
        position += offset_buffer[i].size;
      }

      return page_data_;
    }

   private:
    FStream& data_fs_;
    FStream& offset_fs_;
    const std::size_t hash_box_size_;
    std::vector<std::byte> page_data_;
  };

  // The amount of items that will be grouped into a single hashing group.
  const std::size_t hash_box_size_;

  // The name of the file to save hashes to.
  std::filesystem::path hash_file_;

  // It is used to get positions of the data in the data file.
  std::unique_ptr<FStream> offset_fs_;

  // It is used to get the actual data.
  std::unique_ptr<FStream> data_fs_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;

  // Temporary storage for the result of Get().
  mutable std::vector<std::byte> get_data_;
};

}  // namespace carmen::backend::depot
