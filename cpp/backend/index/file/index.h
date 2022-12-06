#pragma once

#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/file.h"
#include "backend/common/page_pool.h"
#include "backend/index/file/hash_page.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::index {

// A FileIndex implements the Index concept based on a linear hashing based data
// structure. Data is placed in three different files: two comprise lists of
// pages of key/value pairs, while the third contains metadata.
//
// All operations on this index require O(1) page accesses. In most cases, the
// operations only require to access a single page.
//
// This implementation is currently incomplete. The following steps are missing:
//  - implement meta data serialization
//  - implement stable hashing
// Thus, it can currently not be used to open existing data.
//
// Internally, Key/value pairs are mapped to buckets which are represented
// through linked lists of pages. The first, primary, page of each bucket is
// maintained in one file, while all remaining overflow pages are maintained in
// a second file. This simplifies the addressing of primary buckets and avoids
// exessive file growing steps when performing splitting operations.
//
// see: https://en.wikipedia.org/wiki/Linear_hashing
template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size = kFileSystemPageSize>
class FileIndex {
 public:
  using hash_t = std::size_t;
  using key_type = K;
  using value_type = I;

  // The page type used by this index.
  using Page = HashPage<hash_t, K, I, page_size>;

  // The file-type used by instances for primiary and overflow pages.
  using File = F<Page>;

  // A factory function creating an instance of this index type.
  static absl::StatusOr<FileIndex> Open(Context&,
                                        const std::filesystem::path& directory);

  // Creates a new, empty index backed by a default-constructed file.
  FileIndex();

  // Creates an index retaining its data in the given directory.
  FileIndex(std::filesystem::path directory);

  // File indexes are move-constructible.
  FileIndex(FileIndex&&) = default;

  // On destruction file indexes are automatically flushed and closed.
  ~FileIndex() { Close().IgnoreError(); }

  // Retrieves the ordinal number for the given key. If the key
  // is known, it it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  std::pair<I, bool> GetOrAdd(const K& key);

  // Retrieves the ordinal number for the given key if previously registered.
  // Otherwise std::nullopt is returned.
  std::optional<I> Get(const K& key) const;

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() const;

  // Flush unsafed index keys to disk.
  absl::Status Flush();

  // Close this index and release resources.
  absl::Status Close();

  // Prints the content of this index to std::cout. Mainly intended for manual
  // inspection and debugging.
  void Dump() const;

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  // A type used to index buckets.
  using bucket_id_t = std::size_t;
  // The type of one entry within a page (=one key/value pair).
  using Entry = typename Page::Entry;

  // A constant defining the page ID marking the end of a linked list of pages.
  constexpr static const PageId kNullPage = 0;

  // The log_2() of the initial size of an index.
  constexpr static const std::uint8_t kInitialHashLength = 2;

  // Creates an index based on the given files.
  FileIndex(std::unique_ptr<File> primary_page_file,
            std::unique_ptr<File> overflow_page_file,
            std::filesystem::path metadata_file = "");

  // A helper function to locate a entry in this map. Returns a tuple containing
  // the key's hash, the containing bucket, and the containing entry. Only if
  // the entry pointer is not-null the entry has been found.
  std::tuple<hash_t, bucket_id_t, const Entry*> FindInternal(
      const K& key) const;

  // Same as above, but for non-const instances.
  std::tuple<hash_t, bucket_id_t, Entry*> FindInternal(const K& key);

  // Splits one bucket in the hash table causing the table to grow by one
  // bucket.
  void Split();

  // Obtains the index of the bucket the given hash key is supposed to be
  // located in.
  bucket_id_t GetBucket(hash_t hashkey) const {
    bucket_id_t bucket = hashkey & high_mask_;
    return bucket >= num_buckets_ ? hashkey & low_mask_ : bucket;
  }

  // Returns the overflow page being the tail fo the given bucket. Returns
  // nullopt if the given bucket has no overflow pages.
  PageId GetTail(bucket_id_t bucket) const {
    if (bucket_tails_.size() <= bucket) {
      return kNullPage;
    }
    return bucket_tails_[bucket];
  }

  // Updates the tail page ID of the given bucket.
  void SetTail(bucket_id_t bucket, PageId overflow_page_id) {
    assert(overflow_page_id != kNullPage);
    bucket_tails_.resize(std::max(bucket_tails_.size(), bucket + 1));
    bucket_tails_[bucket] = overflow_page_id;
  }

  // Removes the tail page ID of the given bucket. This is used when a bucket is
  // split, resulting in the discarding of overflow pages.
  void ResetTail(bucket_id_t bucket) {
    if (bucket_tails_.size() >= bucket) {
      bucket_tails_[bucket] = kNullPage;
    }
  }

  // Obtains a page ID for an entry in the overflow page that can be used for a
  // new overflow page. Overflow pages may be created (during inserts or splits)
  // and released (during splits). To organize their reuse, a fee list is
  // maintained internally.
  PageId GetFreeOverflowPageId() {
    if (!overflow_page_free_list_.empty()) {
      // For the freelist we use a FIFO policy.
      auto res = overflow_page_free_list_.back();
      overflow_page_free_list_.pop_back();
      return res;
    }
    return num_overflow_pages_++;
  }

  // Signals that the overflow page with the given ID may be reused.
  void ReturnOverflowPage(PageId id) { overflow_page_free_list_.push_back(id); }

  // The page pool wrapping access to the primary page file.
  mutable PagePool<Page, F> primary_pool_;

  // The page pool wrapping access to the overflow page file.
  mutable PagePool<Page, F> overflow_pool_;

  // The file used to store meta information covering the values of the fields
  // below. The path is a unique ptr to manage ownership during moves.
  std::unique_ptr<std::filesystem::path> metadata_file_;

  // A hasher to compute hashes for keys.
  absl::Hash<K> key_hasher_;

  // The number of elements in this index.
  std::size_t size_ = 0;

  // The next bucket to be split.
  std::size_t next_to_split_ = 0;

  // The mask for mapping keys to buckets that have not yet been split in the
  // current bucket split iteration.
  std::size_t low_mask_;

  // The mask for mapping keys to buckets that have already been split in the
  // current bucket split iteration.
  std::size_t high_mask_;

  // ---- Bucket Management ----

  // The total number of buckets (=number of pages in primary page file).
  std::size_t num_buckets_;

  // The IDs of the overflow pages forming the tail of each bucket. The PageID
  // == 0 is reserved for marking the No-Page value.
  std::deque<PageId> bucket_tails_;

  // The size of the overflow page file.
  std::size_t num_overflow_pages_ = 1;  // page zero remains always unused

  // Free pages in the overflow pool, ready for reuse.
  std::vector<PageId> overflow_page_free_list_;

  // ---- Hash Support ----

  mutable std::queue<K> unhashed_keys_;
  mutable Sha256Hasher hasher_;
  mutable Hash hash_;
};

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
absl::StatusOr<FileIndex<K, I, F, page_size>>
FileIndex<K, I, F, page_size>::Open(Context&,
                                    const std::filesystem::path& directory) {
  // TODO: move directory initialization from constructor to factory and do
  // proper error handling.
  return FileIndex(directory);
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
FileIndex<K, I, F, page_size>::FileIndex()
    : FileIndex(std::make_unique<File>(), std::make_unique<File>()) {}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
FileIndex<K, I, F, page_size>::FileIndex(std::filesystem::path directory)
    : FileIndex(std::make_unique<File>(directory / "primary.dat"),
                std::make_unique<File>(directory / "overflow.dat"),
                directory / "metadata.dat") {}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
FileIndex<K, I, F, page_size>::FileIndex(
    std::unique_ptr<File> primary_page_file,
    std::unique_ptr<File> overflow_page_file,
    std::filesystem::path metadata_file)
    : primary_pool_(std::move(primary_page_file)),
      overflow_pool_(std::move(overflow_page_file)),
      low_mask_((1 << kInitialHashLength) - 1),
      high_mask_((low_mask_ << 1) | 0x1),
      num_buckets_(1 << kInitialHashLength) {
  // Take ownership of the metadata file and track it through a unique ptr.
  metadata_file_ =
      std::make_unique<std::filesystem::path>(std::move(metadata_file));
  if (!std::filesystem::exists(*metadata_file_)) return;

  // Load metadata from file.
  std::fstream in(*metadata_file_, std::ios::binary | std::ios::in);
  auto read_scalar = [&](auto& scalar) {
    in.read(reinterpret_cast<char*>(&scalar), sizeof(scalar));
  };

  // Start with scalars.
  read_scalar(size_);
  read_scalar(next_to_split_);
  read_scalar(low_mask_);
  read_scalar(high_mask_);
  read_scalar(num_buckets_);
  read_scalar(num_overflow_pages_);
  read_scalar(hash_);

  // Read bucket tail list.
  assert(sizeof(bucket_tails_.size()) == sizeof(std::size_t));
  std::size_t size;
  read_scalar(size);
  bucket_tails_.resize(size);
  for (std::size_t i = 0; i < size; i++) {
    read_scalar(bucket_tails_[i]);
  }

  // Read free list.
  assert(sizeof(overflow_page_free_list_.size()) == sizeof(std::size_t));
  read_scalar(size);
  overflow_page_free_list_.resize(size);
  for (std::size_t i = 0; i < size; i++) {
    read_scalar(overflow_page_free_list_[i]);
  }
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
std::pair<I, bool> FileIndex<K, I, F, page_size>::GetOrAdd(const K& key) {
  auto [hash, bucket, entry] = FindInternal(key);
  if (entry != nullptr) {
    return {entry->value, false};
  }

  size_++;

  // Trigger a split if the bucket has a overflow bucket.
  if (GetTail(bucket) != kNullPage) {
    Split();

    // After the split, the target bucket may be a different one.
    bucket = GetBucket(hash);
  }

  // Insert a new entry.
  Page* page = nullptr;
  auto tail = GetTail(bucket);
  if (tail == kNullPage) {
    page = &primary_pool_.Get(bucket);
    primary_pool_.MarkAsDirty(bucket);
  } else {
    page = &overflow_pool_.Get(tail);
    overflow_pool_.MarkAsDirty(tail);
  }

  if (page->Insert(hash, key, size_ - 1) == nullptr) {
    auto new_overflow_id = GetFreeOverflowPageId();
    page->SetNext(new_overflow_id);
    auto overflow_page = &overflow_pool_.Get(new_overflow_id);
    assert(overflow_page->Size() == 0);
    assert(overflow_page->GetNext() == 0);
    SetTail(bucket, new_overflow_id);
    overflow_page->Insert(hash, key, size_ - 1);
    overflow_pool_.MarkAsDirty(new_overflow_id);
  }

  unhashed_keys_.push(key);
  return {size_ - 1, true};
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
std::optional<I> FileIndex<K, I, F, page_size>::Get(const K& key) const {
  auto [hash, bucket, entry] = FindInternal(key);
  if (entry == nullptr) {
    return std::nullopt;
  }
  return entry->value;
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
absl::StatusOr<Hash> FileIndex<K, I, F, page_size>::GetHash() const {
  while (!unhashed_keys_.empty()) {
    hash_ = carmen::GetHash(hasher_, hash_, unhashed_keys_.front());
    unhashed_keys_.pop();
  }
  return hash_;
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
absl::Status FileIndex<K, I, F, page_size>::Flush() {
  primary_pool_.Flush();
  overflow_pool_.Flush();

  // Flush metadata if this is an owning instance.
  if (!metadata_file_ || metadata_file_->empty()) return absl::OkStatus();

  // Sync out metadata information.
  std::fstream out(*metadata_file_, std::ios::binary | std::ios::out);
  auto write_scalar = [&](auto scalar) {
    out.write(reinterpret_cast<const char*>(&scalar), sizeof(scalar));
  };

  // Start with scalars.
  write_scalar(size_);
  write_scalar(next_to_split_);
  write_scalar(low_mask_);
  write_scalar(high_mask_);
  write_scalar(num_buckets_);
  write_scalar(num_overflow_pages_);
  auto hash = *GetHash();
  write_scalar(hash);

  // Write bucket tail list.
  write_scalar(bucket_tails_.size());
  for (const auto& page_id : bucket_tails_) {
    write_scalar(page_id);
  }

  // Write free list.
  write_scalar(overflow_page_free_list_.size());
  for (const auto& page_id : overflow_page_free_list_) {
    write_scalar(page_id);
  }
  return absl::OkStatus();
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
absl::Status FileIndex<K, I, F, page_size>::Close() {
  RETURN_IF_ERROR(Flush());
  primary_pool_.Close();
  overflow_pool_.Close();
  return absl::OkStatus();
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
void FileIndex<K, I, F, page_size>::Dump() const {
  std::cout << "\n-----------------------------------------------------\n";
  std::cout << "FileIndex containing " << size_ << " elements in "
            << num_buckets_ << " buckets\n";
  for (std::size_t i = 0; i < num_buckets_; i++) {
    std::cout << "\tBucket " << i << ":\n";
    Page* page = &primary_pool_.Get(i);
    while (page != nullptr) {
      page->Dump();
      auto next = page->GetNext();
      page = next == 0 ? nullptr : &overflow_pool_.Get(next);
    }
  }
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
MemoryFootprint FileIndex<K, I, F, page_size>::GetMemoryFootprint() const {
  MemoryFootprint res(*this);
  res.Add("primary_pool", primary_pool_.GetMemoryFootprint());
  res.Add("overflow_pool", overflow_pool_.GetMemoryFootprint());
  res.Add("bucket_tails", SizeOf(bucket_tails_));
  res.Add("free_list", SizeOf(overflow_page_free_list_));
  return res;
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
std::tuple<typename FileIndex<K, I, F, page_size>::hash_t,
           typename FileIndex<K, I, F, page_size>::bucket_id_t,
           const typename FileIndex<K, I, F, page_size>::Entry*>
FileIndex<K, I, F, page_size>::FindInternal(const K& key) const {
  auto hash = key_hasher_(key);
  auto bucket = GetBucket(hash);

  // Search within that bucket.
  Page* cur = &primary_pool_.Get(bucket);
  while (cur != nullptr) {
    if (auto entry = cur->Find(hash, key)) {
      return {hash, bucket, entry};
    }
    PageId next = cur->GetNext();
    cur = next != 0 ? &overflow_pool_.Get(next) : nullptr;
  }

  // Report a nullpointer if nothing was found.
  return {hash, bucket, nullptr};
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
std::tuple<typename FileIndex<K, I, F, page_size>::hash_t,
           typename FileIndex<K, I, F, page_size>::bucket_id_t,
           typename FileIndex<K, I, F, page_size>::Entry*>
FileIndex<K, I, F, page_size>::FindInternal(const K& key) {
  auto [hash, bucket, entry] =
      const_cast<const FileIndex*>(this)->FindInternal(key);
  return {hash, bucket, const_cast<Entry*>(entry)};
}

template <Trivial K, std::integral I, template <typename> class F,
          std::size_t page_size>
void FileIndex<K, I, F, page_size>::Split() {
  assert(next_to_split_ < num_buckets_);

  // When a full cycle is completed ...
  if (next_to_split_ > low_mask_) {
    // ... increase the hash mask by one bit ...
    low_mask_ = high_mask_;
    high_mask_ = (high_mask_ << 1) | 0x1;
    // ... and start at zero again.
    next_to_split_ = 0;
  }

  auto old_bucket_id = next_to_split_++;
  auto new_bucket_id = num_buckets_++;

  // Load data from page to be split into memory.
  std::deque<Entry> entries;
  Page* page = &primary_pool_.Get(old_bucket_id);
  while (page != nullptr) {
    for (std::size_t i = 0; i < page->Size(); i++) {
      entries.push_back((*page)[i]);
    }
    auto next = page->GetNext();
    if (next != 0) {
      page = &overflow_pool_.Get(next);
    } else {
      page = nullptr;
    }
  }

  // Split entries into subsets.
  std::vector<Entry> old_bucket;
  std::vector<Entry> new_bucket;
  old_bucket.reserve(entries.size());
  new_bucket.reserve(entries.size());

  // Distribute keys between old and new bucket.
  const auto mask = low_mask_ ^ high_mask_;
  for (const Entry& cur : entries) {
    if (cur.hash & mask) {
      new_bucket.push_back(cur);
    } else {
      old_bucket.push_back(cur);
    }
  }

  // Sort entry lists by their hash.
  std::sort(old_bucket.begin(), old_bucket.end());
  std::sort(new_bucket.begin(), new_bucket.end());

  // Write old entries into old bucket.
  page = &primary_pool_.Get(old_bucket_id);
  primary_pool_.MarkAsDirty(old_bucket_id);
  int i = 0;
  ResetTail(old_bucket_id);
  for (const Entry& entry : old_bucket) {
    if (i == Page::kNumEntries) {
      // Need to move on to next page.
      page->Resize(Page::kNumEntries);
      auto next = page->GetNext();
      assert(next != 0);
      page = &overflow_pool_.Get(next);
      overflow_pool_.MarkAsDirty(next);
      SetTail(old_bucket_id, next);
      i = 0;
    }
    (*page)[i++] = entry;
  }
  auto remaining = old_bucket.size() % Page::kNumEntries;
  page->Resize(remaining == 0 ? Page::kNumEntries : remaining);

  // Free remaining overflow pages.
  while (page != nullptr) {
    auto next = page->GetNext();
    if (next != 0) {
      page->SetNext(0);
      ReturnOverflowPage(next);
      page = &overflow_pool_.Get(next);
      page->Resize(0);
      overflow_pool_.MarkAsDirty(next);
    } else {
      page = nullptr;
    }
  }

  // Write new entries into new bucket.
  page = &primary_pool_.Get(new_bucket_id);
  i = 0;
  primary_pool_.MarkAsDirty(new_bucket_id);
  for (const Entry& entry : new_bucket) {
    if (i == Page::kNumEntries) {
      // Need to add an overflow page.
      page->Resize(Page::kNumEntries);
      auto next = GetFreeOverflowPageId();
      page->SetNext(next);
      page = &overflow_pool_.Get(next);
      overflow_pool_.MarkAsDirty(next);
      assert(page->GetNext() == 0);
      SetTail(new_bucket_id, next);
      i = 0;
    }
    (*page)[i++] = entry;
  }
  remaining = new_bucket.size() % Page::kNumEntries;
  page->Resize(remaining == 0 ? Page::kNumEntries : remaining);
}

}  // namespace carmen::backend::index
