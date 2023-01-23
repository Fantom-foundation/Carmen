#pragma once

#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "backend/common/file.h"
#include "backend/common/page_pool.h"
#include "backend/index/file/hash_page.h"
#include "backend/index/file/stable_hash.h"
#include "backend/structure.h"
#include "common/fstream.h"
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
//  - implement metadata serialization
//  - implement stable hashing
// Thus, it can currently not be used to open existing data.
//
// Internally, Key/value pairs are mapped to buckets which are represented
// through linked lists of pages. The first, primary, page of each bucket is
// maintained in one file, while all remaining overflow pages are maintained in
// a second file. This simplifies the addressing of primary buckets and avoids
// excessive file growing steps when performing splitting operations.
//
// see: https://en.wikipedia.org/wiki/Linear_hashing
template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size = kFileSystemPageSize>
class FileIndex {
 public:
  using hash_t = std::size_t;
  using key_type = K;
  using value_type = I;

  // The page type used by this index.
  using Page = HashPage<hash_t, K, I, page_size>;

  // The file-type used by instances for primary and overflow pages.
  using File = F<sizeof(Page)>;

  // A factory function creating an instance of this index type.
  static absl::StatusOr<FileIndex> Open(Context&,
                                        const std::filesystem::path& directory);

  // File indexes are move-constructable.
  FileIndex(FileIndex&&) = default;

  // On destruction file indexes are automatically flushed and closed.
  ~FileIndex() { Close().IgnoreError(); }

  // Retrieves the ordinal number for the given key. If the key
  // is known, it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  absl::StatusOr<std::pair<I, bool>> GetOrAdd(const K& key);

  // Retrieves the ordinal number for the given key if previously registered.
  // Otherwise, returns a not found status.
  absl::StatusOr<I> Get(const K& key) const;

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() const;

  // Flush unsaved index keys to disk.
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
            std::unique_ptr<std::filesystem::path> metadata_file);

  // A helper function to locate an entry in this map. Returns a tuple
  // containing the key's hash, the containing bucket, and the containing entry.
  // Only if the entry pointer is not-null the entry has been found.
  absl::StatusOr<std::tuple<hash_t, bucket_id_t, const Entry*>> FindInternal(
      const K& key) const;

  // Same as above, but for non-const instances.
  absl::StatusOr<std::tuple<hash_t, bucket_id_t, Entry*>> FindInternal(
      const K& key);

  // Splits one bucket in the hash table causing the table to grow by one
  // bucket.
  absl::Status Split();

  // Obtains the index of the bucket the given hash key is supposed to be
  // located in.
  bucket_id_t GetBucket(hash_t hash_key) const {
    bucket_id_t bucket = hash_key & high_mask_;
    return bucket >= num_buckets_ ? hash_key & low_mask_ : bucket;
  }

  // Returns the overflow page being the tail fo the given bucket. Returns
  // defined null value if the given bucket has no overflow pages.
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
  mutable PagePool<File> primary_pool_;

  // The page pool wrapping access to the overflow page file.
  mutable PagePool<File> overflow_pool_;

  // The file used to store meta information covering the values of the fields
  // below. The path is a unique ptr to manage ownership during moves.
  std::unique_ptr<std::filesystem::path> metadata_file_;

  // A hasher to compute hashes for keys.
  StableHash<K> key_hasher_;

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

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::StatusOr<FileIndex<K, I, F, page_size>>
FileIndex<K, I, F, page_size>::Open(Context&,
                                    const std::filesystem::path& directory) {
  ASSIGN_OR_RETURN(auto primary_page_file,
                   File::Open(directory / "primary.dat"));
  ASSIGN_OR_RETURN(auto overflow_page_file,
                   File::Open(directory / "overflow.dat"));

  auto index = FileIndex(
      std::make_unique<File>(std::move(primary_page_file)),
      std::make_unique<File>(std::move(overflow_page_file)),
      std::make_unique<std::filesystem::path>(directory / "metadata.dat"));
  if (!std::filesystem::exists(*index.metadata_file_)) {
    return index;
  }

  ASSIGN_OR_RETURN(auto in, FStream::Open(*index.metadata_file_,
                                          std::ios::binary | std::ios::in));

  // Start with scalars.
  RETURN_IF_ERROR(in.Read(index.size_));
  RETURN_IF_ERROR(in.Read(index.next_to_split_));
  RETURN_IF_ERROR(in.Read(index.low_mask_));
  RETURN_IF_ERROR(in.Read(index.high_mask_));
  RETURN_IF_ERROR(in.Read(index.num_buckets_));
  RETURN_IF_ERROR(in.Read(index.num_overflow_pages_));
  RETURN_IF_ERROR(in.Read(index.hash_));

  // Read bucket tail list.
  assert(sizeof(index.bucket_tails_.size()) == sizeof(std::size_t));
  std::size_t size;
  RETURN_IF_ERROR(in.Read(size));
  index.bucket_tails_.resize(size);
  for (std::size_t i = 0; i < size; i++) {
    RETURN_IF_ERROR(in.Read(index.bucket_tails_[i]));
  }

  // Read free list.
  assert(sizeof(index.overflow_page_free_list_.size()) == sizeof(std::size_t));
  RETURN_IF_ERROR(in.Read(size));
  index.overflow_page_free_list_.resize(size);
  for (std::size_t i = 0; i < size; i++) {
    RETURN_IF_ERROR(in.Read(index.overflow_page_free_list_[i]));
  }

  return index;
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
FileIndex<K, I, F, page_size>::FileIndex(
    std::unique_ptr<File> primary_page_file,
    std::unique_ptr<File> overflow_page_file,
    std::unique_ptr<std::filesystem::path> metadata_file)
    : primary_pool_(std::move(primary_page_file)),
      overflow_pool_(std::move(overflow_page_file)),
      metadata_file_(std::move(metadata_file)),
      low_mask_((1 << kInitialHashLength) - 1),
      high_mask_((low_mask_ << 1) | 0x1),
      num_buckets_(1 << kInitialHashLength) {}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::StatusOr<std::pair<I, bool>> FileIndex<K, I, F, page_size>::GetOrAdd(
    const K& key) {
  ASSIGN_OR_RETURN((auto [hash, bucket, entry]), FindInternal(key));
  if (entry != nullptr) {
    return std::pair{entry->value, false};
  }

  size_++;

  // Trigger a split if the bucket has an overflow bucket.
  if (GetTail(bucket) != kNullPage) {
    RETURN_IF_ERROR(Split());

    // After the split, the target bucket may be a different one.
    bucket = GetBucket(hash);
  }

  // Insert a new entry.
  Page* page;
  auto tail = GetTail(bucket);
  if (tail == kNullPage) {
    ASSIGN_OR_RETURN(page, primary_pool_.template Get<Page>(bucket));
    primary_pool_.MarkAsDirty(bucket);
  } else {
    ASSIGN_OR_RETURN(page, overflow_pool_.template Get<Page>(tail));
    overflow_pool_.MarkAsDirty(tail);
  }

  if (page->Insert(hash, key, size_ - 1) == nullptr) {
    auto new_overflow_id = GetFreeOverflowPageId();
    page->SetNext(new_overflow_id);
    ASSIGN_OR_RETURN(Page * overflow_page,
                     overflow_pool_.template Get<Page>(new_overflow_id));
    assert(overflow_page->Size() == 0);
    assert(overflow_page->GetNext() == 0);
    SetTail(bucket, new_overflow_id);
    overflow_page->Insert(hash, key, size_ - 1);
    overflow_pool_.MarkAsDirty(new_overflow_id);
  }

  unhashed_keys_.push(key);
  return std::pair{size_ - 1, true};
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::StatusOr<I> FileIndex<K, I, F, page_size>::Get(const K& key) const {
  ASSIGN_OR_RETURN((auto [hash, bucket, entry]), FindInternal(key));
  if (entry == nullptr) {
    return absl::NotFoundError("Key not found.");
  }
  return entry->value;
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::StatusOr<Hash> FileIndex<K, I, F, page_size>::GetHash() const {
  while (!unhashed_keys_.empty()) {
    hash_ = carmen::GetHash(hasher_, hash_, unhashed_keys_.front());
    unhashed_keys_.pop();
  }
  return hash_;
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::Status FileIndex<K, I, F, page_size>::Flush() {
  RETURN_IF_ERROR(primary_pool_.Flush());
  RETURN_IF_ERROR(overflow_pool_.Flush());

  // Flush metadata if this is an owning instance.
  if (!metadata_file_ || metadata_file_->empty()) return absl::OkStatus();

  // Sync out metadata information.
  ASSIGN_OR_RETURN(auto out, FStream::Open(*metadata_file_,
                                           std::ios::binary | std::ios::out));

  // Start with scalars.
  RETURN_IF_ERROR(out.Write(size_));
  RETURN_IF_ERROR(out.Write(next_to_split_));
  RETURN_IF_ERROR(out.Write(low_mask_));
  RETURN_IF_ERROR(out.Write(high_mask_));
  RETURN_IF_ERROR(out.Write(num_buckets_));
  RETURN_IF_ERROR(out.Write(num_overflow_pages_));
  ASSIGN_OR_RETURN(auto hash, GetHash());
  RETURN_IF_ERROR(out.Write(hash));

  // Write bucket tail list.
  RETURN_IF_ERROR(out.Write(bucket_tails_.size()));
  for (const auto& page_id : bucket_tails_) {
    RETURN_IF_ERROR(out.Write(page_id));
  }

  // Write free list.
  RETURN_IF_ERROR(out.Write(overflow_page_free_list_.size()));
  for (const auto& page_id : overflow_page_free_list_) {
    RETURN_IF_ERROR(out.Write(page_id));
  }
  return absl::OkStatus();
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::Status FileIndex<K, I, F, page_size>::Close() {
  RETURN_IF_ERROR(Flush());
  RETURN_IF_ERROR(primary_pool_.Close());
  RETURN_IF_ERROR(overflow_pool_.Close());
  return absl::OkStatus();
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
void FileIndex<K, I, F, page_size>::Dump() const {
  std::cout << "\n-----------------------------------------------------\n";
  std::cout << "FileIndex containing " << size_ << " elements in "
            << num_buckets_ << " buckets\n";
  for (std::size_t i = 0; i < num_buckets_; i++) {
    std::cout << "\tBucket " << i << ":\n";
    auto result = primary_pool_.template Get<Page>(i);
    if (!result.ok()) {
      std::cout << "\t\tError: " << result.status() << "\n";
      continue;
    }
    Page* page = result->AsPointer();
    while (page != nullptr) {
      page->Dump();
      auto next = page->GetNext();
      result = overflow_pool_.template Get<Page>(next);
      if (!result.ok()) {
        std::cout << "\t\tError: " << result.status() << "\n";
        break;
      }
      page = next == 0 ? nullptr : result.AsPointer();
    }
  }
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
MemoryFootprint FileIndex<K, I, F, page_size>::GetMemoryFootprint() const {
  MemoryFootprint res(*this);
  res.Add("primary_pool", primary_pool_.GetMemoryFootprint());
  res.Add("overflow_pool", overflow_pool_.GetMemoryFootprint());
  res.Add("bucket_tails", SizeOf(bucket_tails_));
  res.Add("free_list", SizeOf(overflow_page_free_list_));
  return res;
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::StatusOr<std::tuple<typename FileIndex<K, I, F, page_size>::hash_t,
                          typename FileIndex<K, I, F, page_size>::bucket_id_t,
                          const typename FileIndex<K, I, F, page_size>::Entry*>>
FileIndex<K, I, F, page_size>::FindInternal(const K& key) const {
  auto hash = key_hasher_(key);
  auto bucket = GetBucket(hash);

  // Search within that bucket.
  ASSIGN_OR_RETURN(Page * cur, primary_pool_.template Get<Page>(bucket));
  while (cur != nullptr) {
    if (auto entry = cur->Find(hash, key)) {
      return std::tuple{hash, bucket, entry};
    }
    PageId next = cur->GetNext();
    ASSIGN_OR_RETURN(cur, overflow_pool_.template Get<Page>(next));
    cur = next != 0 ? cur : nullptr;
  }

  // Report a null pointer if nothing was found.
  return std::tuple{hash, bucket, nullptr};
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::StatusOr<std::tuple<typename FileIndex<K, I, F, page_size>::hash_t,
                          typename FileIndex<K, I, F, page_size>::bucket_id_t,
                          typename FileIndex<K, I, F, page_size>::Entry*>>
FileIndex<K, I, F, page_size>::FindInternal(const K& key) {
  ASSIGN_OR_RETURN((auto [hash, bucket, entry]),
                   const_cast<const FileIndex*>(this)->FindInternal(key));
  return std::tuple{hash, bucket, const_cast<Entry*>(entry)};
}

template <Trivial K, std::integral I, template <std::size_t> class F,
          std::size_t page_size>
absl::Status FileIndex<K, I, F, page_size>::Split() {
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
  ASSIGN_OR_RETURN(Page * page,
                   primary_pool_.template Get<Page>(old_bucket_id));
  while (page != nullptr) {
    for (std::size_t i = 0; i < page->Size(); i++) {
      entries.push_back((*page)[i]);
    }
    auto next = page->GetNext();
    if (next != 0) {
      ASSIGN_OR_RETURN(page, overflow_pool_.template Get<Page>(next));
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
  ASSIGN_OR_RETURN(page, primary_pool_.template Get<Page>(old_bucket_id));
  primary_pool_.MarkAsDirty(old_bucket_id);
  int i = 0;
  ResetTail(old_bucket_id);
  for (const Entry& entry : old_bucket) {
    if (i == Page::kNumEntries) {
      // Need to move on to next page.
      page->Resize(Page::kNumEntries);
      auto next = page->GetNext();
      assert(next != 0);
      ASSIGN_OR_RETURN(page, overflow_pool_.template Get<Page>(next));
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
      ASSIGN_OR_RETURN(page, overflow_pool_.template Get<Page>(next));
      page->Resize(0);
      overflow_pool_.MarkAsDirty(next);
    } else {
      page = nullptr;
    }
  }

  // Write new entries into new bucket.
  ASSIGN_OR_RETURN(page, primary_pool_.template Get<Page>(new_bucket_id));
  i = 0;
  primary_pool_.MarkAsDirty(new_bucket_id);
  for (const Entry& entry : new_bucket) {
    if (i == Page::kNumEntries) {
      // Need to add an overflow page.
      page->Resize(Page::kNumEntries);
      auto next = GetFreeOverflowPageId();
      page->SetNext(next);
      ASSIGN_OR_RETURN(page, overflow_pool_.template Get<Page>(next));
      overflow_pool_.MarkAsDirty(next);
      assert(page->GetNext() == 0);
      SetTail(new_bucket_id, next);
      i = 0;
    }
    (*page)[i++] = entry;
  }
  remaining = new_bucket.size() % Page::kNumEntries;
  page->Resize(remaining == 0 ? Page::kNumEntries : remaining);

  return absl::OkStatus();
}

}  // namespace carmen::backend::index
