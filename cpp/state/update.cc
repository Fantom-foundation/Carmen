/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "state/update.h"

#include <algorithm>
#include <span>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen {
namespace {

constexpr const std::uint8_t kVersion0 = 0;

class Reader {
 public:
  Reader(std::span<const std::byte> data) : data_(data) {}

  absl::StatusOr<std::uint8_t> ReadUint8() {
    RETURN_IF_ERROR(CheckEnd(1));
    return std::uint8_t(data_[pos_++]);
  }

  absl::StatusOr<std::uint16_t> ReadUint16() {
    RETURN_IF_ERROR(CheckEnd(2));
    auto res = std::uint16_t(data_[pos_]) << 8 | std::uint16_t(data_[pos_ + 1]);
    pos_ += 2;
    return res;
  }

  absl::StatusOr<std::uint32_t> ReadUint32() {
    RETURN_IF_ERROR(CheckEnd(4));
    auto res = std::uint32_t(data_[pos_]) << 24 |
               std::uint32_t(data_[pos_ + 1]) << 16 |
               std::uint32_t(data_[pos_ + 2]) << 8 |
               std::uint32_t(data_[pos_ + 3]);
    pos_ += 4;
    return res;
  }

  absl::StatusOr<std::vector<std::byte>> ReadBytes(int length) {
    RETURN_IF_ERROR(CheckEnd(length));
    std::vector<std::byte> result;
    result.resize(length);
    std::memcpy(result.data(), data_.data() + pos_, length);
    pos_ += length;
    return result;
  }

  template <Trivial T>
  absl::StatusOr<T> Read() {
    RETURN_IF_ERROR(CheckEnd(sizeof(T)));
    T result;
    std::memcpy(&result, data_.data() + pos_, sizeof(T));
    pos_ += sizeof(T);
    return result;
  }

  template <Trivial T>
  absl::StatusOr<std::vector<T>> ReadList(std::size_t length) {
    std::vector<T> result;
    result.reserve(length);
    for (std::size_t i = 0; i < length; i++) {
      ASSIGN_OR_RETURN(auto cur, Read<T>());
      result.push_back(std::move(cur));
    }
    return result;
  }

  absl::StatusOr<std::vector<Update::CodeUpdate>> ReadCodeUpdates(
      std::size_t length) {
    std::vector<Update::CodeUpdate> result;
    result.reserve(length);
    for (std::size_t i = 0; i < length; i++) {
      ASSIGN_OR_RETURN(auto address, Read<Address>());
      ASSIGN_OR_RETURN(auto len, ReadUint16());
      ASSIGN_OR_RETURN(auto code, ReadBytes(len));
      result.push_back(Update::CodeUpdate{address, Code(std::move(code))});
    }
    return result;
  }

 private:
  absl::Status CheckEnd(std::size_t needed_bytes) {
    return pos_ + needed_bytes > data_.size()
               ? absl::InvalidArgumentError("end of data")
               : absl::OkStatus();
  }

  std::span<const std::byte> data_;

  std::size_t pos_ = 0;
};

class Writer {
 public:
  Writer(std::size_t size) { buffer_.reserve(size); }

  Writer& Append(std::uint8_t value) {
    buffer_.push_back(std::byte(value));
    return *this;
  }

  Writer& Append(std::uint16_t value) {
    // Make sure values are written in big-endian.
    buffer_.push_back(std::byte(value >> 8));
    buffer_.push_back(std::byte(value));
    return *this;
  }

  Writer& Append(std::uint32_t value) {
    // Make sure values are written in big-endian.
    buffer_.push_back(std::byte(value >> 24));
    buffer_.push_back(std::byte(value >> 16));
    buffer_.push_back(std::byte(value >> 8));
    buffer_.push_back(std::byte(value));
    return *this;
  }

  template <Trivial T>
  Writer& Append(const T& value) {
    auto span = std::as_bytes(std::span(&value, 1));
    buffer_.insert(buffer_.end(), span.begin(), span.end());
    return *this;
  }

  Writer& Append(std::span<const Update::CodeUpdate> list) {
    for (auto& cur : list) {
      Append(cur.account);
      std::span<const std::byte> code = cur.code;
      Append(std::uint16_t(code.size()));
      Append(code);
    }
    return *this;
  }

  template <Trivial T>
  Writer& Append(std::span<const T> list) {
    for (auto& cur : list) {
      Append(cur);
    }
    return *this;
  }

  template <Trivial T>
  Writer& Append(const std::vector<T>& list) {
    return Append(std::span<const T>(list));
  }

  std::size_t Size() const { return buffer_.size(); }

  std::vector<std::byte> Build() && { return std::move(buffer_); }

 private:
  std::vector<std::byte> buffer_;
};

}  // namespace

absl::StatusOr<Update> Update::FromBytes(std::span<const std::byte> data) {
  // The encoding should at least have the version number and the number of
  // entries.
  if (data.size() < 1 + 6 * 4) {
    return absl::InvalidArgumentError(
        "Encoded update has less than minimum length.");
  }

  // Decode the version number and lengths.
  Reader reader(data);
  ASSIGN_OR_RETURN(auto version, reader.ReadUint8());
  if (version != kVersion0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Invalid version number: %d", version));
  }

  ASSIGN_OR_RETURN(auto deleted_account_size, reader.ReadUint32());
  ASSIGN_OR_RETURN(auto created_account_size, reader.ReadUint32());
  ASSIGN_OR_RETURN(auto balances_size, reader.ReadUint32());
  ASSIGN_OR_RETURN(auto codes_size, reader.ReadUint32());
  ASSIGN_OR_RETURN(auto nonces_size, reader.ReadUint32());
  ASSIGN_OR_RETURN(auto storage_size, reader.ReadUint32());

  Update update;
  ASSIGN_OR_RETURN(update.deleted_accounts_,
                   reader.ReadList<Address>(deleted_account_size));
  ASSIGN_OR_RETURN(update.created_accounts_,
                   reader.ReadList<Address>(created_account_size));
  ASSIGN_OR_RETURN(update.balances_,
                   reader.ReadList<Update::BalanceUpdate>(balances_size));
  ASSIGN_OR_RETURN(update.codes_, reader.ReadCodeUpdates(codes_size));
  ASSIGN_OR_RETURN(update.nonces_,
                   reader.ReadList<Update::NonceUpdate>(nonces_size));
  ASSIGN_OR_RETURN(update.storage_,
                   reader.ReadList<Update::SlotUpdate>(storage_size));

  return update;
}

bool Update::Empty() const {
  return deleted_accounts_.empty() && created_accounts_.empty() &&
         balances_.empty() && nonces_.empty() && codes_.empty() &&
         storage_.empty();
}

absl::StatusOr<Hash> Update::GetHash() const {
  ASSIGN_OR_RETURN(auto data, ToBytes());
  return GetSha256Hash(data);
}

absl::StatusOr<std::vector<std::byte>> Update::ToBytes() const {
  // Compute the total size of required buffer.
  std::size_t size = 1;  // the version number
  size += 6 * 4;         // 4 bytes for the length  of the respective list
  size += deleted_accounts_.size() * sizeof(Address);
  size += created_accounts_.size() * sizeof(Address);
  size += balances_.size() * (sizeof(Address) + sizeof(Balance));
  size += nonces_.size() * (sizeof(Address) + sizeof(Nonce));
  size += storage_.size() * (sizeof(Address) + sizeof(Key) + sizeof(Value));
  for (auto& [_, code] : codes_) {
    size += sizeof(Address) + code.Size() + 2;  // 2 bytes for the code length
  }

  // Allocate the buffer.
  Writer out(size);

  // Start by version number and length of lists.
  out.Append(kVersion0);
  out.Append(std::uint32_t(deleted_accounts_.size()));
  out.Append(std::uint32_t(created_accounts_.size()));
  out.Append(std::uint32_t(balances_.size()));
  out.Append(std::uint32_t(codes_.size()));
  out.Append(std::uint32_t(nonces_.size()));
  out.Append(std::uint32_t(storage_.size()));

  // Followed by the serialization of the individual lists.
  out.Append(GetDeletedAccounts());
  out.Append(GetCreatedAccounts());
  out.Append(GetBalances());
  out.Append(GetCodes());
  out.Append(GetNonces());
  out.Append(GetStorage());

  assert(out.Size() == size);
  return std::move(out).Build();
}

absl::flat_hash_map<Address, AccountUpdate> AccountUpdate::From(
    const Update& update) {
  absl::flat_hash_map<Address, AccountUpdate> res;
  for (const auto& address : update.GetCreatedAccounts()) {
    res[address].created = true;
  }
  for (const auto& address : update.GetDeletedAccounts()) {
    res[address].deleted = true;
  }
  for (const auto& [address, balance] : update.GetBalances()) {
    res[address].balance = balance;
  }
  for (const auto& [address, nonce] : update.GetNonces()) {
    res[address].nonce = nonce;
  }
  for (const auto& [address, code] : update.GetCodes()) {
    res[address].code = code;
  }
  for (const auto& [address, key, value] : update.GetStorage()) {
    res[address].storage.push_back({key, value});
  }
  return res;
}

absl::Status AccountUpdate::IsNormalized() const {
  for (std::size_t i = 1; i < storage.size(); i++) {
    if (storage[i - 1].key >= storage[i].key) {
      return absl::InternalError(
          "Slot updates not in order or contains collisions.");
    }
  }
  return absl::OkStatus();
}

absl::Status AccountUpdate::Normalize() {
  // Sort updates.
  std::sort(storage.begin(), storage.end(),
            [](const auto& a, const auto& b) { return a.key < b.key; });

  // Remove duplciates.
  auto last = std::unique(storage.begin(), storage.end());
  storage.erase(last, storage.end());

  // Check for collisions.
  if (auto status = IsNormalized(); !status.ok()) {
    return absl::InvalidArgumentError(
        "Slot updates containes conflicting updates.");
  }
  return absl::OkStatus();
}

Hash AccountUpdate::GetHash() const {
  // The hash of an account update is computed by hashing a byte string composed
  // as followes:
  //   - a byte summarizing creation/deletion events; bit 0 is set if the
  //   account is created, bit 1 is set if the a account is deleted.
  //   - the 16 byte of the updated balance, if it was updated
  //   - the 8 byte of the updated nonce, if it was updated
  //   - the new code, if it was updated
  //   - the concatenated list of updated slots
  Sha256Hasher hasher;
  std::uint8_t state_change =
      (created ? 1 : 0) | (deleted ? 2 : 0) | (balance.has_value() ? 4 : 0) |
      (nonce.has_value() ? 8 : 0) | (code.has_value() ? 16 : 0);
  hasher.Ingest(state_change);
  if (balance.has_value()) {
    hasher.Ingest(*balance);
  }
  if (nonce.has_value()) {
    hasher.Ingest(*nonce);
  }
  if (code.has_value()) {
    hasher.Ingest(std::uint32_t(code->Size()));
    hasher.Ingest(*code);
  }
  for (const auto& cur : storage) {
    hasher.Ingest(cur.key);
    hasher.Ingest(cur.value);
  }
  return hasher.GetHash();
}

std::ostream& operator<<(std::ostream& out,
                         const AccountUpdate::SlotUpdate& update) {
  return out << update.key << ":" << update.value;
}

std::ostream& operator<<(std::ostream& out, const AccountUpdate& update) {
  std::cout << "Update(";
  if (update.created) {
    out << "Created";
  }
  if (update.deleted) {
    out << "Deleted";
  }
  if (update.balance) {
    out << ",Balance:" << *update.balance;
  }
  if (update.nonce) {
    out << ",Nonce:" << *update.nonce;
  }
  if (update.code) {
    out << ",code: <new_code>";
  }
  for (auto& cur : update.storage) {
    out << "," << cur;
  }
  return out << ")";
}

}  // namespace carmen
