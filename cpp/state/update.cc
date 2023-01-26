#include "state/update.h"

#include <span>
#include <sstream>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen {

// TODO:
//  - implement cryptographic hashing

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

  std::size_t Size() const { return buffer_.size(); }

  std::vector<std::byte> Build() && { return std::move(buffer_); }

 private:
  std::vector<std::byte> buffer_;
};

}  // namespace

absl::StatusOr<Update> Update::FromBytes(std::span<const std::byte> data) {
  // The encoding should at least have the version number and the number of
  // entries.
  if (data.size() < 1 + 6 * 2) {
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

  ASSIGN_OR_RETURN(auto deleted_account_size, reader.ReadUint16());
  ASSIGN_OR_RETURN(auto created_account_size, reader.ReadUint16());
  ASSIGN_OR_RETURN(auto balances_size, reader.ReadUint16());
  ASSIGN_OR_RETURN(auto codes_size, reader.ReadUint16());
  ASSIGN_OR_RETURN(auto nonces_size, reader.ReadUint16());
  ASSIGN_OR_RETURN(auto storage_size, reader.ReadUint16());

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

absl::StatusOr<std::vector<std::byte>> Update::ToBytes() const {
  // Compute the total size of required buffer.
  std::size_t size = 1;  // the version number
  size += 6 * 2;         // 2 bytes for the length  of the respective list
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
  out.Append(std::uint16_t(deleted_accounts_.size()));
  out.Append(std::uint16_t(created_accounts_.size()));
  out.Append(std::uint16_t(balances_.size()));
  out.Append(std::uint16_t(codes_.size()));
  out.Append(std::uint16_t(nonces_.size()));
  out.Append(std::uint16_t(storage_.size()));

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

}  // namespace carmen
