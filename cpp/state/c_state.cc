/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "state/c_state.h"

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <span>
#include <sstream>
#include <string_view>

#include "archive/leveldb/archive.h"
#include "archive/sqlite/archive.h"
#include "common/account_state.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "state/configurations.h"
#include "state/s1/state.h"
#include "state/s2/state.h"
#include "state/s3/state.h"
#include "state/state.h"
#include "state/update.h"

namespace carmen {
namespace {

// An abstract interface definition of WorldState instances.
class WorldState {
 public:
  virtual ~WorldState() {}

  virtual absl::StatusOr<AccountState> GetAccountState(const Address&) = 0;

  virtual absl::StatusOr<Balance> GetBalance(const Address&) = 0;

  virtual absl::StatusOr<Nonce> GetNonce(const Address&) = 0;

  virtual absl::StatusOr<Value> GetValue(const Address&, const Key&) = 0;

  virtual absl::StatusOr<Code> GetCode(const Address&) = 0;
  virtual absl::StatusOr<std::uint32_t> GetCodeSize(const Address&) = 0;
  virtual absl::StatusOr<Hash> GetCodeHash(const Address&) = 0;

  virtual absl::Status Apply(std::uint64_t block, const Update&) = 0;

  virtual WorldState* GetArchiveState(std::uint64_t block) = 0;

  virtual absl::StatusOr<Hash> GetHash() = 0;

  virtual MemoryFootprint GetMemoryFootprint() const = 0;

  virtual absl::Status Flush() = 0;
  virtual absl::Status Close() = 0;
};

// A generic implementation of the WorldState interface forwarding member
// function calls to an owned state instance. This class is the adapter between
// the static template based state implementations and the polymorph virtual
// WorldState interface.
template <State State>
class WorldStateWrapper : public WorldState {
 public:
  WorldStateWrapper(State state) : state_(std::move(state)) {}

  absl::StatusOr<AccountState> GetAccountState(const Address& addr) override {
    return state_.GetAccountState(addr);
  }

  absl::StatusOr<Balance> GetBalance(const Address& address) override {
    return state_.GetBalance(address);
  }

  absl::StatusOr<Nonce> GetNonce(const Address& addr) override {
    return state_.GetNonce(addr);
  }

  absl::StatusOr<Value> GetValue(const Address& addr, const Key& key) override {
    return state_.GetStorageValue(addr, key);
  }

  absl::StatusOr<Code> GetCode(const Address& addr) override {
    return state_.GetCode(addr);
  }

  absl::StatusOr<std::uint32_t> GetCodeSize(const Address& addr) override {
    return state_.GetCodeSize(addr);
  }

  absl::StatusOr<Hash> GetCodeHash(const Address& addr) override {
    return state_.GetCodeHash(addr);
  }

  absl::Status Apply(std::uint64_t block, const Update& update) override {
    return state_.Apply(block, update);
  }

  WorldState* GetArchiveState(std::uint64_t block) override {
    auto archive = state_.GetArchive();
    if (archive == nullptr) return nullptr;
    return new ArchiveState(*archive, block);
  }

  absl::StatusOr<Hash> GetHash() override { return state_.GetHash(); }

  absl::Status Flush() override { return state_.Flush(); }

  absl::Status Close() override { return state_.Close(); }

  MemoryFootprint GetMemoryFootprint() const override {
    return state_.GetMemoryFootprint();
  }

 protected:
  State state_;

 private:
  using Archive = typename State::Archive;

  class ArchiveState : public WorldState {
   public:
    ArchiveState(Archive& archive, BlockId block)
        : archive_(archive), block_(block) {}

    absl::StatusOr<AccountState> GetAccountState(const Address& addr) override {
      ASSIGN_OR_RETURN(bool exists, archive_.Exists(block_, addr));
      return exists ? AccountState::kExists : AccountState::kUnknown;
    }

    absl::StatusOr<Balance> GetBalance(const Address& address) override {
      return archive_.GetBalance(block_, address);
    }

    absl::StatusOr<Nonce> GetNonce(const Address& addr) override {
      return archive_.GetNonce(block_, addr);
    }

    absl::StatusOr<Value> GetValue(const Address& addr,
                                   const Key& key) override {
      return archive_.GetStorage(block_, addr, key);
    }

    absl::StatusOr<Code> GetCode(const Address& addr) override {
      return archive_.GetCode(block_, addr);
    }

    absl::StatusOr<std::uint32_t> GetCodeSize(const Address& addr) override {
      ASSIGN_OR_RETURN(auto code, GetCode(addr));
      return code.Size();
    }

    absl::StatusOr<Hash> GetCodeHash(const Address& addr) override {
      ASSIGN_OR_RETURN(auto code, GetCode(addr));
      return GetKeccak256Hash(code);
    }

    absl::Status Apply(std::uint64_t, const Update&) override {
      return absl::InvalidArgumentError("Cannot apply update on archive");
    }

    WorldState* GetArchiveState(std::uint64_t block) override {
      return new ArchiveState(archive_, block);
    }

    absl::StatusOr<Hash> GetHash() override { return archive_.GetHash(block_); }

    absl::Status Flush() override { return absl::OkStatus(); }

    absl::Status Close() override { return absl::OkStatus(); }

    MemoryFootprint GetMemoryFootprint() const override {
      return MemoryFootprint(*this);
    }

   private:
    Archive& archive_;
    BlockId block_;
  };
};

template <typename State>
WorldState* OpenState(const std::filesystem::path& directory,
                      bool with_archive) {
  auto state = State::Open(directory, with_archive);
  if (!state.ok()) {
    std::cout << "WARNING: Failed to open state: " << state.status() << "\n"
              << std::flush;
    return nullptr;
  }
  return new WorldStateWrapper<State>(*std::move(state));
}

template <template <class A> class Config, template <typename C> class State>
WorldState* Open(const std::filesystem::path& directory, ArchiveImpl archive) {
  switch (archive) {
    case kArchive_None:
      // We have no none-archive implementation, so we take the LevelDB one and
      // disable it.
      return OpenState<State<Config<archive::leveldb::LevelDbArchive>>>(
          directory, false);
    case kArchive_LevelDb:
      return OpenState<State<Config<archive::leveldb::LevelDbArchive>>>(
          directory, true);
    case kArchive_Sqlite:
      return OpenState<State<Config<archive::sqlite::SqliteArchive>>>(directory,
                                                                      true);
  }
  return nullptr;
}

template <template <class A> class Config>
WorldState* Open(const std::filesystem::path& directory, std::uint8_t schema,
                 ArchiveImpl archive) {
  switch (schema) {
    case 0:  // default option is schema 1, fall-through
    case 1:
      return Open<Config, s1::State>(directory, archive);
    case 2:
      return Open<Config, s2::State>(directory, archive);
    case 3:
      return Open<Config, s3::State>(directory, archive);
  }
  return nullptr;
}

}  // namespace
}  // namespace carmen

extern "C" {

C_State Carmen_OpenState(C_Schema schema, StateImpl state, ArchiveImpl archive,
                         const char* directory, int length) {
  std::string_view dir(directory, length);
  switch (state) {
    case kState_Memory:
      return carmen::Open<carmen::InMemoryConfig>(dir, schema, archive);
    case kState_File:
      return carmen::Open<carmen::FileBasedConfig>(dir, schema, archive);
    case kState_LevelDb:
      return carmen::Open<carmen::LevelDbBasedConfig>(dir, schema, archive);
  }
  return nullptr;
}

void Carmen_Flush(C_State state) {
  auto res = reinterpret_cast<carmen::WorldState*>(state)->Flush();
  if (!res.ok()) {
    std::cout << "WARNING: Failed to flush state: " << res << "\n"
              << std::flush;
  }
}

void Carmen_Close(C_State state) {
  auto res = reinterpret_cast<carmen::WorldState*>(state)->Close();
  if (!res.ok()) {
    std::cout << "WARNING: Failed to close state: " << res << "\n"
              << std::flush;
  }
}

void Carmen_ReleaseState(C_State state) {
  delete reinterpret_cast<carmen::WorldState*>(state);
}

C_State Carmen_GetArchiveState(C_State state, uint64_t block) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  return s.GetArchiveState(block);
}

void Carmen_GetAccountState(C_State state, C_Address addr,
                            C_AccountState out_state) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& r = *reinterpret_cast<carmen::AccountState*>(out_state);
  auto res = s.GetAccountState(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get account state: " << res.status()
              << "\n"
              << std::flush;
    return;
  }
  r = *res;
}

void Carmen_GetBalance(C_State state, C_Address addr, C_Balance out_balance) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& b = *reinterpret_cast<carmen::Balance*>(out_balance);
  auto res = s.GetBalance(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get balance: " << res.status() << "\n"
              << std::flush;
    return;
  }
  b = *res;
}

void Carmen_GetNonce(C_State state, C_Address addr, C_Nonce out_nonce) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& n = *reinterpret_cast<carmen::Nonce*>(out_nonce);
  auto res = s.GetNonce(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get nonce: " << res.status() << "\n"
              << std::flush;
    return;
  }
  n = *res;
}

void Carmen_GetStorageValue(C_State state, C_Address addr, C_Key key,
                            C_Value out_value) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& k = *reinterpret_cast<carmen::Key*>(key);
  auto& v = *reinterpret_cast<carmen::Value*>(out_value);
  auto res = s.GetValue(a, k);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get storage value: " << res.status()
              << "\n"
              << std::flush;
    return;
  }
  v = *res;
}

void Carmen_GetCode(C_State state, C_Address addr, C_Code out_code,
                    uint32_t* out_length) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto code = s.GetCode(a);
  if (!code.ok()) {
    std::cout << "WARNING: Failed to get code: " << code.status() << "\n"
              << std::flush;
    return;
  }
  auto capacity = *out_length;
  *out_length = code->Size();
  if (code->Size() > capacity) {
    std::cout << "WARNING: Code buffer too small: " << code->Size() << " > "
              << capacity << "\n"
              << std::flush;
    return;
  }
  memcpy(out_code, code->Data(), code->Size());
}

void Carmen_GetCodeHash(C_State state, C_Address addr, C_Hash out_hash) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& h = *reinterpret_cast<carmen::Hash*>(out_hash);
  auto res = s.GetCodeHash(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get code hash: " << res.status() << "\n"
              << std::flush;
    return;
  }
  h = *res;
}

void Carmen_GetCodeSize(C_State state, C_Address addr, uint32_t* out_length) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto res = s.GetCodeSize(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get code size: " << res.status() << "\n"
              << std::flush;
    return;
  }
  *out_length = *res;
}

void Carmen_Apply(C_State state, uint64_t block, C_Update update,
                  uint64_t length) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  std::span<const std::byte> data(reinterpret_cast<const std::byte*>(update),
                                  length);
  auto change = carmen::Update::FromBytes(data);
  if (!change.ok()) {
    std::cout << "WARNING: Failed to decode update: " << change.status() << "\n"
              << std::flush;

    return;
  }
  auto res = s.Apply(block, *std::move(change));
  if (!res.ok()) {
    std::cout << "WARNING: Failed to apply update: " << res << "\n"
              << std::flush;
  }
}

void Carmen_GetHash(C_State state, C_Hash out_hash) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& h = *reinterpret_cast<carmen::Hash*>(out_hash);
  auto res = s.GetHash();
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get hash: " << res.status() << "\n"
              << std::flush;
    return;
  }
  h = *res;
}

void Carmen_GetMemoryFootprint(C_State state, char** out,
                               uint64_t* out_length) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto fp = s.GetMemoryFootprint();
  std::stringstream buffer;
  fp.WriteTo(buffer);
  auto data = std::move(buffer).str();
  *out_length = data.size();
  *out = reinterpret_cast<char*>(malloc(data.size()));
  std::memcpy(*out, data.data(), data.size());
}

}  // extern "C"
