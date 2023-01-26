#include "state/c_state.h"

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <span>
#include <sstream>
#include <string_view>

#include "common/account_state.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "state/configurations.h"
#include "state/state.h"
#include "state/update.h"

namespace carmen {
namespace {

// An abstract interface definition of WorldState instances.
class WorldState {
 public:
  virtual ~WorldState() {}

  virtual absl::StatusOr<AccountState> GetAccountState(const Address&) = 0;

  virtual StatusOrRef<const Balance> GetBalance(const Address&) = 0;

  virtual StatusOrRef<const Nonce> GetNonce(const Address&) = 0;

  virtual StatusOrRef<const Value> GetValue(const Address&, const Key&) = 0;

  virtual absl::StatusOr<std::span<const std::byte>> GetCode(
      const Address&) = 0;
  virtual absl::StatusOr<std::uint32_t> GetCodeSize(const Address&) = 0;
  virtual absl::StatusOr<Hash> GetCodeHash(const Address&) = 0;

  virtual absl::Status Apply(std::uint64_t block, const Update&) = 0;

  virtual absl::StatusOr<Hash> GetHash() = 0;

  virtual MemoryFootprint GetMemoryFootprint() const = 0;

  virtual absl::Status Flush() = 0;
  virtual absl::Status Close() = 0;
};

// A generic implementation of the WorldState interface forwarding member
// function calls to an owned state instance. This class is the adapter between
// the static template based state implementations and the polymorph virtual
// WorldState interface.
template <typename State>
class WorldStateWrapper : public WorldState {
 public:
  WorldStateWrapper(State state) : state_(std::move(state)) {}

  absl::StatusOr<AccountState> GetAccountState(const Address& addr) override {
    return state_.GetAccountState(addr);
  }

  StatusOrRef<const Balance> GetBalance(const Address& address) override {
    return state_.GetBalance(address);
  }

  StatusOrRef<const Nonce> GetNonce(const Address& addr) override {
    return state_.GetNonce(addr);
  }

  StatusOrRef<const Value> GetValue(const Address& addr,
                                    const Key& key) override {
    return state_.GetStorageValue(addr, key);
  }

  absl::StatusOr<std::span<const std::byte>> GetCode(
      const Address& addr) override {
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

  absl::StatusOr<Hash> GetHash() override { return state_.GetHash(); }

  absl::Status Flush() override { return state_.Flush(); }

  absl::Status Close() override { return state_.Close(); }

  MemoryFootprint GetMemoryFootprint() const override {
    return state_.GetMemoryFootprint();
  }

 protected:
  State state_;
};

template <typename State>
WorldState* Open(const std::filesystem::path& directory) {
  auto state = State::Open(directory);
  if (!state.ok()) {
    std::cout << "WARNING: Failed to open state: " << state.status() << "\n";
    return nullptr;
  }
  return new WorldStateWrapper<State>(*std::move(state));
}

}  // namespace
}  // namespace carmen

extern "C" {

C_State Carmen_CreateInMemoryState() {
  return carmen::Open<carmen::InMemoryState>("");
}

C_State Carmen_CreateFileBasedState(const char* directory, int length) {
  return carmen::Open<carmen::FileBasedState>(
      std::string_view(directory, length));
}

C_State Carmen_CreateLevelDbBasedState(const char* directory, int length) {
  return carmen::Open<carmen::LevelDbBasedState>(
      std::string_view(directory, length));
}

void Carmen_Flush(C_State state) {
  auto res = reinterpret_cast<carmen::WorldState*>(state)->Flush();
  if (!res.ok()) {
    std::cout << "WARNING: Failed to flush state: " << res << "\n";
  }
}

void Carmen_Close(C_State state) {
  auto res = reinterpret_cast<carmen::WorldState*>(state)->Close();
  if (!res.ok()) {
    std::cout << "WARNING: Failed to close state: " << res << "\n";
  }
}

void Carmen_ReleaseState(C_State state) {
  delete reinterpret_cast<carmen::WorldState*>(state);
}

void Carmen_GetAccountState(C_State state, C_Address addr,
                            C_AccountState out_state) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& r = *reinterpret_cast<carmen::AccountState*>(out_state);
  auto res = s.GetAccountState(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get account state: " << res.status()
              << "\n";
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
    std::cout << "WARNING: Failed to get balance: " << res.status() << "\n";
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
    std::cout << "WARNING: Failed to get nonce: " << res.status() << "\n";
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
              << "\n";
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
    std::cout << "WARNING: Failed to get code: " << code.status() << "\n";
    return;
  }
  auto capacity = *out_length;
  *out_length = code->size();
  if (code->size() > capacity) {
    std::cout << "WARNING: Code buffer too small: " << code->size() << " > "
              << capacity << "\n";
    return;
  }
  memcpy(out_code, code->data(), code->size());
}

void Carmen_GetCodeHash(C_State state, C_Address addr, C_Hash out_hash) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto& h = *reinterpret_cast<carmen::Hash*>(out_hash);
  auto res = s.GetCodeHash(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get code hash: " << res.status() << "\n";
    return;
  }
  h = *res;
}

void Carmen_GetCodeSize(C_State state, C_Address addr, uint32_t* out_length) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& a = *reinterpret_cast<carmen::Address*>(addr);
  auto res = s.GetCodeSize(a);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get code size: " << res.status() << "\n";
    return;
  }
  *out_length = *res;
}

void Carmen_Apply(C_State state, uint64_t block, C_Update update,
                  uint32_t length) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  std::span<const std::byte> data(reinterpret_cast<const std::byte*>(update),
                                  length);
  auto change = carmen::Update::FromBytes(data);
  if (!change.ok()) {
    std::cout << "WARNING: Failed to decode update: " << change.status()
              << "\n";
    return;
  }
  auto res = s.Apply(block, *change);
  if (!res.ok()) {
    std::cout << "WARNING: Failed to apply update: " << res << "\n";
  }
}

void Carmen_GetHash(C_State state, C_Hash out_hash) {
  auto& s = *reinterpret_cast<carmen::WorldState*>(state);
  auto& h = *reinterpret_cast<carmen::Hash*>(out_hash);
  auto res = s.GetHash();
  if (!res.ok()) {
    std::cout << "WARNING: Failed to get hash: " << res.status() << "\n";
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
