// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include <cstdint>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "archive/archive.h"
#include "backend/structure.h"
#include "common/type.h"
#include "state/schema.h"
#include "state/update.h"

namespace carmen {

template <typename S>
concept State = requires(S s, const S c) {
  // All states must declare the Schema they are implementing.
  { S::GetSchema() } -> std::same_as<Schema>;

  // All states must be open-able through a static factory function.
  // The provided path points to the directory containing the data files to be
  // opened. If the files (or the directory) is missing, new files should be
  // initialized. The second boolean parameter decides whether an archive should
  // be included.
  {
    S::Open(std::declval<std::filesystem::path>(),
            /*with_archive=*/std::declval<bool>())
    } -> std::same_as<absl::StatusOr<S>>;

  // Obtains the current state of the given account.
  {
    c.GetAccountState(std::declval<Address>())
    } -> std::same_as<absl::StatusOr<AccountState>>;

  // Obtains the current balance of the given account.
  {
    c.GetBalance(std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Balance>>;

  // Obtains the current nonce of the given account.
  {
    c.GetNonce(std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Nonce>>;

  // Obtains the current value of the given storage slot.
  {
    c.GetStorageValue(std::declval<Address>(), std::declval<Key>())
    } -> std::same_as<absl::StatusOr<Value>>;

  // Obtains the current code of the given account.
  { c.GetCode(std::declval<Address>()) } -> std::same_as<absl::StatusOr<Code>>;

  // Obtains the size of the current code of the given account.
  {
    c.GetCodeSize(std::declval<Address>())
    } -> std::same_as<absl::StatusOr<std::uint32_t>>;

  // Obtains the hash of the current code of the given account.
  {
    c.GetCodeHash(std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Hash>>;

  // Applies the given block updates to this state.
  {
    s.Apply(std::declval<BlockId>(), std::declval<Update>())
    } -> std::same_as<absl::Status>;

  // Obtains a state hash providing a unique cryptographic fingerprint of the
  // entire maintained current state (does not include archive data).
  { s.GetHash() } -> std::same_as<absl::StatusOr<Hash>>;

  // Retrieves a pointer to the owned archive or nullptr, if no archive is
  // maintained by the status instance.
  { s.GetArchive() } -> std::same_as<typename S::Archive*>;

  // The provided archive satisfies the Archive concept.
  Archive<typename S::Archive>;

  // A state must have the basic properties of a structure, including Open,
  // Close, and Flush support.
  backend::Structure<S>;
};

}  // namespace carmen
