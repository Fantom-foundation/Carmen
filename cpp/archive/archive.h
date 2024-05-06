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
#include <string_view>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "state/update.h"

namespace carmen {

// An archive retains a history of state mutations in a block chain on a
// block-level granularity. The history is recorded by adding per-block updates.
// All updates are append-only. History written once can no longer be altered.
//
// Archive Add(..) and GetXXX(..) operations are thread safe and may thus be run
// in parallel.
template <typename A>
concept Archive = requires(A a, const A b) {
  // Adds the changes of the given block to this archive.
  {
    a.Add(std::declval<BlockId>(), std::declval<Update>())
    } -> std::same_as<absl::Status>;

  // Allows to test whether an account exists at the given block height.
  {
    a.Exists(std::declval<BlockId>(), std::declval<Address>())
    } -> std::same_as<absl::StatusOr<bool>>;

  // Allows to fetch a historic balance values for a given account.
  {
    a.GetBalance(std::declval<BlockId>(), std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Balance>>;

  // Allows to fetch a historic code values for a given account.
  {
    a.GetCode(std::declval<BlockId>(), std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Code>>;

  // Allows to fetch a historic nonce values for a given account.
  {
    a.GetNonce(std::declval<BlockId>(), std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Nonce>>;

  // Allows to fetch a historic value for a given slot.
  {
    a.GetStorage(std::declval<BlockId>(), std::declval<Address>(),
                 std::declval<Key>())
    } -> std::same_as<absl::StatusOr<Value>>;

  // Computes a hash for the entire archive up until the given block.
  { a.GetHash(std::declval<BlockId>()) } -> std::same_as<absl::StatusOr<Hash>>;

  // Obtains a full list of addresses encountered up until the given block.
  {
    a.GetAccountList(std::declval<BlockId>())
    } -> std::same_as<absl::StatusOr<std::vector<Address>>>;

  // Obtains the last block included in this archive, 0 if empty.
  { a.GetLatestBlock() } -> std::same_as<absl::StatusOr<BlockId>>;

  // Obtains a hash on the content of the given hash at the given block height.
  {
    a.GetAccountHash(std::declval<BlockId>(), std::declval<Address>())
    } -> std::same_as<absl::StatusOr<Hash>>;

  // Verifies that the content of this archive up until the given block.
  {
    a.Verify(std::declval<BlockId>(), std::declval<Hash>(),
             std::declval<absl::FunctionRef<void(std::string_view)>>())
    } -> std::same_as<absl::Status>;

  // An archive must have the basic properties of a structure, including Open,
  // Close, and Flush support.
  backend::Structure<A>;
};

}  // namespace carmen
