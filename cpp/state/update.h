#pragma once

#include "absl/container/btree_map.h"
#include "common/type.h"

namespace carmen {

// A BlockUpdate summarizes all the updates produced by processing a block in
// the chain. It is the unit of data used to update archives and to synchronize
// data between archive instances.
// TODO:
//  - implement balance update support
//  - implement nonce update support
//  - implement account state update support
//  - implement cryptographic hashing of updates
//  - implement serialization and de-serialization of updates
class Update {
 public:
  // The identifier used for slots.
  struct SlotKey {
    Address account;
    Key slot;
    auto operator<=>(const SlotKey&) const = default;
  };

  // Adds the update of a storage slot to the changes to be covered by this
  // update.
  void Set(const Address& account, const Key& key, const Value& value);

  // Provides read access to the sorted map of storage updates maintained.
  const absl::btree_map<SlotKey, Value>& GetStorage() const {
    return storage_;
  };

 private:
  // Retains storage updates in sorted order. By sorting them, a normal form for
  // updates is defined, aiding the verification of updates.
  absl::btree_map<SlotKey, Value> storage_;
};

}
