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

#include <ostream>

#include "absl/base/attributes.h"
#include "common/type.h"

namespace carmen::backend::btree {

// A struct with the distinct feature of being empty. It is called unit because
// there is exactly one value of this type. It is used to mark the absense of a
// value in the entry type below.
struct Unit {};

// An entry of a B-tree comprised of a key and an optional value. The key and
// value are packed tightly to avoid increasing memory and storage consumption
// due to padding.
template <Trivial Key, Trivial Value = Unit>
struct ABSL_ATTRIBUTE_PACKED Entry {
  Entry() = default;
  Entry(Key key) : key(key){};
  Entry(Key key, Value value) : key(key), value(value){};

  bool operator==(const Entry&) const = default;

  friend std::ostream& operator<<(std::ostream& out, const Entry& entry) {
    return out << entry.key << "->" << entry.value;
  }

  Key key;
  Value value;
};

// A specialization of an entry without a value. Removing the extra value field
// removes 1 byte of storage requirement for the empty value.
template <typename Key>
struct ABSL_ATTRIBUTE_PACKED Entry<Key, Unit> {
  Entry() = default;
  Entry(Key key) : key(key) {}

  bool operator==(const Entry&) const = default;

  friend std::ostream& operator<<(std::ostream& out, const Entry& entry) {
    return out << entry.key;
  }

  Key key;
};

}  // namespace carmen::backend::btree
