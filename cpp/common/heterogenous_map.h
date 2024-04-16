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

#pragma once

#include <memory>
#include <typeindex>
#include <typeinfo>

#include "absl/container/flat_hash_map.h"

namespace carmen {

// A HeterogenousMap is a map retaining values of various types, indexed by
// their types. Thus, for each type T a value of type T may be maintained, which
// can be retrieved and modified. It is mainly intended for environments
// depending on generic extensions following the open-closed principle.
class HeterogenousMap {
 public:
  // Sets the value maintained for type T.
  template <typename T>
  void Set(T value) {
    map_[typeid(T)] = std::make_unique<Entry<T>>(std::move(value));
  }

  // Obtains a reference to the value maintained for the given type. The
  // resulting reference is valid until the next reset of this type.
  template <typename T>
  T& Get() {
    auto pos = map_.find(typeid(T));
    if (pos != map_.end()) {
      return pos->second->Get<T>();
    }
    auto entry = std::make_unique<Entry<T>>();
    T& res = entry->value;
    map_[typeid(T)] = std::move(entry);
    return res;
  }

  // Obtains a reference to the value maintained for the given type. The
  // reference is only valid until the next modification of this type.
  template <typename T>
  const T& Get() const {
    static T zero;
    auto pos = map_.find(typeid(T));
    if (pos == map_.end()) {
      return zero;
    }
    return pos->second->Get<T>();
  }

  // Tests whether this map contains an explicit instance of a value of the
  // given type.
  template <typename T>
  bool Contains() const {
    return map_.find(typeid(T)) != map_.end();
  }

  // Resets the contained value to the default value of this type by destroying
  // the currently maintained instance. This invalidates any previously obtained
  // references to this type.
  template <typename T>
  void Reset() {
    map_.erase(typeid(T));
  }

 private:
  // A polymorphic base type for all entries. Its main purpose is to provide a
  // common base type for all stored elements and a virtual destructor for those
  // facilitating proper cleanup on destruction.
  struct EntryBase {
    virtual ~EntryBase() {}
    template <typename T>
    T& Get() {
      assert(dynamic_cast<Entry<T>*>(this));
      return static_cast<Entry<T>*>(this)->value;
    }
  };

  // A concrete entry type wrapping a single entry.
  template <typename T>
  struct Entry : public EntryBase {
    Entry() = default;
    Entry(T value) : value(std::move(value)) {}
    T value;
  };

  // The underlying data structure mapping types to entries. Entries are
  // referenced through their polymorphic base type.
  absl::flat_hash_map<std::type_index, std::unique_ptr<EntryBase>> map_;
};

}  // namespace carmen
