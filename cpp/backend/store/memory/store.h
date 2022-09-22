#pragma once

#include "absl/container/flat_hash_map.h"

namespace carmen::backend::store {

// The InMemoryStore is a in-memory implementation of a mutable key/value
// store. It maps provided mutation and lookup support, as well as global
// state hashing support enabling to obtain a quick hash for the entire
// content.
template<typename K, typename V>
class InMemoryStore {
public:
    // Creates a new InMemoryStore using the provided value as the
    // default value for all its storage cells. Any get for an uninitialized
    // key will return the provided default value.
    InMemoryStore(V default_value = {}) 
        : _default_value(std::move(default_value)) {}

    // Updates the value associated to the given key.
    void Set(const K& key, V value) {
        _data[key] = value;
    }

    // Retrieves the value associated to the given key. If no values has
    // been previously set using a the Set(..) function above, the default
    // value defined during the construction of a store instance is returned.
    const V& Get(const K& key) {
        auto pos = _data.find(key);
        if (pos == _data.end()) {
            return _default_value; 
        }
        return pos->second;
    }

private:
    const K _default_value;
    absl::flat_hash_map<K,V> _data;
};

} // namespace carmen::backend::store