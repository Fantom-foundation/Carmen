#pragma once

#include <array>
#include <bit>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <deque>
#include <iostream>
#include <optional>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::s4 {

class NodeId {
 public:
  NodeId() : id_(0){};

  constexpr static NodeId Empty() { return NodeId(0); };
  constexpr static NodeId Leaf(std::uint32_t id) { return NodeId(id + 1); };
  constexpr static NodeId Branch(std::uint32_t id) {
    return NodeId(0x80000000 | id);
  };
  constexpr static NodeId Extension(std::uint32_t id) {
    return NodeId(0xC0000000 | id);
  };

  std::uint32_t GetIndex() const {
    return IsLeaf() ? (id_ & 0x7FFFFFFF) - 1 : id_ & 0x3FFFFFFF;
  }

  bool IsEmpty() const { return id_ == 0; }

  bool IsLeaf() const { return !IsEmpty() && id_ >> 31 == 0; }

  bool IsBranch() const { return id_ >> 30 == 2; }

  bool IsExtension() const { return id_ >> 30 == 3; }

  bool operator==(const NodeId&) const = default;

 private:
  constexpr NodeId(std::uint32_t id) : id_(id) {}

  // If zero, it is the id of the empty node. If it starts with a 0 bit,
  // the remaining 31 bits are the ID of a leaf node. If it starts with
  // 10, the remaining 30 bits are the ID of a branch node, and if it starts
  // with 11, the remaining 30 bits are the ID of a extension node.
  std::uint32_t id_;
};

class Nibble {
 public:
  Nibble(std::uint8_t value) : value_(value & 0xF) {}

  std::uint8_t ToUint() const { return value_; }

  auto operator<=>(const Nibble&) const = default;

  friend std::ostream& operator<<(std::ostream& out, Nibble nibble) {
    if (nibble.value_ < 10) {
      out << char('0' + nibble.value_);
    } else if (nibble.value_ < 16) {
      out << char('a' + nibble.value_ - 10);
    } else {
      out << '?';
    }
    return out;
  }

 private:
  std::uint8_t value_;
};

template <std::size_t path_length>
class PathSegment {
  static_assert(path_length <= 256 * 256);

 public:
  PathSegment() : length_(0) {}

  PathSegment(Nibble nibble)
      : length_(4), path_(CreateSingleNibblePath(nibble)) {}

  PathSegment(std::initializer_list<std::size_t> nibbles)
      : length_(nibbles.size() * 4) {
    for (std::size_t cur : nibbles) {
      path_ <<= 4;
      path_ |= CreateSingleNibblePath(cur);
    }
  }

  PathSegment(std::uint16_t length, const std::bitset<path_length>& path)
      : length_(length), path_(path & (kAllOne >> (path_length - length))) {}

  std::uint16_t GetLength() const { return length_; }

  const std::bitset<path_length>& GetPath() const { return path_; }

  std::uint8_t GetHead() const { return GetNibble(0); }

  PathSegment GetTail() const { return PathSegment(length_ - 4, path_); }

  std::uint8_t GetNibble(int i) const {
    if (i >= length_ / 4) {
      return 0;
    }
    return ((path_ >> (length_ - 4 * i - 4)) & std::bitset<path_length>(0xF))
        .to_ulong();
  }

  void Prepend(std::uint8_t nibble) {
    path_ |= CreateSingleNibblePath(nibble) << length_;
    length_ += 4;
  }

  void Prepend(const PathSegment& prefix) {
    path_ |= prefix.path_ << length_;
    length_ += prefix.length_;
  }

  void RemovePrefix(std::uint16_t prefix_length) {
    *this = PathSegment(length_ - prefix_length, path_);
  }

  bool IsPrefixOf(const PathSegment& other) const {
    if (length_ > other.GetLength()) {
      return false;
    }
    return other.path_ >> (other.length_ - length_) == path_;
  }

  bool operator==(const PathSegment&) const = default;

  PathSegment operator>>(std::uint16_t size) const {
    PathSegment res;
    res.length_ = length_ - size;
    res.path_ = path_ >> size;
    return res;
  }

  friend std::ostream& operator<<(std::ostream& out, const PathSegment& path) {
    for (int i = path_length; i > 0; i -= 4) {
      out << Nibble(
          ((path.path_ >> (i - 4)) & std::bitset<path_length>(0xF)).to_ulong());
    }
    return out << " : " << int(path.length_);
  }

 private:
  static const std::bitset<path_length> kAllOne;

  static std::bitset<path_length> CreateSingleNibblePath(Nibble nibble) {
    return std::bitset<path_length>(nibble.ToUint());
  }

  // The length of the segment in bits.
  std::uint16_t length_;
  // The path segment, padded with zeros.
  std::bitset<path_length> path_;
};

template <std::size_t path_length>
const std::bitset<path_length> PathSegment<path_length>::kAllOne =
    std::bitset<path_length>().flip();

template <std::size_t path_length>
PathSegment<path_length> GetCommonPrefix(const PathSegment<path_length>& a,
                                         const PathSegment<path_length>& b) {
  if (a.GetLength() > b.GetLength()) {
    return GetCommonPrefix(b, a);
  }

  for (int i = 0; i < a.GetLength() / 4; i++) {
    if (a.GetNibble(i) != b.GetNibble(i)) {
      return a >> (a.GetLength() - i * 4);
    }
  }
  return a;
}

struct Node {};

struct Branch : public Node {
  Branch() { children.fill(NodeId::Empty()); }
  std::array<NodeId, 16> children;
};

template <std::size_t path_length>
struct Extension : public Node {
  PathSegment<path_length> path;
  NodeId next;
};

template <std::size_t path_length, typename V>
struct Leaf : public Node {
  PathSegment<path_length> path;
  V value;
};

template <typename Node>
class NodeContainer {
 public:
  std::pair<std::uint32_t, std::reference_wrapper<Node>> NewNode() {
    if (free_ids_.empty()) {
      std::uint32_t res = nodes_.size();
      nodes_.push_back(Node{});
      hashes_.push_back(Hash{});
      return {res, nodes_[res]};
    }
    std::uint32_t res = free_ids_.back();
    free_ids_.pop_back();
    nodes_[res] = Node{};
    return {res, nodes_[res]};
  }

  void ReleaseNode(std::uint32_t pos) { free_ids_.push_back(pos); }

  const Node& Get(std::uint32_t pos) const {
    if (hashes_.size() <= pos) {
      assert(false && "invalid index");
    }
    return nodes_[pos];
  }

  Node& Get(std::uint32_t pos) {
    if (hashes_.size() <= pos) {
      assert(false && "invalid index");
    }
    return nodes_[pos];
  }

  const Hash& GetHash(std::uint32_t pos) const {
    if (hashes_.size() <= pos) {
      return kZeroHash;
    }
    return hashes_[pos];
  }

  void SetHash(std::uint32_t pos, const Hash& hash) {
    if (hashes_.size() <= pos) {
      return;
    }
    hashes_[pos] = hash;
  }

  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("nodes", SizeOf(nodes_));
    res.Add("hashes", SizeOf(hashes_));
    res.Add("free_ids", SizeOf(free_ids_));
    return res;
  }

 private:
  constexpr static Hash kZeroHash{};
  std::deque<Node> nodes_;
  std::deque<Hash> hashes_;
  std::deque<std::uint32_t> free_ids_;
};

template <std::size_t path_length>
class PathIterator {
 public:
  PathIterator(std::bitset<path_length> key) : key_(key), pos_(0) {}

  PathSegment<path_length> GetRemaining() const {
    return PathSegment(path_length - pos_, key_);
  }

  Nibble Next() {
    std::uint8_t res =
        ((key_ >> (path_length - pos_ - 4)) & std::bitset<path_length>(0xF))
            .to_ulong();
    pos_ += 4;
    return Nibble(res);
  }

  void Skip(std::uint16_t bits) { pos_ += bits; }

 private:
  std::bitset<path_length> key_;
  std::uint16_t pos_;
};

template <typename K, typename V>
class MerklePatriciaTrieForrest {
 public:
  bool Set(NodeId& root, const K& key, const V& value) {
    if (value == V{}) {
      return Remove(root, key);
    }

    PathIterator iter(ToBitset(key));
    NodeId* cur = &root;
    for (;;) {
      if (cur->IsEmpty()) {
        auto [newId, leaf] = leafs_.NewNode();
        *cur = NodeId::Leaf(newId);
        leaf.get().path = iter.GetRemaining();
        leaf.get().value = value;
        return true;
      } else if (cur->IsLeaf()) {
        // If the leaf is the value to be updated, do so.
        const NodeId leafId = *cur;
        auto& leaf = leafs_.Get(cur->GetIndex());
        auto remaining = iter.GetRemaining();
        if (leaf.path == remaining) {
          if (leaf.value != value) {
            leaf.value = value;
            return true;
          }
          return false;
        }

        // Check whether an extension node can be inserted.
        auto common = GetCommonPrefix(remaining, leaf.path);
        if (common.GetLength() > 0) {
          // Introduce an extension node.
          auto [newId, extension] = extensions_.NewNode();
          extension.get().path = common;
          *cur = NodeId::Extension(newId);
          cur = &extension.get().next;

          leaf.path.RemovePrefix(common.GetLength());
          iter.Skip(common.GetLength());
        }

        // Add the split at the end of the extension node.
        auto [newId, branch] = branches_.NewNode();
        branch.get().children[leaf.path.GetHead()] = leafId;
        leaf.path = leaf.path.GetTail();
        *cur = NodeId::Branch(newId);
      } else if (cur->IsBranch()) {
        Branch& branch = branches_.Get(cur->GetIndex());
        cur = &branch.children[iter.Next().ToUint()];
      } else if (cur->IsExtension()) {
        const auto extensionId = *cur;
        auto& extension = extensions_.Get(cur->GetIndex());
        // Check whether the extension is a prefix of the new value.
        auto remaining = iter.GetRemaining();
        if (extension.path.IsPrefixOf(remaining)) {
          cur = &extension.next;
          iter.Skip(extension.path.GetLength());
        } else {
          auto common = GetCommonPrefix(extension.path, remaining);
          if (common.GetLength() > 0) {
            auto [newId, prefix] = extensions_.NewNode();
            prefix.get().path = common;
            *cur = NodeId::Extension(newId);
            cur = &prefix.get().next;

            remaining.RemovePrefix(common.GetLength());
            extension.path.RemovePrefix(common.GetLength());
            iter.Skip(common.GetLength());
          }

          // Shorten old extension and discard it if no longer needed.
          auto nextId = extensionId;
          auto childPosition = extension.path.GetHead();
          if (extension.path.GetLength() == 4) {
            nextId = extension.next;
            extensions_.ReleaseNode(extensionId.GetIndex());
          } else {
            extension.path.RemovePrefix(4);
          }

          auto [newId, branch] = branches_.NewNode();
          *cur = NodeId::Branch(newId);
          branch.get().children[childPosition] = nextId;
          cur = &branch.get().children[iter.Next().ToUint()];
        }

      } else {
        assert(false && "Unsupported node type");
      }
    }
  }

  V Get(NodeId root, const K& key) const {
    auto [_, ptr] = GetInternal(root, key);
    return ptr == nullptr ? V{} : *ptr;
  }

  int GetDepth(NodeId root, const K& key) const {
    auto [count, _] = GetInternal(root, key);
    return count;
  }

  void RemoveTree(NodeId root) {
    if (root.IsEmpty()) {
      // Nothing to do.
    } else if (root.IsLeaf()) {
      leafs_.ReleaseNode(root.GetIndex());
    } else if (root.IsBranch()) {
      auto& branch = branches_.Get(root.GetIndex());
      for (auto child : branch.children) {
        RemoveTree(child);
      }
      branches_.ReleaseNode(root.GetIndex());
    } else if (root.IsExtension()) {
      auto& extension = extensions_.Get(root.GetIndex());
      RemoveTree(extension.next);
      extensions_.ReleaseNode(root.GetIndex());
    } else {
      assert(false && "Unsupported node type");
    }
  }

  void Dump(NodeId root) const {
    Dump(root, "");
    std::cout << std::endl;
  }

  absl::Status Check(NodeId root) const { return Check(root, 0); }

  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("branches", branches_.GetMemoryFootprint());
    res.Add("leafs", leafs_.GetMemoryFootprint());
    res.Add("extensions", extensions_.GetMemoryFootprint());
    return res;
  }

 private:
  std::pair<int, const V*> GetInternal(NodeId root, const K& key) const {
    PathIterator iter(ToBitset(key));
    NodeId cur = root;
    int count = 1;
    for (;; count++) {
      if (cur.IsEmpty()) {
        return {count - 1, nullptr};
      } else if (cur.IsLeaf()) {
        const auto& leaf = leafs_.Get(cur.GetIndex());
        if (leaf.path == iter.GetRemaining()) {
          return {count, &leaf.value};
        }
        return {count, nullptr};
      } else if (cur.IsBranch()) {
        const Branch& branch = branches_.Get(cur.GetIndex());
        cur = branch.children[iter.Next().ToUint()];
      } else if (cur.IsExtension()) {
        const auto& extension = extensions_.Get(cur.GetIndex());
        if (!extension.path.IsPrefixOf(iter.GetRemaining())) {
          return {count, nullptr};
        }
        cur = extension.next;
        iter.Skip(extension.path.GetLength());
      } else {
        assert(false && "Unsupported node type");
      }
    }
    return {count, nullptr};
  }

  bool Remove(NodeId& root, const K& key) {
    PathIterator iter(ToBitset(key));
    return Remove(&root, iter);
  }

  bool Remove(NodeId* cur, PathIterator<sizeof(K) * 8>& iter) {
    if (cur->IsEmpty()) {
      // nothing to do
      return false;
    } else if (cur->IsLeaf()) {
      // If the leaf is the value to be updated, do so.
      auto& leaf = leafs_.Get(cur->GetIndex());
      auto remaining = iter.GetRemaining();
      if (leaf.path != remaining) {
        return false;  // Not the target, nothing to do.
      }

      // This leaf needs to be removed from the tree.
      leafs_.ReleaseNode(cur->GetIndex());
      *cur = NodeId::Empty();
      return true;

    } else if (cur->IsBranch()) {
      Branch& branch = branches_.Get(cur->GetIndex());
      NodeId& next = branch.children[iter.Next().ToUint()];
      bool changed = Remove(&next, iter);

      if (!changed || !next.IsEmpty()) {
        return changed;
      }

      // Check whether there are at least 2 remaining non-empty children.
      std::optional<std::size_t> childPosition = std::nullopt;
      for (std::size_t i = 0; i < branch.children.size(); i++) {
        if (!branch.children[i].IsEmpty()) {
          if (childPosition.has_value()) {
            return true;
          }
          childPosition = i;
        }
      }

      auto branchId = *cur;

      // If the child node is a leaf, replace the branch node with an extended
      // leaf node.
      NodeId& childId = branch.children[*childPosition];
      if (childId.IsLeaf()) {
        auto& leaf = leafs_.Get(childId.GetIndex());
        leaf.path.Prepend(*childPosition);
        *cur = childId;

      } else if (childId.IsExtension()) {
        auto& extension = extensions_.Get(childId.GetIndex());
        extension.path.Prepend(*childPosition);
        *cur = childId;

      } else {
        // Replace the branch node by a new extension node.
        auto [newId, extension] = extensions_.NewNode();
        *cur = NodeId::Extension(newId);
        extension.get().next = branch.children[*childPosition];
        extension.get().path = PathSegment<sizeof(K) * 8>(*childPosition);
      }

      branches_.ReleaseNode(branchId.GetIndex());
      return true;

    } else if (cur->IsExtension()) {
      auto& extension = extensions_.Get(cur->GetIndex());
      auto remaining = iter.GetRemaining();
      if (!extension.path.IsPrefixOf(remaining)) {
        return false;
      }

      iter.Skip(extension.path.GetLength());

      NodeId& next = extension.next;
      bool changed = Remove(&next, iter);

      if (next.IsBranch()) {
        return changed;
      }
      auto extensionId = *cur;
      if (next.IsLeaf()) {
        auto& leaf = leafs_.Get(next.GetIndex());
        leaf.path.Prepend(extension.path);
        *cur = next;

      } else if (next.IsExtension()) {
        auto& child = extensions_.Get(next.GetIndex());
        child.path.Prepend(extension.path);
        *cur = next;

      } else {
        assert(false &&
               "Unexpected next node of extension node after element deletion");
      }
      extensions_.ReleaseNode(extensionId.GetIndex());
      return true;
    } else {
      assert(false && "Unsupported node type");
    }
    return false;
  }

  void Dump(NodeId cur, std::string prefix) const {
    if (cur.IsEmpty()) {
      std::cout << prefix << "-empty-\n";
    } else if (cur.IsLeaf()) {
      const auto& leaf = leafs_.Get(cur.GetIndex());
      std::cout << prefix << "Leaf(" << cur.GetIndex() << ") - " << leaf.path
                << " => " << leaf.value << "\n";
    } else if (cur.IsBranch()) {
      const Branch& branch = branches_.Get(cur.GetIndex());
      std::cout << prefix << "Branch: " << cur.GetIndex() << "\n";
      for (int i = 0; i < 16; i++) {
        if (!branch.children[i].IsEmpty()) {
          std::stringstream buffer;
          buffer << prefix << "  " << Nibble(i) << " ";
          Dump(branch.children[i], buffer.str());
        }
      }
    } else if (cur.IsExtension()) {
      const auto& extension = extensions_.Get(cur.GetIndex());
      std::cout << prefix << "Extension(" << cur.GetIndex() << ") - "
                << extension.path << "\n";
      Dump(extension.next, prefix + "    ");
    } else {
      assert(false && "Unsupported node type");
    }
  }

  absl::Status Check(NodeId cur, int depth) const {
    // Invariants checked:
    //  - branches have 2+ children
    //  - extensions have length >= 1
    //  - extensions are not followed by extensions
    //  - all paths have the same length
    //  - leafs do not contain the default value

    if (cur.IsEmpty()) {
      return absl::OkStatus();
    }
    if (cur.IsLeaf()) {
      const auto& leaf = leafs_.Get(cur.GetIndex());
      if (depth + leaf.path.GetLength() != sizeof(K) * 8) {
        return absl::InternalError(absl::StrFormat(
            "Invalid leaf depth: %d", depth + leaf.path.GetLength()));
      }
      if (leaf.value == V{}) {
        return absl::InternalError(
            absl::StrFormat("Invalid leaf value: value is default value"));
      }
      return absl::OkStatus();
    }
    if (cur.IsBranch()) {
      const auto& branch = branches_.Get(cur.GetIndex());

      int non_empty_count = 0;
      for (auto id : branch.children) {
        if (!id.IsEmpty()) {
          RETURN_IF_ERROR(Check(id, depth + 4));
          non_empty_count++;
        }
      }
      if (non_empty_count < 2) {
        return absl::InternalError(
            absl::StrFormat("Invalid branch node: only %d non-empty children",
                            non_empty_count));
      }
      return absl::OkStatus();
    }

    if (cur.IsExtension()) {
      const auto& extension = extensions_.Get(cur.GetIndex());
      if (extension.path.GetLength() < 4) {
        return absl::InternalError(
            absl::StrFormat("Invalid extension node: path length %d",
                            extension.path.GetLength()));
      }

      if (!extension.next.IsBranch()) {
        return absl::InternalError(absl::StrFormat(
            "Invalid extension node: extension not followed by a branch"));
      }

      return Check(extension.next, depth + extension.path.GetLength());
    }

    return absl::InternalError("Invalid node ID encountered");
  }

  static std::bitset<sizeof(K) * 8> ToBitset(const K& key) {
    // TODO: find a faster way to do this!
    std::bitset<sizeof(K) * 8> res;
    auto bytes = std::as_bytes(std::span<const K, 1>(&key, 1));
    for (auto byte : bytes) {
      res <<= 8;
      res |= std::bitset<sizeof(K) * 8>(int(byte));
    }
    return res;
  }

  NodeContainer<Branch> branches_;
  NodeContainer<Extension<sizeof(K) * 8>> extensions_;
  NodeContainer<Leaf<sizeof(K) * 8, V>> leafs_;
};

template <typename K, typename V>
class MerklePatriciaTrie {
 public:
  bool Set(const K& key, const V& value) { return forrest_.Set(root_, key, value); }

  V Get(const K& key) const { return forrest_.Get(root_, key); }

  int GetDepth(const K& key) const { return forrest_.GetDepth(root_, key); }

  void Dump() const { forrest_.Dump(root_); }

  absl::Status Check() const { return forrest_.Check(root_); }

  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("trie", forrest_.GetMemoryFootprint());
    return res;
  }

 private:
  MerklePatriciaTrieForrest<K, V> forrest_;
  NodeId root_ = NodeId::Empty();
};

}  // namespace carmen::s4
