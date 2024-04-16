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

#include "common/memory_usage.h"

#include <cmath>
#include <iomanip>
#include <istream>
#include <ostream>
#include <string_view>

#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "common/status_util.h"

namespace carmen {

Memory& Memory::operator+=(const Memory& a) {
  bytes_ += a.bytes_;
  return *this;
}

Memory& Memory::operator-=(const Memory& a) {
  bytes_ -= a.bytes_;
  return *this;
}

Memory& Memory::operator*=(int factor) {
  bytes_ *= factor;
  return *this;
}

std::ostream& operator<<(std::ostream& out, const Memory& memory) {
  const std::string_view prefixes = " KMGTPE";
  const auto base = 1024;

  std::int64_t value = memory.bytes();
  if (value < 0) {
    out << "-";
    value = -value;
  }

  if (value == 0) {
    return out << "0 byte";
  }

  int exp =
      std::min<int>(std::log(value) / std::log(base), prefixes.size() - 1);
  if (exp == 0) {
    return out << value << " byte";
  }

  return out << absl::StrFormat("%.1f %ciB", value / std::pow(base, exp),
                                prefixes[exp]);
}

Memory MemoryFootprint::GetTotal() const {
  Memory sum;
  absl::flat_hash_set<ObjectId> seen;
  std::vector<const MemoryFootprint*> stack;
  stack.push_back(this);
  while (!stack.empty()) {
    const MemoryFootprint& cur = *stack.back();
    stack.pop_back();
    sum += cur.self_;
    // Enqueue sub-components.
    for (const auto& [_, component] : cur.components_) {
      // Skip nodes already visited.
      if (component.source_ != kUnique &&
          !seen.insert(component.source_).second) {
        continue;
      }
      stack.push_back(&component);
    }
  }
  return sum;
}

MemoryFootprint& MemoryFootprint::Add(std::string_view label,
                                      MemoryFootprint footprint) {
  components_.insert_or_assign(label, std::move(footprint));
  return *this;
}

void MemoryFootprint::PrintTo(std::ostream& out, std::string_view path) const {
  // For improved readability and comparability labels are sorted.
  std::vector<std::string_view> labels;
  for (const auto& [label, component] : components_) {
    labels.push_back(label);
  }
  std::sort(labels.begin(), labels.end());

  for (const auto& label : labels) {
    const auto& component = components_.find(label)->second;
    component.PrintTo(out,
                      absl::StrCat(std::string(path), "/", std::string(label)));
  }
  std::stringstream buffer;
  buffer << GetTotal();
  out << absl::StrFormat("%9s\t%s\n", buffer.str(), path);
}

namespace {

template <typename T>
void Write(std::ostream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
absl::StatusOr<T> Read(std::istream& in) {
  T value;
  if (!in.read(reinterpret_cast<char*>(&value), sizeof(T))) {
    return absl::InvalidArgumentError("Failed to read from stream.");
  }
  return value;
}

void WriteStr(std::ostream& out, std::string_view str) {
  uint32_t size = str.size();
  Write(out, size);
  out.write(str.data(), size);
}

absl::StatusOr<std::string> ReadStr(std::istream& in) {
  std::string res;
  ASSIGN_OR_RETURN(auto size, Read<uint32_t>(in));
  res.resize(size);
  if (!in.read(res.data(), size)) {
    return absl::InvalidArgumentError("Failed to string from stream.");
  }
  return res;
}

}  // namespace

std::ostream& MemoryFootprint::WriteTo(std::ostream& out) const {
  //  Serialization format:
  //   - 16 byte source (8 byte location, 8 byte type)
  //   - 8 byte self memory usage
  //   - 4 byte number of components
  //   - list of components with (<label>,<component>)* format
  //      - label is stored as <length>,<characters>*

  static_assert(sizeof(source_) == 16);
  Write(out, source_);
  static_assert(sizeof(self_) == 8);
  Write(out, self_);

  uint32_t num_components = components_.size();
  Write(out, num_components);

  for (const auto& [label, component] : components_) {
    WriteStr(out, label);
    component.WriteTo(out);
  }

  return out;
}

absl::StatusOr<MemoryFootprint> MemoryFootprint::ReadFrom(std::istream& in) {
  MemoryFootprint res;
  ASSIGN_OR_RETURN(res.source_, Read<ObjectId>(in));
  ASSIGN_OR_RETURN(res.self_, Read<Memory>(in));

  ASSIGN_OR_RETURN(auto num_components, Read<uint32_t>(in));
  for (uint32_t i = 0; i < num_components; i++) {
    ASSIGN_OR_RETURN(auto label, ReadStr(in));
    ASSIGN_OR_RETURN(res.components_[label], ReadFrom(in));
  }

  return res;
}

std::ostream& operator<<(std::ostream& out, const MemoryFootprint& footprint) {
  footprint.PrintTo(out, ".");
  return out;
}

}  // namespace carmen
