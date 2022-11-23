#include "common/memory_usage.h"

#include <cmath>
#include <iomanip>
#include <ostream>
#include <string_view>

#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/str_format.h"

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
      if (component.source_ != nullptr &&
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

std::ostream& operator<<(std::ostream& out, const MemoryFootprint& footprint) {
  footprint.PrintTo(out, ".");
  return out;
}

}  // namespace carmen
