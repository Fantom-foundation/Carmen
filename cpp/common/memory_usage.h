#pragma once

#include <cstdint>
#include <ostream>

namespace carmen {

// A type describing an amount of memory in a type-safe way.
// Memory can be compared, assigned, added, subtracted and scaled.
class Memory {
 public:
  explicit constexpr Memory(uint64_t bytes = 0) : bytes_(bytes) {}
  auto operator<=>(const Memory&) const = default;
  constexpr std::int64_t bytes() const { return bytes_; }
 private:
  std::int64_t bytes_;
};

constexpr Memory operator+(const Memory& a, const Memory& b) {
return Memory(a.bytes() + b.bytes());
}

constexpr Memory operator-(const Memory& a, const Memory& b) {
    return Memory(a.bytes() - b.bytes());
}

constexpr Memory operator*(const Memory& a, int factor) {
    return Memory(a.bytes() * factor);
}

constexpr Memory operator*(int factor, const Memory& a) {
    return a * factor;
}

std::ostream& operator<<(std::ostream& out, const Memory& memory);

// Some memory constants.
constexpr static const Memory Byte(1);
constexpr static const Memory KiB = Byte * 1024;
constexpr static const Memory MiB = KiB * 1024;
constexpr static const Memory GiB = MiB * 1024;
constexpr static const Memory TiB = GiB * 1024;
constexpr static const Memory PiB = TiB * 1024;
constexpr static const Memory EiB = PiB * 1024;

}  // namespace carmen
