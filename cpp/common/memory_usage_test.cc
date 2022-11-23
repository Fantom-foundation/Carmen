#include "common/memory_usage.h"

#include <type_traits>

#include "common/test_util.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

TEST(MemoryTest, TypeTraits) {
  EXPECT_TRUE(std::is_default_constructible_v<Memory>);
  EXPECT_TRUE(std::is_trivially_copy_constructible_v<Memory>);
  EXPECT_TRUE(std::is_trivially_copy_assignable_v<Memory>);
}

TEST(MemoryTest, DefaultValueIsZero) {
  Memory a;
  EXPECT_EQ(a.bytes(), 0);
}

TEST(MemoryTest, Comparison) {
  Memory a(12);
  Memory b(14);
  EXPECT_EQ(a, a);
  EXPECT_NE(a, b);
  EXPECT_LT(a, b);
  EXPECT_LE(a, b);
  EXPECT_GT(b, a);
  EXPECT_GE(b, a);
}

TEST(MemoryTest, Adding) {
  Memory a(10);
  Memory b(12);
  EXPECT_EQ(a + b, Memory(22));
  EXPECT_EQ(b + a, Memory(22));
}

TEST(MemoryTest, Subtraction) {
  Memory a(10);
  Memory b(12);
  EXPECT_EQ(a - b, Memory(-2));
  EXPECT_EQ(b - a, Memory(2));
}

TEST(MemoryTest, Scaling) {
  Memory a(10);
  EXPECT_EQ(a * 2, Memory(20));
  EXPECT_EQ(a * 5, Memory(50));
  EXPECT_EQ(4 * a, Memory(40));
  EXPECT_EQ(-4 * a, Memory(-40));
}

TEST(MemoryTest, Printing) {
  EXPECT_EQ(Print(Memory(12)), "12 byte");
  EXPECT_EQ(Print(Memory(15)), "15 byte");
  EXPECT_EQ(Print(Memory(-10)), "-10 byte");

  EXPECT_EQ(Print(Memory(1000)), "1000 byte");
  EXPECT_EQ(Print(Memory(1023)), "1023 byte");
  EXPECT_EQ(Print(Memory(1024)), "1.0 KiB");
  EXPECT_EQ(Print(Memory(1025)), "1.0 KiB");

  EXPECT_EQ(Print(Memory(15 * 1024)), "15.0 KiB");
  EXPECT_EQ(Print(Memory(15 * 1024 + 200)), "15.2 KiB");

  EXPECT_EQ(Print(Memory(-1024)), "-1.0 KiB");
  EXPECT_EQ(Print(Memory(-15 * 1024 + 200)), "-14.8 KiB");

  EXPECT_EQ(Print(1 * KiB), "1.0 KiB");
  EXPECT_EQ(Print(2 * MiB), "2.0 MiB");
  EXPECT_EQ(Print(3 * GiB), "3.0 GiB");
  EXPECT_EQ(Print(4 * TiB), "4.0 TiB");
  EXPECT_EQ(Print(5 * PiB), "5.0 PiB");
  EXPECT_EQ(Print(6 * EiB), "6.0 EiB");
}

}  // namespace
}  // namespace carmen
