// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "common/memory_usage.h"

#include <type_traits>

#include "common/status_test_util.h"
#include "common/test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::HasSubstr;

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

TEST(MemoryFootprintTest, MemoryUsageReportsSizeOf) {
  int a;
  std::string s;
  EXPECT_EQ(MemoryFootprint(a).GetTotal(), Memory(sizeof(a)));
  EXPECT_EQ(MemoryFootprint(s).GetTotal(), Memory(sizeof(s)));
}

TEST(MemoryFootprintTest, SelfIsIncludedInTotal) {
  MemoryFootprint a(Memory(12));
  EXPECT_EQ(a.GetTotal(), Memory(12));
}

TEST(MemoryFootprintTest, ComponentsAreIncludedInTotal) {
  MemoryFootprint a;
  a.Add("B", Memory(10));
  a.Add("C", Memory(14));
  EXPECT_EQ(a.GetTotal(), Memory(24));
}

TEST(MemoryFootprintTest, DeeperHierarchiesAreCovered) {
  MemoryFootprint l;
  l.Add("l1", Memory(1));
  l.Add("l2", Memory(2));

  MemoryFootprint r;
  r.Add("r1", Memory(3));
  r.Add("r2", Memory(4));
  r.Add("r3", Memory(5));

  MemoryFootprint t;
  t.Add("l", l);
  t.Add("r", r);
  EXPECT_EQ(t.GetTotal(), Memory(1 + 2 + 3 + 4 + 5));
}

TEST(MemoryFootprintTest, CommonSubComponentsAreOnlyCountedOnce) {
  int obj;
  MemoryFootprint o(obj);
  MemoryFootprint r;
  r.Add("l", o);
  r.Add("r", o);
  EXPECT_EQ(r.GetTotal(), Memory(sizeof(obj)));
}

TEST(MemoryFootprintTest, PrintingListsComponents) {
  MemoryFootprint l;
  l.Add("l1", Memory(1));
  l.Add("l2", Memory(2));

  MemoryFootprint r;
  r.Add("r1", Memory(3));
  r.Add("r2", Memory(4));
  r.Add("r3", Memory(5));

  MemoryFootprint t;
  t.Add("l", l);
  t.Add("r", r);

  auto print = Print(t);
  EXPECT_THAT(print, HasSubstr("1 byte\t./l/l1"));
  EXPECT_THAT(print, HasSubstr("2 byte\t./l/l2"));
  EXPECT_THAT(print, HasSubstr("3 byte\t./l"));
  EXPECT_THAT(print, HasSubstr("3 byte\t./r/r1"));
  EXPECT_THAT(print, HasSubstr("4 byte\t./r/r2"));
  EXPECT_THAT(print, HasSubstr("5 byte\t./r/r3"));
  EXPECT_THAT(print, HasSubstr("12 byte\t./r"));
  EXPECT_THAT(print, HasSubstr("15 byte\t."));
}

TEST(MemoryFootprintTest, ObjectsAtSameLocationAreDifferentiated) {
  struct Data {
    int32_t a;
  };

  Data data;
  ASSERT_EQ(reinterpret_cast<void*>(&data), reinterpret_cast<void*>(&data.a));
  ASSERT_EQ(sizeof(data), sizeof(data.a));

  // This example is not what should actually be done since it computes the
  // memory usage of field 'a' twice, but it demonstrates that the memory
  // footprint infra can distinguish between the parent object and the field.
  MemoryFootprint res(data);
  res.Add("a", MemoryFootprint(data.a));
  EXPECT_EQ(res.GetTotal(), 2 * Memory(sizeof(data)));
}

TEST(MemoryFootprintTest, PrintingListsSharedComponents) {
  MemoryFootprint s;
  s.Add("s1", Memory(1));
  s.Add("s2", Memory(2));

  MemoryFootprint t;
  t.Add("l", s);
  t.Add("r", s);

  auto print = Print(t);
  EXPECT_THAT(print, HasSubstr("1 byte\t./l/s1"));
  EXPECT_THAT(print, HasSubstr("2 byte\t./l/s2"));
  EXPECT_THAT(print, HasSubstr("3 byte\t./l"));
  EXPECT_THAT(print, HasSubstr("1 byte\t./r/s1"));
  EXPECT_THAT(print, HasSubstr("2 byte\t./r/s2"));
  EXPECT_THAT(print, HasSubstr("3 byte\t./r"));
  EXPECT_THAT(print, HasSubstr("3 byte\t."));
}

TEST(MemoryFootprintTest, CanBeSerializedAndReloaded) {
  MemoryFootprint s;
  s.Add("s1", Memory(1));
  s.Add("s2", Memory(2));

  MemoryFootprint t;
  t.Add("l", s);
  t.Add("r", s);

  std::stringstream buffer;
  t.WriteTo(buffer);

  ASSERT_OK_AND_ASSIGN(auto reloaded, MemoryFootprint::ReadFrom(buffer));
  EXPECT_EQ(Print(t), Print(reloaded));
}

}  // namespace
}  // namespace carmen
