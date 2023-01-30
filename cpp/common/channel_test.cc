#include "common/channel.h"

#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::ElementsAre;
using ::testing::Optional;

TEST(Channel, CreateAndClose) {
  Channel<int> channel;
  EXPECT_FALSE(channel.IsClosed());
  channel.Close();
  EXPECT_TRUE(channel.IsClosed());
}

TEST(Channel, CapacityLimitIsEnforced) {
  {
    Channel<int> channel(2);
    EXPECT_TRUE(channel.TryPush(0));
    EXPECT_TRUE(channel.TryPush(1));
    EXPECT_FALSE(channel.TryPush(2));
  }
  {
    Channel<int> channel(3);
    EXPECT_TRUE(channel.TryPush(0));
    EXPECT_TRUE(channel.TryPush(1));
    EXPECT_TRUE(channel.TryPush(2));
    EXPECT_FALSE(channel.TryPush(3));
  }
}

TEST(Channel, ElementsAreDeliveredInOrder) {
  Channel<int> channel(3);
  channel.Push(1);
  channel.Push(2);
  channel.Push(3);

  EXPECT_THAT(channel.Pop(), Optional(1));
  EXPECT_THAT(channel.Pop(), Optional(2));
  EXPECT_THAT(channel.Pop(), Optional(3));
}

TEST(Channel, ClosedChannelDeliversANullOpt) {
  Channel<int> channel(3);
  channel.Close();
  EXPECT_THAT(channel.Pop(), std::nullopt);
  EXPECT_THAT(channel.Pop(), std::nullopt);
}

TEST(Channel, ElementsPushedAfterClosingAChannelAreIgnored) {
  Channel<int> channel(3);
  channel.Push(1);
  channel.Close();
  channel.Push(2);
  EXPECT_THAT(channel.Pop(), Optional(1));
  EXPECT_THAT(channel.Pop(), std::nullopt);
  EXPECT_THAT(channel.Pop(), std::nullopt);
}

TEST(Channel, PushingFailsAfterClosingAChannel) {
  Channel<int> channel(3);
  EXPECT_TRUE(channel.TryPush(1));
  channel.Close();
  EXPECT_FALSE(channel.TryPush(2));
}

TEST(Channel, SequencesCanBeStreamedThroughChannel) {
  constexpr int N = 1000;
  Channel<int> channel(3);

  channel.Push(0);
  channel.Push(1);

  for (int i = 0; i <= N; i++) {
    channel.Push(i + 2);
    EXPECT_THAT(channel.Pop(), Optional(i));
  }

  EXPECT_THAT(channel.Pop(), Optional(N + 1));
  EXPECT_THAT(channel.Pop(), Optional(N + 2));
}

TEST(Channel, CanBeUsedToPipelineWork) {
  Channel<int> channel;

  std::vector<int> data;
  auto thread = std::thread([&] {
    auto in = channel.Pop();
    while (in.has_value()) {
      data.push_back(*in);
      in = channel.Pop();
    }
  });

  for (int i = 0; i < 5; i++) {
    channel.Push(i);
  }
  channel.Close();
  thread.join();

  EXPECT_THAT(data, ElementsAre(0, 1, 2, 3, 4));
}

}  // namespace
}  // namespace carmen
