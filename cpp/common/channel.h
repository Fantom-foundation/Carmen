#pragma once

#include <optional>
#include <queue>

#include "absl/synchronization/mutex.h"

namespace carmen {

// Provides a fixed-length blocking queue facilitating the thread-safe streaming
// of data between threads. Each channel has a capacity (>0) of elements it can
// buffer internally. While the buffer is not full, new elements can be added
// without blocking the inserting thread. Also, while the buffer is not empty,
// elements can be read without blocking the reader. However, writers will block
// on full buffers and readers will block un empty channels, unless, in the
// latter case, the channel gets closed.
template <typename T>
class Channel {
 public:
  // Creates a channel with the given buffer capacity. The capacity has to be
  // larger than zero. The resulting channel is open and ready to forward data.
  Channel(std::size_t capacity = 10);

  // Pushes a new element into the channel, blocks if the channel is open and
  // full. If the channel is closed, this call has no effect.
  void Push(T value) LOCKS_EXCLUDED(lock_);

  // Attempts to push an element into the channel. It returns true if the
  // channel was open and there was capacity in the channel and the element was
  // added, false otherwise. This function never blocks.
  bool TryPush(T value) LOCKS_EXCLUDED(lock_);

  // Retrieves an element from this channel. If the channel is currently empty,
  // the operation blocks until either an element is available or the channel
  // got closed. The result is std::nullopt if the channel got closed.
  std::optional<T> Pop() LOCKS_EXCLUDED(lock_);

  // Allows to test whether the channel is still open.
  bool IsClosed() const LOCKS_EXCLUDED(lock_);

  // Closes the channel. A channel can only be closed once, never re-opened.
  // Closing it a second time is a no-op.
  void Close() LOCKS_EXCLUDED(lock_);

 private:
  const std::size_t capacity;
  mutable absl::Mutex lock_;
  std::queue<T> queue_ GUARDED_BY(lock_);
  bool closed_ GUARDED_BY(lock_) = false;
};

template <typename T>
Channel<T>::Channel(std::size_t capacity) : capacity(capacity) {}

template <typename T>
void Channel<T>::Push(T value) {
  absl::MutexLock guard(&lock_);
  if (closed_) return;
  auto free_space = [&]() EXCLUSIVE_LOCKS_REQUIRED(lock_) {
    return queue_.size() < capacity;
  };
  lock_.Await(absl::Condition(&free_space));
  queue_.push(std::move(value));
}

template <typename T>
bool Channel<T>::TryPush(T value) {
  absl::MutexLock guard(&lock_);
  if (closed_) return false;
  if (queue_.size() >= capacity) {
    return false;
  }
  queue_.push(std::move(value));
  return true;
}

template <typename T>
std::optional<T> Channel<T>::Pop() {
  absl::MutexLock guard(&lock_);
  auto has_result = [&]() EXCLUSIVE_LOCKS_REQUIRED(lock_) {
    return closed_ || queue_.size() > 0;
  };
  lock_.Await(absl::Condition(&has_result));
  if (!queue_.empty()) {
    T res = std::move(queue_.front());
    queue_.pop();
    return res;
  }
  return std::nullopt;
}

template <typename T>
bool Channel<T>::IsClosed() const {
  absl::MutexLock guard(&lock_);
  return closed_;
}

template <typename T>
void Channel<T>::Close() {
  absl::MutexLock guard(&lock_);
  closed_ = true;
}

}  // namespace carmen
