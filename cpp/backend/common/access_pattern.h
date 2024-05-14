// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include <random>

namespace carmen::backend {

// Simulates a sequential access pattern accessing elements in a half-open range
// [0,...,size) in round-robin order.
class Sequential {
 public:
  explicit Sequential(std::size_t size) : size_(size) {}

  // Retrieves the next value in the access sequence.
  std::size_t Next() {
    auto res = next_++;
    if (next_ >= size_) {
      next_ = 0;
    }
    return res;
  }

 private:
  std::size_t size_;
  std::size_t next_;
};

// Simulates an uniformly distributed access pattern to a range of
// [0,...,size).
class Uniform {
 public:
  explicit Uniform(std::size_t size) : gen_(rd_()), dist_(0, size - 1) {}

  // Retrieves the next value in the access sequence.
  std::size_t Next() { return dist_(gen_); }

 private:
  std::random_device rd_;
  std::mt19937 gen_;
  std::uniform_int_distribution<> dist_;
};

// Simulates an exponentially distributed access pattern to a range of
// [0,...,size).
class Exponential {
 public:
  explicit Exponential(std::size_t size)
      : size_(size), gen_(rd_()), dist_(double(10) / size) {}

  // Retrieves the next value in the access sequence.
  std::size_t Next() { return static_cast<std::size_t>(dist_(gen_)) % size_; }

 private:
  std::size_t size_;
  std::random_device rd_;
  std::mt19937 gen_;
  std::exponential_distribution<> dist_;
};

}  // namespace carmen::backend
