#pragma once

#include <vector>

namespace carmen::backend {

// Generates a vector containing the elements [0,...,size-1].
std::vector<int> GetSequence(int size);

// Shuffles the provided vector and returns the shuffled version.
std::vector<int> Shuffle(std::vector<int> data);

}  // namespace carmen::backend
