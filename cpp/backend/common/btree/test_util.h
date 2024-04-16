/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <vector>

namespace carmen::backend {

// Generates a vector containing the elements [0,...,size-1].
std::vector<int> GetSequence(int size);

// Shuffles the provided vector and returns the shuffled version.
std::vector<int> Shuffle(std::vector<int> data);

}  // namespace carmen::backend
