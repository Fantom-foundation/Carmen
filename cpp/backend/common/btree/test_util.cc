/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/common/btree/test_util.h"

#include <algorithm>
#include <random>
#include <vector>

namespace carmen::backend {

std::vector<int> GetSequence(int size) {
  std::vector<int> data;
  for (int i = 0; i < size; i++) {
    data.push_back(i);
  }
  return data;
}

std::vector<int> Shuffle(std::vector<int> data) {
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(data.begin(), data.end(), g);
  return data;
}

}  // namespace carmen::backend
