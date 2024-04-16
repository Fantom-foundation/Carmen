/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#include "archive/leveldb/encoding.h"

#include <cstdint>
#include <span>

namespace carmen::archive::leveldb {

void Write(std::uint32_t value, std::span<char, 4> trg) {
  for (int i = 0; i < 4; i++) {
    trg[i] = value >> (3 - i) * 8;
  }
}

std::uint32_t ReadUint32(std::span<const char, 4> src) {
  auto byte = [&](int i) { return std::uint32_t(std::uint8_t(src[i])); };
  return byte(0) << 24 | byte(1) << 16 | byte(2) << 8 | byte(3);
}

}  // namespace carmen::archive::leveldb
