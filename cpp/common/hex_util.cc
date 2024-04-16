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

#include "hex_util.h"

#include <cstdint>
#include <ostream>
#include <span>

namespace carmen::hex_util {

namespace {
const std::array<char, 16> kHexMap = {'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
}  // namespace

// Write hex representation of given sequence prefixed with "0x" into ostream.
void WriteTo(std::ostream& out, std::span<const std::uint8_t> data) {
  out << "0x";
  for (const auto& i : data) {
    out << kHexMap[i >> 4];
    out << kHexMap[i & 0xF];
  }
}

}  // namespace carmen::hex_util
