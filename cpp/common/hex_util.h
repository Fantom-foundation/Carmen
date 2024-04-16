/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <cstdint>
#include <ostream>
#include <span>

namespace carmen::hex_util {

// Write hex representation of given sequence prefixed with "0x" into ostream.
void WriteTo(std::ostream& out, std::span<const std::uint8_t> data);

}  // namespace carmen::hex_util
