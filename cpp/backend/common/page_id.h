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

#include <cstdint>

namespace carmen::backend {

// A PageId is used to identify a page within a file. Pages are to be indexed in
// sequence starting with 0. Thus, a page ID of 5 present in a file implicitly
// asserts the existence of pages 0-4 in the same file.
using PageId = std::size_t;

}  // namespace carmen::backend
