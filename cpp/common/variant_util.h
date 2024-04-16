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

#pragma once

namespace carmen {

// ----------------------------------------------------------------------------
//                                  Match
// ----------------------------------------------------------------------------

// Defines a utility to simplify the definition of static visitors for variants.
// A common use case would look as follows:
//
//  std::variant<TypeA,TypeB,TypeC> value = ...;
//  std::visit(
//     match {
//       [&](const TypeA& a) {
//           // handle the case where a value of type A is stored in value.
//       },
//       [&](const TypeB& b) {
//           // handle the case where a value of type B is stored in value.
//       },
//       [&](const TypeC& c) {
//           // handle the case where a value of type C is stored in value.
//       },
//     }, value
//  );
//
template <class... Ts>
struct match : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
match(Ts...) -> match<Ts...>;

}  // namespace carmen
