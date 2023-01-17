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
