#pragma once

#include <type_traits>
#include <variant>

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

// ----------------------------------------------------------------------------
//                                De-duplication
// ----------------------------------------------------------------------------

namespace internal {

// A meta-programming function testing whether T is listed in the Ts.
template <typename T, typename... Ts>
struct contains;

template <typename T>
struct contains<T> : public std::false_type {};

template <typename T, typename F, typename... Ts>
struct contains<T, F, Ts...>
    : public std::conditional_t<std::is_same_v<T, F>, std::true_type,
                                contains<T, Ts...>> {};

// The internal implementation of the Variant<..> type alias declared below.
template <typename... Types>
struct to_variant_type;

template <>
struct to_variant_type<> {
  template <typename... Prefix>
  using to_type = std::variant<Prefix...>;
};

template <typename First, typename... Rest>
struct to_variant_type<First, Rest...> {
  template <typename... Prefix>
  using to_type = std::conditional_t<
      contains<First, Prefix...>::value,
      typename to_variant_type<Rest...>::template to_type<Prefix...>,
      typename to_variant_type<Rest...>::template to_type<Prefix..., First>>;
};

}  // namespace internal

// A type alias combining the given types into a variant by pruning types listed
// more than once.
//
// For instance, the std::variant<..> type would now allow a type like
// std::vector<int,bool,int>, since int is listed more than once. However, for
// most use cases, such a type is equivalent to std::variant<int,bool>. This
// type alias is removing duplicates types and produces a variant listing each
// type exactly once. In particular, Variant<int,bool,int> is the same as
// std::variant<int,bool>.
template <typename... Types>
using Variant =
    typename internal::to_variant_type<Types...>::template to_type<>;

}  // namespace carmen
