#pragma once

#include <string_view>
#include <tuple>
#include <vector>

#include "absl/strings/str_cat.h"
#include "benchmark/benchmark.h"
#include "common/macro_utils.h"

namespace carmen::common::internal {

// This header file provides macros to define groups of generic benchmarks based
// on lists of types. A typical use case consists of the following steps:
//
//
//   // (1) Definition of list of target types.
//   BENCHMARK_TYPE_LIST(MyList, int, double, std::string);
//
//   // (2) Definition of benchmarks.
//   template <typename Type>
//   void BM_Example(benchmark::State& state) { .. }
//
//   // (3) Instantiation and registration of benchmarks.
//   BENCHMARK_ALL(BM_Example, MyList)->Arg(12)->Arg(14);
//
// The list defined in (1) can be reused for multiple registrations and the
// argument list in (3) can be freely adjusted. Benchmarks may also be
// registered multiple times using different type lists and/or arguments.

// --------------------------------- Type List --------------------------------

template <typename T>
struct ArgType;

template <typename R, typename A>
struct ArgType<R(A)> {
  using type = A;
};

// Binds a type to a name used as part of a generic benchmark name.
template <typename T>
struct NamedType {
  using type = T;
  NamedType(std::string_view str) : name(str) {
    // Remove extra ( ) from name.
    if (name.size() > 2 && name.front() == '(' && name.back() == ')') {
      name = name.substr(1, name.size() - 2);
    }
  }
  std::string_view name;
};

#define _INTERNAL_TO_NAMED_TYPE(TYPE)                          \
  ::carmen::common::internal::NamedType<                       \
      ::carmen::common::internal::ArgType<void(TYPE)>::type> { \
#TYPE                                                      \
  }

// Defines a list of types that can be used to instantiate a generic benchmark
// for each of its element types.
#define BENCHMARK_TYPE_LIST(NAME, ...) \
  static const auto NAME =             \
      std::make_tuple(MAP_LIST(_INTERNAL_TO_NAMED_TYPE, __VA_ARGS__));

// ------------------------------ Benchmark Group ----------------------------

// A group of instantiated generic benchmarks used internally by the
class BenchmarkGroup {
 public:
  template <typename... Args>
  BenchmarkGroup(Args&&... args) : benchmarks_({std::forward<Args>(args)...}) {}

  BenchmarkGroup(std::initializer_list<benchmark::internal::Benchmark*> list)
      : benchmarks_(std::move(list)) {}

  BenchmarkGroup* Get() { return this; }

  BenchmarkGroup* Arg(int64_t x) {
    for (auto& benchmark : benchmarks_) {
      benchmark->Arg(x);
    }
    return this;
  }

  BenchmarkGroup* ArgList(const std::vector<int64_t>& args) {
    for (auto& benchmark : benchmarks_) {
      for (const auto& cur : args) {
        benchmark->Arg(cur);
      }
    }
    return this;
  }

 private:
  std::vector<benchmark::internal::Benchmark*> benchmarks_;
};

template <template <typename T> class F, typename TypeList>
BenchmarkGroup CreateGroup(std::string_view bench_name, TypeList list) {
  return std::apply(
      [&](auto... args) {
        return BenchmarkGroup(
            (::benchmark::internal::RegisterBenchmarkInternal(
                 new ::benchmark::internal::FunctionBenchmark(
                     "", &F<typename decltype(args)::type>::run))
                 ->Name(absl::StrCat(bench_name, "<", args.name, ">")))...);
      },
      list);
}

#define _INTERNAL_BENCHMARK_ALL_IMPL(BM, LIST, UNIQUE_NAME)          \
  template <typename T>                                              \
  struct _##BM##_Wrapper_##UNIQUE_NAME {                             \
    static void run(benchmark::State& state) { BM<T>(state); }       \
  };                                                                 \
                                                                     \
  static auto BENCHMARK_PRIVATE_NAME(_benchmark_) BENCHMARK_UNUSED = \
      CreateGroup<_##BM##_Wrapper_##UNIQUE_NAME>(#BM, LIST).Get()

#define _INTERNAL_BENCHMARK_ALL(BM, LIST, LINE) \
  _INTERNAL_BENCHMARK_ALL_IMPL(BM, LIST, LINE)

#define BENCHMARK_ALL(BM, LIST) _INTERNAL_BENCHMARK_ALL(BM, LIST, __LINE__)

}  // namespace carmen::common::internal
