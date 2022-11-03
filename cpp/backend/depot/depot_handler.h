#include <cstddef>

#include "backend/depot/depot.h"

namespace carmen::backend::depot {
namespace {

// A generic depot handler enclosing the setup and tear down of various depot
// implementations in benchmarks handled by depot_benchmark.cc. A handler holds
// an instance of a depot configured with given branching factor and number of
// boxes used for hashing.
//
// This generic DepotHandler is a mere wrapper on a depot reference, while
// specializations may add additional setup and tear-down operations.
template <Depot Depot, std::size_t branching_factor, std::size_t num_hash_boxes>
class DepotHandler {
 public:
  constexpr static std::size_t kBranchingFactor = branching_factor;
  constexpr static std::size_t kNumHashBoxes = num_hash_boxes;

  DepotHandler() : depot_(branching_factor, num_hash_boxes) {}
  Depot& GetDepot() { return depot_; }

 private:
  Depot depot_;
};
}  // namespace
}  // namespace carmen::backend::depot
