#include "state/schema.h"

#include <ostream>

namespace carmen {

std::ostream& operator<<(std::ostream& out, const Schema& s) {
  out << '{';
  bool first = false;
  if (s.HashFeature(StateFeature::kKeyId)) {
    out << "key_id";
    first = false;
  }
  if (s.HashFeature(StateFeature::kAccountReincarnation)) {
    if (!first) {
      out << ',';
    }
    out << "account_reincarnation";
    first = false;
  }
  return out << '}';
}

bool ProduceSameHash(Schema a, Schema b) {
  return (a.HashFeature(StateFeature::kAccountReincarnation) ==
          b.HashFeature(StateFeature::kAccountReincarnation));
}

}  // namespace carmen
