#include "state/schema.h"

#include <ostream>
#include <string_view>

namespace carmen {

std::ostream& operator<<(std::ostream& out, const Schema& s) {
  static const StateFeature features[] = {
      StateFeature::kAddressId,
      StateFeature::kKeyId,
      StateFeature::kAccountReincarnation,
  };
  static const std::string_view names[] = {
      "address_id",
      "key_id",
      "account_reincarnation",
  };

  out << '{';
  bool first = true;
  for (std::size_t i = 0; i < sizeof(features); i++) {
    if (!s.HashFeature(features[i])) continue;
    if (!first) {
      out << ',';
    }
    first = false;
    out << names[i];
  }
  return out << '}';
}

}  // namespace carmen
