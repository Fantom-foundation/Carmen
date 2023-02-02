#include "common/account_state.h"

#include <ostream>

namespace carmen {

std::ostream& operator<<(std::ostream& out, AccountState s) {
  switch (s) {
    case AccountState::kUnknown:
      return out << "unknown";
    case AccountState::kExists:
      return out << "exists";
  }
  return out << "invalid";
}

}  // namespace carmen
