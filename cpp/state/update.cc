#include "state/update.h"

namespace carmen {

void Update::Set(const Address& account, const Key& key,
                      const Value& value) {
  storage_[{account, key}] = value;
}

}
