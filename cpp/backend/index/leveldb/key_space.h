#pragma once

namespace carmen::backend::index {
enum class KeySpace : char {
  kBalance = 'B',
  kNonce = 'N',
  kSlot = 'S',
  kValue = 'V'
};
}


