#include "common/type.h"

#include <iostream>

namespace carmen {

std::ostream& operator<<(std::ostream& out, const Hash& hash) {
    return out << "Hello";
}


}  // namespace carmen