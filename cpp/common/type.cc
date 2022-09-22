#include "common/type.h"

namespace carmen {

    template<std::size_t SIZE>
    std::ostream& operator<<(std::ostream& out, const HexContainer<SIZE>& container) {
        std::array<char, 16> map = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

        out << "0x";
        for (auto i: container._data) {
            out << map[i >> 4];
            out << map[i & 0xF];
        }

        return out;
    }

}  // namespace carmen