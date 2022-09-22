#include "common/type.h"

namespace carmen {
/**
 * Overload << operator.
 *
 * This overload will fill output stream with hex representation of Hash prefixed with "0x".
 *
 * @param out Output stream to be filled with data.
 * @param hash Hash to be read.
 * @return output stream filled with hex representation.
 */
std::ostream& operator<<(std::ostream& out, const Hash& hash) {
    std::array<char, 16> map = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    out << "0x";
    for (auto i: hash._data) {
        out << map[i >> 4];
        out << map[i & 0xF];
    }

    return out;
}

}  // namespace carmen