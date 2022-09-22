#pragma once

#include <array>
#include <cstdint>
#include <iostream>

namespace carmen {

template<std::size_t N>
class HexContainer {
public:
    HexContainer<N>(std::array<std::uint8_t, N> data): _data(data) {}

    friend std::ostream& operator<<(std::ostream&, const HexContainer<N>&);
    friend auto operator<=>(const HexContainer<N>&, const HexContainer<N>&) = default;
private:
    std::array<std::uint8_t, N> _data;
};

class Hash: public HexContainer<32>  {

public:
    Hash(std::array<std::uint8_t, 32> data): HexContainer(data) {}
};


} // namespace carmen