package carmen

import (
	"errors"
	"math/big"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
)

type queryContext struct {
	state state.State
	err   error
}

func (c *queryContext) GetBalance(address Address) *big.Int {
	if c.err != nil {
		return big.NewInt(0)
	}
	res, err := c.state.GetBalance(common.Address(address))
	if err != nil {
		c.err = err
		return big.NewInt(0)
	}
	return new(big.Int).SetBytes(res[:])
}

func (c *queryContext) GetNonce(address Address) uint64 {
	if c.err != nil {
		return 0
	}
	res, err := c.state.GetNonce(common.Address(address))
	if err != nil {
		c.err = err
		return 0
	}
	return res.ToUint64()
}

func (c *queryContext) GetState(address Address, key Key) Value {
	if c.err != nil {
		return Value{}
	}
	res, err := c.state.GetStorage(common.Address(address), common.Key(key))
	if err != nil {
		c.err = err
		return Value{}
	}
	return Value(res)
}

func (c *queryContext) GetCode(address Address) []byte {
	if c.err != nil {
		return nil
	}
	res, err := c.state.GetCode(common.Address(address))
	if err != nil {
		c.err = err
		return nil
	}
	return res
}

func (c *queryContext) GetCodeHash(address Address) Hash {
	if c.err != nil {
		return Hash{}
	}
	res, err := c.state.GetCodeHash(common.Address(address))
	if err != nil {
		c.err = err
		return Hash{}
	}
	return Hash(res)
}

func (c *queryContext) GetCodeSize(address Address) int {
	if c.err != nil {
		return 0
	}
	res, err := c.state.GetCodeSize(common.Address(address))
	if err != nil {
		c.err = err
		return 0
	}
	return res
}

func (c *queryContext) GetStateHash() Hash {
	if c.err != nil {
		return Hash{}
	}
	res, err := c.state.GetHash()
	if err != nil {
		c.err = err
		return Hash{}
	}
	return Hash(res)
}

func (c *queryContext) Check() error {
	return errors.Join(c.err, c.state.Check())
}
