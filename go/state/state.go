package state

import "github.com/Fantom-foundation/Carmen/go/common"

type State interface {
	GetBalance(common.Address)
	GetNonce(address common.Address) uint64
	GetStorage(address common.Address, key common.Key) common.Value
}
