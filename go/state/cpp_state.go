package state

//go:generate sh ../lib/build_libstate.sh

/*
#cgo CFLAGS: -I${SRCDIR}/../../cpp
#cgo LDFLAGS: -L${SRCDIR}/../lib -lstate
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/../lib
#include "state/c_state.h"
*/
import "C"
import (
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// CppState implements the state interface by forwarding all calls to a C++ based implementation.
type CppState struct {
	// A pointer to an owned C++ object containing the actual state information.
	state unsafe.Pointer
}

func NewCppState() (*CppState, error) {
	return &CppState{state: C.Carmen_CreateState()}, nil
}

func (cs *CppState) Release() {
	if cs.state != nil {
		C.Carmen_ReleaseState(cs.state)
		cs.state = nil
	}
}

func (cs *CppState) GetBalance(address common.Address) (common.Balance, error) {
	var balance common.Balance
	C.Carmen_GetBalance(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&balance[0]))
	return balance, nil
}

func (cs *CppState) SetBalance(address common.Address, balance common.Balance) error {
	C.Carmen_SetBalance(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&balance[0]))
	return nil
}

func (cs *CppState) GetNonce(address common.Address) (common.Nonce, error) {
	var nonce common.Nonce
	C.Carmen_GetNonce(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&nonce[0]))
	return nonce, nil
}

func (cs *CppState) SetNonce(address common.Address, nonce common.Nonce) error {
	C.Carmen_SetNonce(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&nonce[0]))
	return nil
}

func (cs *CppState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	var value common.Value
	C.Carmen_GetStorageValue(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&key[0]), unsafe.Pointer(&value[0]))
	return value, nil
}

func (cs *CppState) SetStorage(address common.Address, key common.Key, value common.Value) error {
	C.Carmen_SetStorageValue(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&key[0]), unsafe.Pointer(&value[0]))
	return nil
}

func (cs *CppState) GetHash() (common.Hash, error) {
	var hash common.Hash
	C.Carmen_GetHash(cs.state, unsafe.Pointer(&hash[0]))
	return hash, nil
}
