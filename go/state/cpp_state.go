package state

//go:generate sh ../lib/build_libcarmen.sh

/*
#cgo CFLAGS: -I${SRCDIR}/../../cpp
#cgo LDFLAGS: -L${SRCDIR}/../lib -lcarmen
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/../lib
#include <stdlib.h>
#include "state/c_state.h"
*/
import "C"
import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// CppState implements the state interface by forwarding all calls to a C++ based implementation.
type CppState struct {
	// A pointer to an owned C++ object containing the actual state information.
	state unsafe.Pointer
}

func NewCppInMemoryState() (*CppState, error) {
	return &CppState{state: C.Carmen_CreateInMemoryState()}, nil
}

func NewCppFileBasedState(directory string) (*CppState, error) {
	dir := C.CString(directory)
	defer C.free(unsafe.Pointer(dir))
	return &CppState{state: C.Carmen_CreateFileBasedState(dir, C.int(len(directory)))}, nil
}

func (cs *CppState) Release() {
	if cs.state != nil {
		C.Carmen_ReleaseState(cs.state)
		cs.state = nil
	}
}

func (s *CppState) CreateAccount(address common.Address) error {
	C.Carmen_CreateAccount(s.state, unsafe.Pointer(&address[0]))
	return nil
}

func (s *CppState) GetAccountState(address common.Address) (common.AccountState, error) {
	var res common.AccountState
	C.Carmen_GetAccountState(s.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&res))
	return res, nil
}

func (s *CppState) DeleteAccount(address common.Address) error {
	C.Carmen_DeleteAccount(s.state, unsafe.Pointer(&address[0]))
	return nil
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

func (cs *CppState) GetCode(address common.Address) ([]byte, error) {
	const max_size = 25000 // Contract limit is 24577
	code := make([]byte, max_size)
	var size C.uint32_t
	C.Carmen_GetCode(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&code[0]), &size)
	if size >= max_size {
		return nil, fmt.Errorf("Unable to load contract exceeding maximum capacity of %v", max_size)
	}
	return code[0:size], nil
}

func (cs *CppState) SetCode(address common.Address, code []byte) error {
	C.Carmen_SetCode(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&code[0]), C.uint32_t(len(code)))
	return nil
}

func (cs *CppState) GetCodeHash(address common.Address) (common.Hash, error) {
	var hash common.Hash
	C.Carmen_GetCodeHash(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&hash[0]))
	return hash, nil
}

func (cs *CppState) GetHash() (common.Hash, error) {
	var hash common.Hash
	C.Carmen_GetHash(cs.state, unsafe.Pointer(&hash[0]))
	return hash, nil
}

func (cs *CppState) Flush() error {
	C.Carmen_Flush(cs.state)
	return nil
}

func (cs *CppState) Close() error {
	C.Carmen_Close(cs.state)
	return nil
}
