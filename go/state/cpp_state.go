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
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const CodeCacheSize = 8_000 // ~ 200 MiB of memory for go-side code cache
const CodeMaxSize = 25000   // Contract limit is 24577

// CppState implements the state interface by forwarding all calls to a C++ based implementation.
type CppState struct {
	// A pointer to an owned C++ object containing the actual state information.
	state unsafe.Pointer
	// cache of contract codes
	codeCache *common.Cache[common.Address, []byte]
}

func newCppState(impl C.enum_StateImpl, params Parameters) (State, error) {
	dir := C.CString(params.Directory)
	defer C.free(unsafe.Pointer(dir))

	state := C.Carmen_OpenState(C.C_Schema(params.Schema), impl, C.enum_StateImpl(params.Archive), dir, C.int(len(params.Directory)))
	if state == unsafe.Pointer(nil) {
		return nil, UnsupportedConfiguration(fmt.Sprintf("failed to create C++ state instance for parameters %v", params))
	}

	return wrapIntoSyncedState(&CppState{
		state:     state,
		codeCache: common.NewCache[common.Address, []byte](CodeCacheSize),
	}), nil
}

func NewCppInMemoryState(params Parameters) (State, error) {
	return newCppState(C.kState_Memory, params)
}

func NewCppFileBasedState(params Parameters) (State, error) {
	return newCppState(C.kState_File, params)
}

func NewCppLevelDbBasedState(params Parameters) (State, error) {
	return newCppState(C.kState_LevelDb, params)
}

func (cs *CppState) createAccount(address common.Address) error {
	update := common.Update{}
	update.AppendCreateAccount(address)
	return cs.Apply(0, update)
}

func (cs *CppState) Exists(address common.Address) (bool, error) {
	var res common.AccountState
	C.Carmen_GetAccountState(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&res))
	return res == common.Exists, nil
}

func (cs *CppState) deleteAccount(address common.Address) error {
	update := common.Update{}
	update.AppendDeleteAccount(address)
	return cs.Apply(0, update)
}

func (cs *CppState) GetBalance(address common.Address) (common.Balance, error) {
	var balance common.Balance
	C.Carmen_GetBalance(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&balance[0]))
	return balance, nil
}

func (cs *CppState) setBalance(address common.Address, balance common.Balance) error {
	update := common.Update{}
	update.AppendBalanceUpdate(address, balance)
	return cs.Apply(0, update)
}

func (cs *CppState) GetNonce(address common.Address) (common.Nonce, error) {
	var nonce common.Nonce
	C.Carmen_GetNonce(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&nonce[0]))
	return nonce, nil
}

func (cs *CppState) setNonce(address common.Address, nonce common.Nonce) error {
	update := common.Update{}
	update.AppendNonceUpdate(address, nonce)
	return cs.Apply(0, update)
}

func (cs *CppState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	var value common.Value
	C.Carmen_GetStorageValue(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&key[0]), unsafe.Pointer(&value[0]))
	return value, nil
}

func (cs *CppState) setStorage(address common.Address, key common.Key, value common.Value) error {
	update := common.Update{}
	update.AppendSlotUpdate(address, key, value)
	return cs.Apply(0, update)
}

func (cs *CppState) GetCode(address common.Address) ([]byte, error) {
	// Try to obtain the code from the cache
	code, exists := cs.codeCache.Get(address)
	if exists {
		return code, nil
	}

	// Load the code from C++
	code = make([]byte, CodeMaxSize)
	var size C.uint32_t = CodeMaxSize
	C.Carmen_GetCode(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&code[0]), &size)
	if size >= CodeMaxSize {
		return nil, fmt.Errorf("unable to load contract exceeding maximum capacity of %d", CodeMaxSize)
	}
	if size > 0 {
		code = code[0:size]
	} else {
		code = nil
	}
	cs.codeCache.Set(address, code)
	return code, nil
}

func (cs *CppState) setCode(address common.Address, code []byte) error {
	update := common.Update{}
	update.AppendCodeUpdate(address, code)
	return cs.Apply(0, update)
}

func (cs *CppState) GetCodeHash(address common.Address) (common.Hash, error) {
	var hash common.Hash
	C.Carmen_GetCodeHash(cs.state, unsafe.Pointer(&address[0]), unsafe.Pointer(&hash[0]))
	return hash, nil
}

func (cs *CppState) GetCodeSize(address common.Address) (int, error) {
	var size C.uint32_t
	C.Carmen_GetCodeSize(cs.state, unsafe.Pointer(&address[0]), &size)
	return int(size), nil
}

func (cs *CppState) GetHash() (common.Hash, error) {
	var hash common.Hash
	C.Carmen_GetHash(cs.state, unsafe.Pointer(&hash[0]))
	return hash, nil
}

func (cs *CppState) Apply(block uint64, update common.Update) error {
	if update.IsEmpty() {
		return nil
	}
	data := update.ToBytes()
	dataPtr := unsafe.Pointer(&data[0])
	C.Carmen_Apply(cs.state, C.uint64_t(block), dataPtr, C.uint64_t(len(data)))
	// Apply code changes to Go-sided code cache.
	for _, change := range update.Codes {
		cs.codeCache.Set(change.Account, change.Code)
	}
	return nil
}

func (cs *CppState) Dump() {
	fmt.Printf("dumping of C++ state not implemented")
}

func (cs *CppState) Flush() error {
	C.Carmen_Flush(cs.state)
	return nil
}

func (cs *CppState) Close() error {
	if cs.state != nil {
		C.Carmen_Close(cs.state)
		C.Carmen_ReleaseState(cs.state)
		cs.state = nil
	}
	return nil
}

func (s *CppState) GetProof() (backend.Proof, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *CppState) CreateSnapshot() (backend.Snapshot, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *CppState) Restore(data backend.SnapshotData) error {
	return backend.ErrSnapshotNotSupported
}

func (s *CppState) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (cs *CppState) GetMemoryFootprint() *common.MemoryFootprint {
	if cs.state == nil {
		return common.NewMemoryFootprint(unsafe.Sizeof(*cs))
	}

	// Fetch footprint data from C++.
	var buffer *C.char
	var size C.uint64_t
	C.Carmen_GetMemoryFootprint(cs.state, &buffer, &size)
	defer func() {
		C.free(unsafe.Pointer(buffer))
	}()

	data := C.GoBytes(unsafe.Pointer(buffer), C.int(size))

	// Use an index map mapping object IDs to memory footprints to facilitate
	// sharing of sub-structures.
	index := map[objectId]*common.MemoryFootprint{}
	res, unusedData := parseCMemoryFootprint(data, index)
	if len(unusedData) != 0 {
		panic("Failed to consume all of the provided footprint data")
	}

	res.AddChild("goCodeCache", cs.codeCache.GetDynamicMemoryFootprint(func(code []byte) uintptr {
		return uintptr(cap(code)) // memory consumed by the code slice
	}))
	return res
}

func (cs *CppState) GetArchiveState(block uint64) (State, error) {
	return &CppState{
		state:     C.Carmen_GetArchiveState(cs.state, C.uint64_t(block)),
		codeCache: common.NewCache[common.Address, []byte](CodeCacheSize),
	}, nil
}

type objectId struct {
	obj_loc, obj_type uint64
}

func (o *objectId) isUnique() bool {
	return o.obj_loc == 0 && o.obj_type == 0
}

func readUint32(data []byte) (uint32, []byte) {
	return binary.LittleEndian.Uint32(data[:4]), data[4:]
}

func readUint64(data []byte) (uint64, []byte) {
	return binary.LittleEndian.Uint64(data[:8]), data[8:]
}

func readObjectId(data []byte) (objectId, []byte) {
	obj_loc, data := readUint64(data)
	obj_type, data := readUint64(data)
	return objectId{obj_loc, obj_type}, data
}

func readString(data []byte) (string, []byte) {
	length, data := readUint32(data)
	return string(data[:length]), data[length:]
}

func parseCMemoryFootprint(data []byte, index map[objectId]*common.MemoryFootprint) (*common.MemoryFootprint, []byte) {
	// 1) read object ID
	objId, data := readObjectId(data)

	// 2) read memory usage
	memUsage, data := readUint64(data)
	res := common.NewMemoryFootprint(uintptr(memUsage))

	// 3) read number of sub-components
	num_components, data := readUint32(data)

	// 4) read sub-components
	for i := 0; i < int(num_components); i++ {
		var label string
		label, data = readString(data)
		var child *common.MemoryFootprint
		child, data = parseCMemoryFootprint(data, index)
		res.AddChild(label, child)
	}

	// Unique objects are not cached since they shall not be reused.
	if objId.isUnique() {
		return res, data
	}

	// Return representative instance based on object ID.
	if represent, exists := index[objId]; exists {
		return represent, data
	}
	index[objId] = res
	return res, data
}
