// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"go.uber.org/mock/gomock"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/maps"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
)

var stateFactories = map[string]func(string) (io.Closer, error){
	"memory": func(dir string) (io.Closer, error) {
		return OpenGoMemoryState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	},
	"file": func(dir string) (io.Closer, error) {
		return OpenGoFileState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	},
	"archive": func(dir string) (io.Closer, error) {
		return OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	},
	"verify": func(dir string) (io.Closer, error) {
		return openVerificationNodeSource(context.Background(), dir, S5LiveConfig)
	},
}

var mptStateFactories = map[string]func(string) (*MptState, error){
	"memory": func(dir string) (*MptState, error) {
		return OpenGoMemoryState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	},
	"file": func(dir string) (*MptState, error) {
		return OpenGoFileState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	},
}

func TestState_CanOnlyBeOpenedOnce(t *testing.T) {
	for nameA, openA := range stateFactories {
		for nameB, openB := range stateFactories {
			t.Run(nameA+"_"+nameB, func(t *testing.T) {
				dir := t.TempDir()
				state, err := openA(dir)
				if err != nil {
					t.Fatalf("failed to open test state: %v", err)
				}
				if _, err := openB(dir); err == nil {
					t.Fatalf("state should not be accessible by more than one instance")
				} else if !strings.Contains(err.Error(), "failed to acquire file lock") {
					t.Errorf("missing hint of locking issue in error: %v", err)
				}
				if err := state.Close(); err != nil {
					t.Errorf("failed to close the state: %v", err)
				}
			})
		}
	}
}

func TestState_CanBeReOpened(t *testing.T) {
	for name, open := range stateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			for i := 0; i < 5; i++ {
				state, err := open(dir)
				if err != nil {
					t.Fatalf("failed to open test state: %v", err)
				}
				if err := state.Close(); err != nil {
					t.Errorf("failed to close the state: %v", err)
				}
			}
		})
	}
}

func TestState_DirtyStateCanNotBeOpened(t *testing.T) {
	for name, open := range stateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			if err := markDirty(dir); err != nil {
				t.Fatalf("failed to mark directory as dirty: %v", err)
			}

			_, err := open(dir)
			if err == nil {
				t.Fatalf("dirty state should fail to be opened")
			}

			if !strings.Contains(err.Error(), "likely corrupted") {
				t.Errorf("unexpected error message: %s", err.Error())
			}
		})
	}
}

func TestState_RegularCloseResultsInCleanState(t *testing.T) {
	dir := t.TempDir()
	if dirty, err := isDirty(dir); dirty || err != nil {
		t.Fatalf("directory initially in invalid state: %t, %v", dirty, err)
	}
	state, err := OpenGoFileState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to open test state: %v", err)
	}

	if dirty, err := isDirty(dir); !dirty || err != nil {
		t.Fatalf("opened directory in invalid state: %t, %v", dirty, err)
	}

	if err := state.Close(); err != nil {
		t.Errorf("failed to close the state: %v", err)
	}

	if dirty, err := isDirty(dir); dirty || err != nil {
		t.Fatalf("closed directory in invalid state: %t, %v", dirty, err)
	}
}

func TestState_ErrorsLeadToDirtyState(t *testing.T) {
	dir := t.TempDir()
	if dirty, err := isDirty(dir); dirty || err != nil {
		t.Fatalf("directory initially in invalid state: %t, %v", dirty, err)
	}
	state, err := OpenGoFileState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to open test state: %v", err)
	}

	if dirty, err := isDirty(dir); !dirty || err != nil {
		t.Fatalf("opened directory in invalid state: %t, %v", dirty, err)
	}

	injectedError := fmt.Errorf("injected-error")
	if err := state.closeWithError(injectedError); !errors.Is(err, injectedError) {
		t.Errorf("failed to close the state: %v", err)
	}

	if dirty, err := isDirty(dir); !dirty || err != nil {
		t.Fatalf("closed directory in invalid state: %t, %v", dirty, err)
	}
}

func BenchmarkStorageChanges(b *testing.B) {
	for _, config := range allMptConfigs {
		for _, withHashing := range []bool{false, true} {
			mode := "just_update"
			if withHashing {
				mode = "with_hashing"
			}
			b.Run(fmt.Sprintf("%s/%s", config.Name, mode), func(b *testing.B) {
				state, err := OpenGoMemoryState(b.TempDir(), config, NodeCacheConfig{Capacity: 1024})
				if err != nil {
					b.Fail()
				}
				defer state.Close()

				address := common.Address{}
				state.SetNonce(address, common.ToNonce(12))

				key := common.Key{}
				value := common.Value{}

				for i := 0; i < b.N; i++ {
					binary.BigEndian.PutUint64(key[:], uint64(i%1024))
					binary.BigEndian.PutUint64(value[:], uint64(i))
					state.SetStorage(address, key, value)
					if withHashing {
						state.GetHash()
					}
				}
			})
		}
	}
}

func TestReadCodes(t *testing.T) {
	var h1 common.Hash
	var h2 common.Hash
	var h3 common.Hash

	h1[0] = 0xAA
	h2[0] = 0xBB
	h3[0] = 0xCC

	h1[31] = 0xAA
	h2[31] = 0xBB
	h3[31] = 0xCC

	code1 := []byte{0xDD, 0xEE, 0xFF}
	code2 := []byte{0xDD, 0xEE}
	code3 := []byte{0xEE}

	var data []byte
	data = append(data, append(binary.BigEndian.AppendUint32(h1[:], uint32(len(code1))), code1...)...)
	data = append(data, append(binary.BigEndian.AppendUint32(h2[:], uint32(len(code2))), code2...)...)
	data = append(data, append(binary.BigEndian.AppendUint32(h3[:], uint32(len(code3))), code3...)...)

	reader := utils.NewChunkReader(data, 3)
	res, err := parseCodes(reader)
	if err != nil {
		t.Fatalf("should not fail: %s", err)
	}

	if code, exists := res[h1]; !exists || !bytes.Equal(code, code1) {
		t.Errorf("bytes do not match: %x != %x", code, code1)
	}

	if code, exists := res[h2]; !exists || !bytes.Equal(code, code2) {
		t.Errorf("bytes do not match: %x != %x", code, code1)
	}

	if code, exists := res[h3]; !exists || !bytes.Equal(code, code3) {
		t.Errorf("bytes do not match: %x != %x", code, code1)
	}
}

func TestState_tryMarkDirty_Fail_Access_Dir(t *testing.T) {
	if err := tryMarkDirty("abc"); err == nil {
		t.Errorf("marking the directory dirty should fail")
	}
	dir := path.Join(t.TempDir(), "read-only")
	if err := os.MkdirAll(dir, os.FileMode(0555)); err != nil {
		t.Fatalf("cannot create dir: %s", err)
	}
	if err := tryMarkDirty(dir); err == nil {
		t.Errorf("marking the directory dirty should fail")
	}
}

func TestState_OpenGoMemoryState_CannotWrite(t *testing.T) {
	for name, open := range stateFactories {
		t.Run(name, func(t *testing.T) {
			dir := path.Join(t.TempDir(), "read-only")
			if err := os.MkdirAll(dir, os.FileMode(0555)); err != nil {
				t.Fatalf("cannot create dir: %s", err)
			}
			if _, err := open(dir); err == nil {
				t.Errorf("opening a state should fail")
			}
		})
	}
}

func TestState_OpenGoMemoryState_Corrupted_Meta(t *testing.T) {
	for name, open := range mptStateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			// corrupt meta
			if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte("Hello, World!"), 0644); err != nil {
				t.Fatalf("cannot update meta: %v", err)
			}
			if _, err := open(dir); err == nil {
				t.Errorf("opening a state should fail")
			}
		})
	}
}

func TestState_StateModifications_Failing(t *testing.T) {
	for name, open := range mptStateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			state, err := open(dir)
			if err != nil {
				t.Fatalf("cannot open state: %s", err)
			}

			// inject failing stock to trigger an error applying the update
			var injectedErr = errors.New("injectedError")
			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().updateHashesFor(gomock.Any()).Return(common.Hash{}, nil, injectedErr).AnyTimes()
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).Return(AccountInfo{}, false, injectedErr).AnyTimes()
			db.EXPECT().SetAccountInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(NodeReference{}, injectedErr).AnyTimes()
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, injectedErr).AnyTimes()
			db.EXPECT().SetValue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(NodeReference{}, injectedErr).AnyTimes()
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Return(injectedErr)
			state.trie.forest = db

			if _, err := state.Exists(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if err := state.DeleteAccount(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetBalance(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if err := state.SetBalance(common.Address{1}, amount.New(1)); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetNonce(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if err := state.SetNonce(common.Address{1}, common.Nonce{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetStorage(common.Address{1}, common.Key{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if err := state.SetStorage(common.Address{1}, common.Key{1}, common.Value{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetCode(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if err := state.SetCode(common.Address{1}, make([]byte, 10)); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetCodeHash(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetCodeSize(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			if _, err := state.GetHash(); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			update := common.Update{}
			update.CreatedAccounts = []common.Address{{1}}
			if _, err := state.Apply(0, update); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
			nodeVisitor := NewMockNodeVisitor(ctrl)
			if err := state.Visit(nodeVisitor, false); !errors.Is(err, injectedErr) {
				t.Errorf("accessing data should fail")
			}
		})
	}
}

func TestState_HasEmptyStorage(t *testing.T) {
	for name, open := range mptStateFactories {
		t.Run(name, func(t *testing.T) {
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()

				state, err := open(dir)
				if err != nil {
					t.Fatalf("cannot open state: %s", err)
				}

				addr := common.Address{0x1}
				ctrl := gomock.NewController(t)
				db := NewMockDatabase(ctrl)
				db.EXPECT().HasEmptyStorage(gomock.Any(), addr)

				state.trie.forest = db
				state.HasEmptyStorage(addr)
			})
		})
	}
}

func TestState_StateModificationsWithoutErrorHaveExpectedEffects(t *testing.T) {
	for name, open := range mptStateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			state, err := open(dir)
			if err != nil {
				t.Fatalf("cannot open state: %s", err)
			}

			balance := amount.New(1)
			if err := state.SetBalance(common.Address{1}, balance); err != nil {
				t.Errorf("error to set balance: %s", err)
			}
			if exists, err := state.Exists(common.Address{1}); err != nil || !exists {
				t.Errorf("account should exist: %v err: %s", exists, err)
			}
			if got, err := state.GetBalance(common.Address{1}); err != nil || balance != got {
				t.Errorf("wrong balance: %v != %v err: %s", got, balance, err)
			}

			nonce := common.Nonce{1}
			if err := state.SetNonce(common.Address{1}, nonce); err != nil {
				t.Errorf("error to set nonce: %s", err)
			}
			if got, err := state.GetNonce(common.Address{1}); err != nil || got != nonce {
				t.Errorf("wrong nonce: %v != %v err: %s", got, nonce, err)
			}

			value := common.Value{1}
			if err := state.SetStorage(common.Address{1}, common.Key{1}, value); err != nil {
				t.Errorf("error to set value: %s", err)
			}
			if got, err := state.GetStorage(common.Address{1}, common.Key{1}); err != nil || got != value {
				t.Errorf("wrong value: %v != %v err: %s", got, value, err)
			}

			code := []byte{1, 2, 3, 4, 5, 6}
			if err := state.SetCode(common.Address{1}, code); err != nil {
				t.Errorf("error to set code: %s", err)
			}
			// no change to apply the same code twice
			if err := state.SetCode(common.Address{1}, code); err != nil {
				t.Errorf("error to set code: %s", err)
			}
			if got, err := state.GetCode(common.Address{1}); err != nil || !slices.Equal(got, code) {
				t.Errorf("wrong code: %v != %v, err: %s", got, code, err)
			}
			if got, err := state.GetCodeSize(common.Address{1}); err != nil || got != len(code) {
				t.Errorf("wrong code size: %v != %v, err: %s", got, len(code), err)
			}
			if got, err := state.GetCodeHash(common.Address{1}); err != nil || got != common.Keccak256(code) {
				t.Errorf("wrong code hash: %v != %v err: %s", got, common.Keccak256(code), err)
			}

			if err := state.DeleteAccount(common.Address{1}); err != nil {
				t.Errorf("error to access data: %s", err)
			}
			if exists, err := state.Exists(common.Address{1}); err != nil || exists {
				t.Errorf("account should not exist: %v err: %s", exists, err)
			}

			emptyBalance := amount.New()
			if got, err := state.GetBalance(common.Address{1}); err != nil || got != emptyBalance {
				t.Errorf("wrong balance: %v != %v err: %s", got, emptyBalance, err)
			}
			var emptyNonce common.Nonce
			if got, err := state.GetNonce(common.Address{1}); err != nil || got != emptyNonce {
				t.Errorf("wrong nonce: %v != %v err: %s", got, emptyNonce, err)
			}
			var emptyValue common.Value
			if got, err := state.GetStorage(common.Address{1}, common.Key{1}); err != nil || got != emptyValue {
				t.Errorf("wrong value: %v != %v err: %s", got, emptyValue, err)
			}
			if got, err := state.GetCode(common.Address{1}); err != nil || got != nil {
				t.Errorf("wrong code: %v != nil, err: %s", got, err)
			}
			if got, err := state.GetCodeSize(common.Address{1}); err != nil || got != 0 {
				t.Errorf("wrong code size: %v != 0, err: %s", got, err)
			}
			if got, err := state.GetCodeHash(common.Address{1}); err != nil || got != emptyCodeHash {
				t.Errorf("wrong code hash: %v != %v err: %s", got, emptyCodeHash, err)
			}
			// set non-existing empty code is noop
			if err := state.SetCode(common.Address{1}, make([]byte, 0)); err != nil {
				t.Errorf("error to set code: %s", err)
			}

			if _, err := state.GetHash(); err != nil {
				t.Errorf("error to get hash: %s", err)
			}

			update := common.Update{}
			if _, err := state.Apply(0, update); err != nil {
				t.Errorf("error to apply: %s", err)
			}
			if state.GetSnapshotableComponents() != nil {
				t.Errorf("not implemented method should return nil")
			}
			if err := state.RunPostRestoreTasks(); err != nil {
				t.Errorf("error to access data: %s", err)
			}
		})
	}
}

func TestMptState_GetRootId(t *testing.T) {
	for name, open := range mptStateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			state, err := open(dir)
			if err != nil {
				t.Fatalf("cannot open state: %s", err)
			}

			if got, want := state.GetRootId(), EmptyId(); got != want {
				t.Errorf("values do not match: got %v != want %v", got, want)
			}
		})
	}
}

func TestState_GetCodes(t *testing.T) {
	hasher := sha3.NewLegacyKeccak256()
	for name, open := range mptStateFactories {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			state, err := open(dir)
			if err != nil {
				t.Fatalf("cannot open state: %s", err)
			}

			const size = 1000
			for i := 1; i < size; i++ {
				var address common.Address
				code := make([]byte, i)
				address[i%20] = byte(i)
				code[i-1] = byte(i)
				if err := state.SetCode(address, code); err != nil {
					t.Fatalf("cannot set code: %s", err)
				}
			}

			codes := state.GetCodes()
			if got, want := len(codes), size-1; got != want {
				t.Errorf("sizes do not much: got: %d != want: %d", got, want)
			}
			for i := 1; i < size; i++ {
				code := make([]byte, i)
				code[i-1] = byte(i)
				if got, want := codes[common.GetHash(hasher, code)], code; !slices.Equal(got, want) {
					t.Errorf("codes do not match: got: %v != %v", got, want)
				}
			}
		})
	}
}

func TestState_ForestErrorIsReportedInFlushAndClose(t *testing.T) {

	dir := t.TempDir()
	state, err := OpenGoFileState(dir, S4LiveConfig, NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to open test state: %v", err)
	}

	injectedError := fmt.Errorf("injected error")
	ctrl := gomock.NewController(t)
	db := NewMockDatabase(ctrl)
	db.EXPECT().updateHashesFor(gomock.Any()).AnyTimes()
	db.EXPECT().Flush().AnyTimes()
	db.EXPECT().Close().AnyTimes()
	db.EXPECT().CheckErrors().Return(injectedError).Times(2)
	state.trie.forest = db

	if want, got := injectedError, state.Flush(); !errors.Is(got, want) {
		t.Errorf("missing forest error in Flush result, wanted %v, got %v", want, got)
	}
	if want, got := injectedError, state.Close(); !errors.Is(got, want) {
		t.Errorf("missing forest error in Close result, wanted %v, got %v", want, got)
	}
}

func TestState_Flush_WriteDirtyCodesOnly(t *testing.T) {
	dir := t.TempDir()
	state, err := OpenGoFileState(dir, S5LiveConfig, NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to open test state: %v", err)
	}
	if err := state.SetCode(common.Address{}, []byte{0x12}); err != nil {
		t.Errorf("SetCode failed: %v", err)
	}
	if err := state.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	stat1, err := os.Stat(dir + "/codes.dat")
	if err != nil {
		t.Errorf("failed to get modtime of codes file before second flush")
	}

	if err := state.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	stat2, err := os.Stat(dir + "/codes.dat")
	if err != nil {
		t.Errorf("failed to get modtime of codes file after second flush")
	}
	if stat1.ModTime() != stat2.ModTime() {
		t.Errorf("codes written even when not dirty")
	}

	if err := state.SetCode(common.Address{}, []byte{0x12, 0x34}); err != nil {
		t.Errorf("SetCode failed: %v", err)
	}
	if err := state.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	stat3, err := os.Stat(dir + "/codes.dat")
	if err != nil {
		t.Errorf("failed to get modtime of codes file after third flush")
	}
	if stat2.ModTime() == stat3.ModTime() {
		t.Errorf("codes not written when dirty")
	}
}

func TestEstimatePerNodeMemoryUsage(t *testing.T) {

	// Use the size of the largest node
	var maxNodeSize uintptr
	if cur := unsafe.Sizeof(BranchNode{}); cur > maxNodeSize {
		maxNodeSize = cur
	}
	if cur := unsafe.Sizeof(AccountNode{}); cur > maxNodeSize {
		maxNodeSize = cur
	}
	if cur := unsafe.Sizeof(ExtensionNode{}); cur > maxNodeSize {
		maxNodeSize = cur
	}
	if cur := unsafe.Sizeof(ValueNode{}); cur > maxNodeSize {
		maxNodeSize = cur
	}

	ownerSize := unsafe.Sizeof(nodeOwner{})
	indexEntrySize := unsafe.Sizeof(NodeId(0)) + unsafe.Sizeof(ownerPosition(0))
	sharedNodeSize := unsafe.Sizeof(shared.Shared[Node]{})

	staticSizes := ownerSize + indexEntrySize
	dynamicSizes := maxNodeSize + sharedNodeSize

	lowerLimit := staticSizes + dynamicSizes
	upperLimit := staticSizes + uintptr(float32(dynamicSizes)*1.2)

	got := EstimatePerNodeMemoryUsage()
	if got < int(lowerLimit) {
		t.Errorf("Estimated nodes size is too small, wanted at least %d, got %d", lowerLimit, got)
	}
	if got > int(upperLimit) {
		t.Errorf("Estimated nodes size is too big, wanted at most %d, got %d", upperLimit, got)
	}
}

func runFlushBenchmark(b *testing.B, config MptConfig, forceDirtyNodes bool) {
	numAccounts := 100_000
	state, err := OpenGoFileState(b.TempDir(), config, NodeCacheConfig{Capacity: numAccounts})
	if err != nil {
		b.Fatalf("failed to open state, err %v", err)
	}

	// Create some content in the MPT large enough to
	// fill the full node cache.
	addrs := getTestAddresses(numAccounts)
	for j, addr := range addrs {
		if j%(numAccounts/2) == 0 && j > 0 {
			if _, _, err = state.UpdateHashes(); err != nil {
				b.Fatalf("failed to update hashes: %v", err)
			}
		}
		err = state.CreateAccount(addr)
		if err != nil {
			b.Fatalf("failed to create account: %v", err)
		}
	}
	if _, _, err = state.UpdateHashes(); err != nil {
		b.Fatalf("failed to update hashes: %v", err)
	}

	// Add some codes to be flushed.
	for i := 0; i < numAccounts; i++ {
		state.codes.codes[common.Hash{byte(i >> 8), byte(i)}] = make([]byte, 100)
	}

	if err = state.Flush(); err != nil {
		b.Fatalf("failed to flush state: %v", err)
	}

	for i := 0; i < b.N; i++ {
		if forceDirtyNodes {
			// Mark all nodes in the cache as dirty.
			state.trie.forest.(*Forest).nodeCache.ForEach(func(id NodeId, node *shared.Shared[Node]) {
				handle := node.GetWriteHandle()
				switch node := handle.Get().(type) {
				case *BranchNode:
					node.clean = false
				case *ExtensionNode:
					node.clean = false
				case *ValueNode:
					node.clean = false
				case *AccountNode:
					node.clean = false
				}
				handle.Release()
			})
			if err := os.Remove(state.codes.file); err != nil {
				b.Fatalf("failed to remove codes file: %v", err)
			}
			state.codes.pending = maps.Keys(state.codes.codes)
		}
		if err = state.Flush(); err != nil {
			b.Fatalf("failed to flush state: %v", err)
		}
	}

	if err = state.Close(); err != nil {
		b.Fatalf("failed to close state: %v", err)
	}

}

// To run these benchmarks, use the following command:
// go test ./database/mpt -run none -bench BenchmarkMptState_ArchiveFlush --benchtime 10s

func BenchmarkMptState_ArchiveFlush(b *testing.B) {
	runFlushBenchmark(b, S5ArchiveConfig, false)
}

func BenchmarkMptState_ArchiveFlushForcedDirty(b *testing.B) {
	runFlushBenchmark(b, S5ArchiveConfig, true)
}
