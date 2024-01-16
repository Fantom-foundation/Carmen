package mpt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

var stateFactories = map[string]func(string) (io.Closer, error){
	"memory":  func(dir string) (io.Closer, error) { return OpenGoMemoryState(dir, S5LiveConfig, 1024) },
	"file":    func(dir string) (io.Closer, error) { return OpenGoFileState(dir, S5LiveConfig, 1024) },
	"archive": func(dir string) (io.Closer, error) { return OpenArchiveTrie(dir, S5ArchiveConfig, 1024) },
	"verify":  func(dir string) (io.Closer, error) { return openVerificationNodeSource(dir, S5LiveConfig) },
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
	state, err := OpenGoFileState(dir, S5LiveConfig, 1024)
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
	state, err := OpenGoFileState(dir, S5LiveConfig, 1024)
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
				state, err := OpenGoMemoryState(b.TempDir(), config, 1024)
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
