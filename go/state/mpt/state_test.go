package mpt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

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
