package mpt

import (
	"encoding/binary"
	"fmt"
	"testing"

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
				state, err := OpenGoMemoryState(b.TempDir(), config)
				if err != nil {
					b.Fail()
				}

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
