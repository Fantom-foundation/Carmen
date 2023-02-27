package archive_test

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

const (
	bmAddressToCreate                = 100
	bmBlocksToInsert                 = 1_000
	bmAddressToUseParBlock           = 20
	bmKeysToInsertParAddressAndBlock = 50
)

func BenchmarkAdding(b *testing.B) {
	for _, factory := range getArchiveFactories(b) {
		a := factory.getArchive(b.TempDir())
		defer a.Close()

		// initialize
		var update common.Update
		for i := byte(0); i < byte(bmAddressToCreate); i++ {
			update.AppendCreateAccount(common.Address{i})
			update.AppendBalanceUpdate(common.Address{i}, common.Balance{i})
		}
		if err := a.Add(1, update); err != nil {
			b.Fatalf("failed to add block; %s", err)
		}

		var block uint64 = 2
		b.Run(factory.label, func(b *testing.B) {
			for i := 0; i < bmBlocksToInsert; i++ {
				var update common.Update
				for addrIt := 0; addrIt < bmAddressToUseParBlock; addrIt++ {
					addr := byte(rand.Intn(bmAddressToCreate))
					for keyIt := 0; keyIt < bmKeysToInsertParAddressAndBlock; keyIt++ {
						key := byte(rand.Intn(0xFF))
						update.AppendSlotUpdate(common.Address{addr}, common.Key{key}, common.Value{addr + key})
					}
				}
				if err := update.Normalize(); err != nil {
					b.Fatalf("failed to normalize update; %s", err)
				}
				if err := a.Add(block, update); err != nil {
					b.Fatalf("failed to add block; %s", err)
				}
				block++
			}
			// add flush here if parallel archives are implemented
		})
	}
}
