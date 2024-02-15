package hashtree_test

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	branchingFactors = []int{2, 4, 8, 16, 32, 64, 128, 258, 512, 1024}
	initialSizes     = []int{1 << 20, 1 << 24, 1 << 30} // 2 exp 20 * 32B key = 32 MB data;  2 exp 30 * 32B key = 32 GB data
	updateSizes      = []int{100}

	sinkHash common.Hash
)

func BenchmarkHashTree(b *testing.B) {
	for _, fac := range initHashTreeFactories() {
		for _, factor := range branchingFactors {
			for _, initialSize := range initialSizes {
				s := fac(factor, b.TempDir())

				// initialize data
				b.StopTimer()
				for i := 0; i < initialSize; i++ {
					s.hashTree.MarkUpdated(i)
				}
				var err error
				sinkHash, err = s.hashTree.HashRoot()
				if err != nil {
					b.Fatalf("error: %s", err)
				}
				b.StartTimer()

				for _, dist := range common.GetDistributions(initialSize) {
					for _, updateSize := range updateSizes {
						b.Run(fmt.Sprintf("HashTree %s initialSize %d factor %d updateSize %d dist %s", s.name, initialSize, factor, updateSize, dist.Label), func(b *testing.B) {
							for i := 0; i < b.N; i++ {

								// update random pages based on distribution
								b.StopTimer()
								for i := 0; i < updateSize; i++ {
									s.hashTree.MarkUpdated(int(dist.GetNext()))
								}
								b.StartTimer()

								// compute the hash for updated elements
								var err error
								sinkHash, err = s.hashTree.HashRoot()
								if err != nil {
									b.Fatalf("error: %s", err)
								}
							}
						})
					}
				}
				_ = s.Close()
			}
		}
	}
}

// memSingleItemPage represents a single in-memory page
type memSingleItemPage struct{}

// newMemSingleItemPage inits the test page with a single fixed size array
func newMemSingleItemPage() *memSingleItemPage {
	return &memSingleItemPage{}
}

type dummyPageBytes [1 << 12]byte // 4kB Page
var dummyPage = dummyPageBytes{}

func (m *memSingleItemPage) GetPage(int) ([]byte, error) {
	// return the same data for every page
	return dummyPage[:], nil
}

type hashTreeFunc func(branchingFactor int, path string) *closeableHashTree

func initHashTreeFactories() []hashTreeFunc {
	return []hashTreeFunc{
		func(branchingFactor int, path string) *closeableHashTree {
			return &closeableHashTree{"memory", htmemory.NewHashTree(branchingFactor, newMemSingleItemPage()), func() error { return nil }}
		},
		func(branchingFactor int, path string) *closeableHashTree {
			return &closeableHashTree{"file", htfile.NewHashTree(path, branchingFactor, newMemSingleItemPage()), func() error { return nil }}

		},
		func(branchingFactor int, path string) *closeableHashTree {
			db, err := backend.OpenLevelDb(path, nil)
			cleanup := func() error {
				if err != nil {
					err = db.Close()
				}
				return err
			}
			return &closeableHashTree{"ldb", htldb.NewHashTree(db, backend.AccountStoreKey, branchingFactor, newMemSingleItemPage()), cleanup}

		},
	}
}

type closeableHashTree struct {
	name     string
	hashTree hashtree.HashTree
	close    func() error
}

func (m closeableHashTree) Close() error {
	return m.close()
}
