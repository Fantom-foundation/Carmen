package mpt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/shadow"
	"github.com/Fantom-foundation/Carmen/go/common"
	"go.uber.org/mock/gomock"
)

var variants = []struct {
	name    string
	factory func(directory string, mptConfig MptConfig, forestConfig ForestConfig) (*Forest, error)
}{
	{"InMemory", OpenInMemoryForest},
	{"FileBased", OpenFileForest},
	{"FileShadow", openFileShadowForest},
}

var forestConfigs = map[string]ForestConfig{
	"mutable_1k":     {Mode: Mutable, CacheCapacity: 1024},
	"mutable_128k":   {Mode: Mutable, CacheCapacity: 128 * 1024},
	"immutable_1k":   {Mode: Immutable, CacheCapacity: 1024},
	"immutable_128k": {Mode: Immutable, CacheCapacity: 128 * 1024},
}

func TestForest_OpenAndClose(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}
					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_ClosedAndReOpened(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					addr := common.Address{1}
					info := AccountInfo{Nonce: common.Nonce{12}}

					root := NewNodeReference(EmptyId())
					root, err = forest.SetAccountInfo(&root, addr, info)
					if err != nil {
						t.Fatalf("failed to set account info: %v", err)
					}

					if _, _, err = forest.updateHashesFor(&root); err != nil {
						t.Fatalf("failed to update hash of modified forest: %v", err)
					}

					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}

					reopened, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to re-open forest: %v", err)
					}

					if got, found, err := reopened.GetAccountInfo(&root, addr); info != got || !found || err != nil {
						t.Fatalf("reopened forest does not contain expected value, wanted %v, got %v, found %t, err %v", info, got, found, err)
					}

					if err := reopened.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_ArchiveInfoCanBeSetAndRetrieved(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					addr := common.Address{1}
					info0 := AccountInfo{}
					info1 := AccountInfo{Nonce: common.Nonce{12}}

					root := NewNodeReference(EmptyId())
					if info, found, err := forest.GetAccountInfo(&root, addr); info != info0 || found || err != nil {
						t.Errorf("empty tree should not contain any info, wanted (%v,%t), got (%v,%t), err %v", info0, false, info, true, err)
					}

					root, err = forest.SetAccountInfo(&root, addr, info1)
					if err != nil {
						t.Fatalf("failed to set account info: %v", err)
					}

					if info, found, err := forest.GetAccountInfo(&root, addr); info != info1 || !found || err != nil {
						t.Errorf("empty tree should not contain any info, wanted (%v,%t), got (%v,%t), err %v", info1, true, info, true, err)
					}

					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Fatalf("failed to update hashes: %v", err)
					}

					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_ValueCanBeSetAndRetrieved(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					addr := common.Address{1}
					info := AccountInfo{Nonce: common.Nonce{12}}
					key := common.Key{12}
					value0 := common.Value{}
					value1 := common.Value{1}

					// Initially, the value is zero.
					root := NewNodeReference(EmptyId())
					if value, err := forest.GetValue(&root, addr, key); value != value0 || err != nil {
						t.Errorf("empty tree should not contain any info, wanted %v, got %v, err %v", value0, value, err)
					}

					// Setting it without an account does not have an effect.
					if newRoot, err := forest.SetValue(&root, addr, key, value1); newRoot != root || err != nil {
						t.Errorf("setting a value without an account should not change the root, wanted %v, got %v, err %v", root, newRoot, err)
					}

					// Setting the value of an existing account should have an effect.
					root, err = forest.SetAccountInfo(&root, addr, info)
					if err != nil {
						t.Fatalf("failed to create an account: %v", err)
					}

					if root, err = forest.SetValue(&root, addr, key, value1); err != nil {
						t.Errorf("setting a value failed: %v", err)
					}

					if value, err := forest.GetValue(&root, addr, key); value != value1 || err != nil {
						t.Errorf("value should be contained now, wanted %v, got %v, err %v", value1, value, err)
					}

					if err := forest.Check(&root); err != nil {
						t.Errorf("inconsistent trie: %v", err)
					}

					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Errorf("failed to update hash for root")
					}

					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_TreesCanBeHashedAndNavigatedInParallel(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					addr := common.Address{1}
					info := AccountInfo{Nonce: common.Nonce{12}}
					key := common.Key{12}
					value := common.Value{1}

					root := NewNodeReference(EmptyId())
					root, err = forest.SetAccountInfo(&root, addr, info)
					if err != nil {
						t.Fatalf("failed to create an account: %v", err)
					}

					if root, err = forest.SetValue(&root, addr, key, value); err != nil {
						t.Errorf("setting a value failed: %v", err)
					}

					// Acquire read access on the forest root.
					read, err := forest.getReadAccess(&root)
					if err != nil {
						t.Fatalf("failed to acquire read access on the root node")
					}

					// While holding read access on the root, hashing should be supported.
					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Errorf("failed to update hash for root")
					}

					read.Release()

					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_InLiveModeHistoryIsOverridden(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
				forest, err := variant.factory(t.TempDir(), config, ForestConfig{Mode: Mutable, CacheCapacity: 1024})
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}
				defer forest.Close()

				addr := common.Address{1}
				info1 := AccountInfo{Nonce: common.Nonce{12}}
				info2 := AccountInfo{Nonce: common.Nonce{14}}

				// Initially, the value is zero.
				root0 := NewNodeReference(EmptyId())

				// Update the account info in two steps.
				root1, err := forest.SetAccountInfo(&root0, addr, info1)
				if err != nil {
					t.Fatalf("failed to create an account: %v", err)
				}

				root2, err := forest.SetAccountInfo(&root1, addr, info2)
				if err != nil {
					t.Fatalf("failed to create an account: %v", err)
				}

				// The second update should have not introduced a new root.
				if root1 != root2 {
					t.Errorf("expected same root, got %v and %v", root1, root2)
				}
				if info, found, err := forest.GetAccountInfo(&root1, addr); info != info2 || !found || err != nil {
					t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info2, info, found, err)
				}

				if _, _, err := forest.updateHashesFor(&root2); err != nil {
					t.Fatalf("failed to update hashes: %v", err)
				}
			})
		}
	}
}

func TestForest_InArchiveModeHistoryIsPreserved(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
				forest, err := variant.factory(t.TempDir(), config, ForestConfig{Mode: Immutable, CacheCapacity: 1024})
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}
				defer forest.Close()

				addr := common.Address{1}
				info1 := AccountInfo{Nonce: common.Nonce{12}}
				info2 := AccountInfo{Nonce: common.Nonce{14}}

				// Initially, the value is zero.
				root0 := NewNodeReference(EmptyId())
				if err := forest.Freeze(&root0); err != nil {
					t.Errorf("failed to freeze root0: %v", err)
				}

				// Update the account info in two steps.
				root1, err := forest.SetAccountInfo(&root0, addr, info1)
				if err != nil {
					t.Fatalf("failed to create an account: %v", err)
				}
				if err := forest.Freeze(&root1); err != nil {
					t.Errorf("failed to freeze root1: %v", err)
				}

				root2, err := forest.SetAccountInfo(&root1, addr, info2)
				if err != nil {
					t.Fatalf("failed to create an account: %v", err)
				}
				if err := forest.Freeze(&root2); err != nil {
					t.Errorf("failed to freeze root2: %v", err)
				}

				// All versions should still be accessible.
				if info, found, err := forest.GetAccountInfo(&root1, addr); info != info1 || !found || err != nil {
					t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info1, info, found, err)
				}
				if info, found, err := forest.GetAccountInfo(&root1, addr); info != info1 || !found || err != nil {
					t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info1, info, found, err)
				}
				if info, found, err := forest.GetAccountInfo(&root2, addr); info != info2 || !found || err != nil {
					t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info2, info, found, err)
				}

				for _, root := range []NodeReference{root0, root1, root2} {
					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Fatalf("failed to update hashes: %v", err)
					}
				}
			})
		}
	}
}

func TestForest_ProvidesMemoryFoodPrint(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}
					defer forest.Close()

					footprint := forest.GetMemoryFootprint()
					if footprint.Total() <= uintptr(0) {
						t.Errorf("memory footprint not provided")
					}

					for _, memChild := range []string{"accounts", "branches", "extensions", "values", "cache",
						"hashedKeysCache", "hashedAddressesCache"} {
						if footprint.GetChild(memChild) == nil {
							t.Errorf("memory footprint not provided: %v\ngot: %v", memChild, footprint)
						}
					}
				})
			}
		}
	}
}

func TestForest_ConcurrentReadsAreRaceFree(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					const N = 100
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// Fill in some data (sequentially).
					root := NewNodeReference(EmptyId())
					for i := 0; i < N; i++ {
						root, err = forest.SetAccountInfo(&root, common.Address{byte(i)}, AccountInfo{Nonce: common.ToNonce(uint64(i + 1))})
						if err != nil {
							t.Fatalf("failed to insert account %d: %v", i, err)
						}
					}

					// Read account information concurrently.
					var errors [N]error
					var wg sync.WaitGroup
					wg.Add(N)
					for i := 0; i < N; i++ {
						go func() {
							defer wg.Done()
							for i := 0; i < N; i++ {
								info, _, err := forest.GetAccountInfo(&root, common.Address{byte(i)})
								if err != nil {
									errors[i] = err
									return
								}
								if got, want := info.Nonce.ToUint64(), uint64(i+1); got != want {
									errors[i] = fmt.Errorf("unexpected nonce for account %d: wanted %d, got %d", i, want, got)
									return
								}
							}
						}()
					}
					wg.Wait()

					for i, err := range errors {
						if err != nil {
							t.Errorf("error in goroutine %d: %v", i, err)
						}
					}

					// Update hashes to avoid writing dirty hashes during close.
					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Fatalf("failed to get hash for forest content, err %v", err)
					}
					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_ConcurrentWritesAreRaceFree(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					const N = 100
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// Fill in some data (sequentially).
					root := NewNodeReference(EmptyId())
					for i := 0; i < N; i++ {
						root, err = forest.SetAccountInfo(&root, common.Address{byte(i)}, AccountInfo{Nonce: common.ToNonce(uint64(i + 1))})
						if err != nil {
							t.Fatalf("failed to insert account %d: %v", i, err)
						}
					}

					// Update account information concurrently.
					var errors [N]error
					var wg sync.WaitGroup
					wg.Add(N)
					for i := 0; i < N; i++ {
						go func() {
							defer wg.Done()
							for i := 0; i < N; i++ {
								_, err := forest.SetAccountInfo(&root, common.Address{byte(i)}, AccountInfo{Nonce: common.ToNonce(uint64(i + 2))})
								if err != nil {
									errors[i] = err
									return
								}
							}
						}()
					}
					wg.Wait()

					for i, err := range errors {
						if err != nil {
							t.Errorf("error in goroutine %d: %v", i, err)
						}
					}

					// Check that the resulting nonce is i + 2
					for i := 0; i < N; i++ {
						info, exits, err := forest.GetAccountInfo(&root, common.Address{byte(i)})
						if err != nil {
							t.Fatalf("failed to read account %d: %v", i, err)
						}
						if !exits {
							t.Errorf("account %d should exist", i)
						}
						if got, want := info.Nonce.ToUint64(), uint64(i+2); got != want {
							t.Errorf("invalid final account state, wanted %d, got %d", want, got)
						}
					}

					// Update hashes to avoid writing dirty hashes during close.
					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Fatalf("failed to get hash for forest content, err %v", err)
					}
					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		}
	}
}

func TestForest_ReleaserReleasesNodesOnlyOnce(t *testing.T) {
	ctrl := gomock.NewController(t)

	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
	accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
	values := stock.NewMockStock[uint64, ValueNode](ctrl)

	forest, err := makeForest(
		MptConfig{Hashing: DirectHashing},
		t.TempDir(),
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create test forest: %v", err)
	}

	gomock.InOrder(
		accounts.EXPECT().New().Return(uint64(1), nil),
		values.EXPECT().New().Return(uint64(1), nil),
	)
	accounts.EXPECT().Delete(uint64(1))
	values.EXPECT().Delete(uint64(1))

	branches.EXPECT().Flush()
	extensions.EXPECT().Flush()
	accounts.EXPECT().Flush()
	values.EXPECT().Flush()

	branches.EXPECT().Close()
	extensions.EXPECT().Close()
	accounts.EXPECT().Close()
	values.EXPECT().Close()

	// Create an account with some storage.
	addr := common.Address{}
	root := NewNodeReference(EmptyId())
	root, err = forest.SetAccountInfo(&root, addr, AccountInfo{Nonce: common.ToNonce(12)})
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}

	root, err = forest.SetValue(&root, addr, common.Key{}, common.Value{1})
	if err != nil {
		t.Fatalf("failed to set value: %v", err)
	}

	// Deleting the account should free the storage -` Accounts.Delete()` and `Values.Delete()` get called.
	root, err = forest.SetAccountInfo(&root, addr, AccountInfo{})
	if err != nil {
		t.Fatalf("failed to delete account: %v", err)
	}

	if err = forest.Close(); err != nil {
		t.Fatalf("failed to close the forest: %v", err)
	}
}

func TestForest_WriteBufferRecoveryIsThreadSafe(t *testing.T) {
	testForest_WriteBufferRecoveryIsThreadSafe(t, false)
}

func TestForest_WriteBufferRecoveryIsThreadSafeWithConcurrentNodeCreation(t *testing.T) {
	testForest_WriteBufferRecoveryIsThreadSafe(t, true)
}

func testForest_WriteBufferRecoveryIsThreadSafe(t *testing.T, withConcurrentNodeGeneration bool) {
	// This test stress-tests the code recovering nodes from the write buffer.
	// To that end, it creates a forest with a node cache of only one node.
	// In this forest, two trees are placed, and then 10 workers are used
	// to concurrently update those trees, mutually pushing nodes out of
	// the cache and recovering it.
	// This test reproduces issue
	// https://github.com/Fantom-foundation/Carmen/issues/687
	//
	// Another issue related to this was reported by
	// https://github.com/Fantom-foundation/Carmen/issues/709
	// In this case, parallel to the swapping of nodes between the write buffer
	// and the cache, new nodes are created concurrently. Due to a
	// synchronization issue in the cache this lead to a panic indicating that
	// the node retrieved from the buffer could not be correctly restored.

	ctrl := gomock.NewController(t)

	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
	accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
	values := stock.NewMockStock[uint64, ValueNode](ctrl)

	branches.EXPECT().Flush()
	branches.EXPECT().Close()
	extensions.EXPECT().Flush()
	extensions.EXPECT().Close()
	accounts.EXPECT().Flush()
	accounts.EXPECT().Close()
	values.EXPECT().Flush()
	values.EXPECT().Close()

	forest, err := makeForest(
		MptConfig{Hashing: DirectHashing},
		t.TempDir(),
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{CacheCapacity: 1},
	)
	if err != nil {
		t.Fatalf("failed to create test forest: %v", err)
	}

	var counter atomic.Uint64
	accounts.EXPECT().New().AnyTimes().DoAndReturn(func() (uint64, error) {
		return counter.Add(1) - 1, nil
	})
	accounts.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(func(i uint64) (AccountNode, error) {
		return AccountNode{address: common.Address{byte(i)}, info: AccountInfo{Nonce: common.Nonce{byte(i)}}}, nil
	})

	// We need to set expectations for output operations to avoid a panic in the node writer
	// goroutine which causes a deadlock since held locks are not properly released while resolving
	// the panic triggered by a missing expectation.
	accounts.EXPECT().Set(gomock.Any(), gomock.Any()).AnyTimes()

	empty := NewNodeReference(EmptyId())
	treeA, err := forest.SetAccountInfo(&empty, common.Address{0}, AccountInfo{Nonce: common.Nonce{1}})
	if err != nil {
		t.Fatalf("failed to create tree A: %v", err)
	}

	treeB, err := forest.SetAccountInfo(&empty, common.Address{1}, AccountInfo{Nonce: common.Nonce{2}})
	if err != nil {
		t.Fatalf("failed to create tree B: %v", err)
	}

	const N = 10
	const M = 1000
	var wg sync.WaitGroup
	wg.Add(N)

	if withConcurrentNodeGeneration {
		// Optionally new nodes are generated in parallel to the reloading of
		// the root nodes of the forest.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < M; i++ {
				_, handle, err := forest.createAccount()
				if err != nil {
					t.Errorf("failed to create new node: %v", err)
				} else {
					handle.Release()
				}
			}
		}()
	}

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < M; i++ {
				tree := treeA
				if i%2 > 0 {
					tree = treeB
				}
				_, err := forest.SetAccountInfo(&tree, common.Address{byte(i % 2)}, AccountInfo{Nonce: common.Nonce{byte(i%200 + 2)}})
				if err != nil {
					t.Errorf("failed to update trie: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	if err := forest.Close(); err != nil {
		t.Errorf("failed to close forest: %v", err)
	}
}

// openFileShadowForest creates a forest instance based on a shadowed file
// based stock implementation for stress-testing the file based stock. This
// is mainly intended to detect implementation issues in the caching and
// synchronization features of the file-based stock which have caused problems
// in the past.
func openFileShadowForest(directory string, mptConfig MptConfig, forestConfig ForestConfig) (*Forest, error) {
	accountEncoder, branchEncoder, extensionEncoder, valueEncoder := getEncoder(mptConfig)
	branchesA, err := file.OpenStock[uint64, BranchNode](branchEncoder, directory+"/A/branches")
	if err != nil {
		return nil, err
	}
	extensionsA, err := file.OpenStock[uint64, ExtensionNode](extensionEncoder, directory+"/A/extensions")
	if err != nil {
		return nil, err
	}
	accountsA, err := file.OpenStock[uint64, AccountNode](accountEncoder, directory+"/A/accounts")
	if err != nil {
		return nil, err
	}
	valuesA, err := file.OpenStock[uint64, ValueNode](valueEncoder, directory+"/A/values")
	if err != nil {
		return nil, err
	}
	branchesB, err := memory.OpenStock[uint64, BranchNode](branchEncoder, directory+"/B/branches")
	if err != nil {
		return nil, err
	}
	extensionsB, err := memory.OpenStock[uint64, ExtensionNode](extensionEncoder, directory+"/B/extensions")
	if err != nil {
		return nil, err
	}
	accountsB, err := memory.OpenStock[uint64, AccountNode](accountEncoder, directory+"/B/accounts")
	if err != nil {
		return nil, err
	}
	valuesB, err := memory.OpenStock[uint64, ValueNode](valueEncoder, directory+"/B/values")
	if err != nil {
		return nil, err
	}
	branches := shadow.MakeShadowStock(branchesA, branchesB)
	extensions := shadow.MakeShadowStock(extensionsA, extensionsB)
	accounts := shadow.MakeShadowStock(accountsA, accountsB)
	values := shadow.MakeShadowStock(valuesA, valuesB)
	return makeForest(mptConfig, directory, branches, extensions, accounts, values, forestConfig)
}

func TestForest_NodeHandlingDoesNotDeadlock(t *testing.T) {
	// This test used to trigger a bug leading to a deadlock as reported in
	// https://github.com/Fantom-foundation/Carmen/issues/724
	//
	// It runs a stress test on the forest's node management code with a
	// particular focus on the transitioning of nodes between a tiny cache
	// and the write buffer. To increase the likelihood of the issue to
	// occur, the size of the channel used to transfer nodes from the cache
	// to the write buffer is reduced to 1.
	//
	// For a full description of the identified deadlock see the comments of
	// issue https://github.com/Fantom-foundation/Carmen/issues/724

	ctrl := gomock.NewController(t)

	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
	accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
	values := stock.NewMockStock[uint64, ValueNode](ctrl)

	branches.EXPECT().Flush()
	branches.EXPECT().Close()
	extensions.EXPECT().Flush()
	extensions.EXPECT().Close()
	accounts.EXPECT().Flush()
	accounts.EXPECT().Close()
	values.EXPECT().Flush()
	values.EXPECT().Close()

	forest, err := makeForest(
		MptConfig{Hashing: DirectHashing},
		t.TempDir(),
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{
			CacheCapacity:          1,
			writeBufferChannelSize: 1,
		},
	)
	if err != nil {
		t.Fatalf("failed to create test forest: %v", err)
	}

	var counter atomic.Uint64
	accounts.EXPECT().New().AnyTimes().DoAndReturn(func() (uint64, error) {
		return counter.Add(1) - 1, nil
	})
	accounts.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(func(i uint64) (AccountNode, error) {
		return AccountNode{address: common.Address{byte(i)}, info: AccountInfo{Nonce: common.Nonce{byte(i)}}}, nil
	})

	// We need to set expectations for output operations to avoid a panic in the node writer
	// goroutine which causes a deadlock since held locks are not properly released while resolving
	// the panic triggered by a missing expectation.
	accounts.EXPECT().Set(gomock.Any(), gomock.Any()).AnyTimes()

	empty := NewNodeReference(EmptyId())
	treeA, err := forest.SetAccountInfo(&empty, common.Address{0}, AccountInfo{Nonce: common.Nonce{1}})
	if err != nil {
		t.Fatalf("failed to create tree A: %v", err)
	}

	treeB, err := forest.SetAccountInfo(&empty, common.Address{1}, AccountInfo{Nonce: common.Nonce{2}})
	if err != nil {
		t.Fatalf("failed to create tree B: %v", err)
	}

	const N = 10
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				tree := treeA
				if i%2 > 0 {
					tree = treeB
				}
				_, err := forest.SetAccountInfo(&tree, common.Address{byte(i % 2)}, AccountInfo{Nonce: common.Nonce{byte(i%200 + 2)}})
				if err != nil {
					t.Errorf("failed to update trie: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	if err := forest.Close(); err != nil {
		t.Errorf("failed to close forest: %v", err)
	}
}
