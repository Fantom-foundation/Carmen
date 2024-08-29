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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/shadow"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"go.uber.org/mock/gomock"
)

type variant struct {
	name    string
	factory func(directory string, mptConfig MptConfig, forestConfig ForestConfig) (*Forest, error)
}

var variants = []variant{
	{"InMemory", OpenInMemoryForest},
	{"FileBased", OpenFileForest},
	{"FileShadow", openFileShadowForest},
}

var fileAndMemVariants = []variant{
	{"InMemory", OpenInMemoryForest},
	{"FileBased", OpenFileForest},
}

var forestConfigs = map[string]ForestConfig{
	"mutable_1k":     {Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}},
	"mutable_128k":   {Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 128 * 1024}},
	"immutable_1k":   {Mode: Immutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}},
	"immutable_128k": {Mode: Immutable, NodeCacheConfig: NodeCacheConfig{Capacity: 128 * 1024}},
}

func TestForest_Cannot_Open_Corrupted_Stock_Meta(t *testing.T) {
	for _, variant := range fileAndMemVariants {
		for _, config := range allMptConfigs {
			for _, stockDir := range []string{"branches", "extensions", "accounts", "values"} {
				t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
					rootDir := t.TempDir()
					dir := filepath.Join(rootDir, stockDir)
					// Corrupt metadata to prevent opening
					if err := os.MkdirAll(dir, 0744); err != nil {
						t.Fatalf("cannot prepare for test: %s", err)
					}
					if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte("Hello"), 0644); err != nil {
						t.Fatalf("cannot prepare for test: %s", err)
					}

					if _, err := variant.factory(rootDir, config, ForestConfig{Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}}); err == nil {
						t.Errorf("opening forest should fail")
					}
				})
			}
		}
	}
}

func TestForest_Cannot_Open_Corrupted_Forest_Meta(t *testing.T) {
	for _, variant := range fileAndMemVariants {
		for _, config := range allMptConfigs {
			t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte("Hello"), 0644); err != nil {
					t.Fatalf("cannot prepare for test: %s", err)
				}

				if _, err := variant.factory(dir, config, ForestConfig{Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}}); err == nil {
					t.Errorf("opening forest should fail")
				}
			})
		}
	}
}

func TestForest_Cannot_Open_Cannot_Parse_Meta(t *testing.T) {
	for _, variant := range fileAndMemVariants {
		for _, config := range allMptConfigs {
			t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
				dir := t.TempDir()
				meta := "{\"Configuration\":\"S4-Live\",\"Mutable\":THIS_IS_NOT_BOOLEAN}"
				if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte(meta), 0644); err != nil {
					t.Fatalf("cannot prepare for test: %s", err)
				}

				if _, err := variant.factory(dir, config, ForestConfig{Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}}); err == nil {
					t.Errorf("opening forest should fail")
				}
			})
		}
	}
}

func TestForest_Cannot_Open_Meta_DoesNot_Match(t *testing.T) {
	for _, variant := range fileAndMemVariants {
		t.Run(fmt.Sprintf("%s", variant.name), func(t *testing.T) {
			dir := t.TempDir()
			meta := "{\"Configuration\":\"S4-Live\",\"Mutable\":true}"
			if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte(meta), 0644); err != nil {
				t.Fatalf("cannot prepare for test: %s", err)
			}

			if _, err := variant.factory(dir, S5LiveConfig, ForestConfig{Mode: Mutable}); err == nil {
				t.Errorf("opening forest should fail")
			}
			if _, err := variant.factory(dir, S4LiveConfig, ForestConfig{Mode: Immutable}); err == nil {
				t.Errorf("opening forest should fail")
			}
		})
	}
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

func TestForest_GettingAccountInfo_Fails(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to get value from stock")
					ctrl := gomock.NewController(t)
					stock := stock.NewMockStock[uint64, AccountNode](ctrl)
					stock.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, injectedErr)
					forest.accounts = stock
					root := NewNodeReference(AccountId(123))

					if _, err := forest.SetAccountInfo(&root, common.Address{1}, AccountInfo{}); !errors.Is(err, injectedErr) {
						t.Errorf("setting account should fail")
					}
					if _, _, err := forest.GetAccountInfo(&root, common.Address{1}); !errors.Is(err, injectedErr) {
						t.Errorf("getting account should fail")
					}
					if _, err := forest.SetValue(&root, common.Address{1}, common.Key{2}, common.Value{}); !errors.Is(err, injectedErr) {
						t.Errorf("setting value should fail")
					}
					if _, err := forest.GetValue(&root, common.Address{1}, common.Key{2}); !errors.Is(err, injectedErr) {
						t.Errorf("getting value should fail")
					}
					if _, err := forest.ClearStorage(&root, common.Address{1}); !errors.Is(err, injectedErr) {
						t.Errorf("getting account should fail")
					}
					nodeVisitor := NewMockNodeVisitor(ctrl)
					if err := forest.VisitTrie(&root, nodeVisitor); !errors.Is(err, injectedErr) {
						t.Errorf("getting account should fail")
					}
				})
			}
		}
	}
}

func TestForest_CreatingAccountInfo_Fails(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call New")
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, nil)
					accounts.EXPECT().New().AnyTimes().Return(uint64(0), injectedErr)

					values := stock.NewMockStock[uint64, ValueNode](ctrl)
					values.EXPECT().Get(gomock.Any()).AnyTimes().Return(ValueNode{}, nil)
					values.EXPECT().New().AnyTimes().Return(uint64(0), injectedErr)

					forest.accounts = accounts
					forest.values = values
					root := NewNodeReference(AccountId(123))

					if _, err := forest.SetAccountInfo(&root, common.Address{1}, AccountInfo{Nonce: common.Nonce{1}}); !errors.Is(err, injectedErr) {
						t.Errorf("setting account should fail")
					}
					if _, err := forest.SetValue(&root, common.Address{}, common.Key{2}, common.Value{1}); !errors.Is(err, injectedErr) {
						t.Errorf("setting value should fail")
					}
				})
			}
		}
	}
}

func TestForest_setHashesFor_Getting_Node_Fails(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call Get")
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, injectedErr)
					forest.accounts = accounts
					root := NewNodeReference(AccountId(123))

					hashes := NodeHashes{make([]NodeHash, 1)}
					if err := forest.setHashesFor(&root, &hashes); !errors.Is(err, injectedErr) {
						t.Errorf("setting hashes should fail")
					}
				})
			}
		}
	}
}

func TestForest_Freeze_Fails(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
				directory := t.TempDir()

				forest, err := variant.factory(directory, config, ForestConfig{Mode: Immutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}})
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}

				// inject failing stock to trigger an error applying the update
				var injectedErr = errors.New("failed to call Get")
				ctrl := gomock.NewController(t)
				accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
				accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, injectedErr)
				forest.accounts = accounts
				root := NewNodeReference(AccountId(123))

				if err := forest.Freeze(&root); !errors.Is(err, injectedErr) {
					t.Errorf("freezing node should fail")
				}
			})
		}
	}
}

func TestForest_CreatingNodes_Fails(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call New")
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, nil)
					accounts.EXPECT().New().AnyTimes().Return(uint64(0), injectedErr)

					values := stock.NewMockStock[uint64, ValueNode](ctrl)
					values.EXPECT().Get(gomock.Any()).AnyTimes().Return(ValueNode{}, nil)
					values.EXPECT().New().AnyTimes().Return(uint64(0), injectedErr)

					branches := stock.NewMockStock[uint64, BranchNode](ctrl)
					branches.EXPECT().Get(gomock.Any()).AnyTimes().Return(BranchNode{}, nil)
					branches.EXPECT().New().AnyTimes().Return(uint64(0), injectedErr)

					extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
					extensions.EXPECT().Get(gomock.Any()).AnyTimes().Return(ExtensionNode{}, nil)
					extensions.EXPECT().New().AnyTimes().Return(uint64(0), injectedErr)

					forest.accounts = accounts
					forest.values = values
					forest.branches = branches
					forest.extensions = extensions

					if _, _, err := forest.createAccount(); !errors.Is(err, injectedErr) {
						t.Errorf("creating node should fail")
					}
					if _, _, err := forest.createValue(); !errors.Is(err, injectedErr) {
						t.Errorf("creating node should fail")
					}
					if _, _, err := forest.createBranch(); !errors.Is(err, injectedErr) {
						t.Errorf("creating node should fail")
					}
					if _, _, err := forest.createExtension(); !errors.Is(err, injectedErr) {
						t.Errorf("creating node should fail")
					}
				})
			}
		}
	}
}

func TestForest_Cannot_Release_Node(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// empty ID cannot be released
					ref := NewNodeReference(EmptyId())
					if err := forest.release(&ref); err == nil {
						t.Errorf("creating node should fail")
					}
				})
			}
		}
	}
}

func TestForest_Release_Queue_Error_Get_Node(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call Get")
					ctrl := gomock.NewController(t)
					values := stock.NewMockStock[uint64, ValueNode](ctrl)
					// first call will succeed on getting the node but fails on releasing it
					values.EXPECT().Get(gomock.Any()).AnyTimes().Return(ValueNode{}, injectedErr)
					forest.values = values

					forest.releaseQueue <- ValueId(456)
					<-forest.releaseDone

					if err := forest.collectReleaseWorkerErrors(); !errors.Is(err, injectedErr) {
						t.Errorf("error should be produced from the release queue")
					}
				})
			}
		}
	}
}

func TestForest_Release_Queue_Error_Release_Node(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			t.Run(fmt.Sprintf("%s-%s", variant.name, config.Name), func(t *testing.T) {
				directory := t.TempDir()

				forest, err := variant.factory(directory, config, ForestConfig{Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}})
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}

				// inject failing stock to trigger an error applying the update
				var injectedErr = errors.New("failed to call Delete")
				ctrl := gomock.NewController(t)
				values := stock.NewMockStock[uint64, ValueNode](ctrl)
				// first call will succeed on getting the node but fails on releasing it
				values.EXPECT().Get(gomock.Any()).AnyTimes().Return(ValueNode{}, nil)
				values.EXPECT().Delete(gomock.Any()).AnyTimes().Return(injectedErr)
				forest.values = values

				forest.releaseQueue <- ValueId(456)
				<-forest.releaseDone

				if err := forest.collectReleaseWorkerErrors(); !errors.Is(err, injectedErr) {
					t.Errorf("error should be produced from the release queue")
				}
			})
		}
	}
}

func TestForest_getAccess_Fails(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call Get")
					ctrl := gomock.NewController(t)
					cache := NewMockNodeCache(ctrl)
					cache.EXPECT().Get(gomock.Any()).AnyTimes().Return(nil, false)
					var n Node
					cache.EXPECT().GetOrSet(gomock.Any(), gomock.Any()).AnyTimes().Return(shared.MakeShared(n), false, EmptyId(), nil, false)
					cache.EXPECT().Touch(gomock.Any()).AnyTimes()
					forest.nodeCache = cache

					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					// only the second call must fail - repeats four times for four calls
					calls := make([]*gomock.Call, 0, 8)
					for i := 0; i < 4; i++ {
						calls = append(calls, accounts.EXPECT().Get(gomock.Any()).Return(AccountNode{}, nil))
						calls = append(calls, accounts.EXPECT().Get(gomock.Any()).Return(AccountNode{}, injectedErr))
					}
					gomock.InOrder(calls...)
					forest.accounts = accounts

					root := NewNodeReference(AccountId(123))

					if _, err := forest.getReadAccess(&root); !errors.Is(err, injectedErr) {
						t.Errorf("getting access should fail")
					}
					if _, err := forest.getViewAccess(&root); !errors.Is(err, injectedErr) {
						t.Errorf("getting access should fail")
					}
					if _, err := forest.getHashAccess(&root); !errors.Is(err, injectedErr) {
						t.Errorf("getting access should fail")
					}
					if _, err := forest.getWriteAccess(&root); !errors.Is(err, injectedErr) {
						t.Errorf("getting access should fail")
					}
				})
			}
		}
	}
}

func TestForest_getSharedNode_Fails_Get_Copy(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					ctrl := gomock.NewController(t)
					cache := NewMockNodeCache(ctrl)
					cache.EXPECT().Get(gomock.Any()).AnyTimes().Return(nil, false)
					var n Node
					cache.EXPECT().GetOrSet(gomock.Any(), gomock.Any()).AnyTimes().Return(shared.MakeShared(n), false, EmptyId(), nil, false)
					forest.nodeCache = cache

					buffer := NewMockWriteBuffer(ctrl)
					buffer.EXPECT().Cancel(gomock.Any()).AnyTimes().Return(nil, true)
					forest.writeBuffer = buffer

					root := NewNodeReference(AccountId(123))

					defer func() {
						if r := recover(); r == nil {
							t.Errorf("method call did not panic")
						}
					}()

					if _, err := forest.getSharedNode(&root); err == nil {
						t.Errorf("getting shared node should fail")
					}
				})
			}
		}
	}
}

func TestForest_Flush_Fail_MissingCachedNode(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					ctrl := gomock.NewController(t)
					cache := NewMockNodeCache(ctrl)
					cache.EXPECT().Get(gomock.Any()).AnyTimes().Return(nil, false)
					forest.nodeCache = cache

					ids := []NodeId{AccountId(123)}
					if err := forest.flushDirtyIds(ids); err == nil {
						t.Errorf("flush should fail")
					}
				})
			}
		}
	}
}

func TestForest_Flush_Fail_CannotReadNode(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call Set")
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Set(gomock.Any(), gomock.Any()).AnyTimes().Return(injectedErr)
					forest.accounts = accounts

					cache := NewMockNodeCache(ctrl)
					var n Node = &AccountNode{}
					cache.EXPECT().Get(gomock.Any()).AnyTimes().Return(shared.MakeShared(n), true)
					forest.nodeCache = cache

					ids := []NodeId{AccountId(123)}
					if err := forest.flushDirtyIds(ids); !errors.Is(err, injectedErr) {
						t.Errorf("flush should fail")
					}
				})
			}
		}
	}
}

func TestForest_Flush_Makes_Node_Clean(t *testing.T) {
	for _, variant := range fileAndMemVariants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()
					var err error
					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}
					defer func() {
						if err := forest.Close(); err != nil {
							t.Fatalf("cannot close forest: %d", err)
						}
					}()

					addresses := getTestAddresses(10)
					keys := getTestKeys(10)

					root := NewNodeReference(EmptyId())
					for i, address := range addresses {
						root, err = forest.SetAccountInfo(&root, address, AccountInfo{Balance: amount.New(1)})
						if err != nil {
							t.Fatalf("cannot update account: %v", err)
						}
						for j, key := range keys {
							root, err = forest.SetValue(&root, address, key, common.Value{byte(i), byte(j)})
							if err != nil {
								t.Fatalf("cannot update storage: %v", err)
							}
						}
					}

					if _, _, err := forest.updateHashesFor(&root); err != nil {
						t.Fatalf("cannot update hashes: %v", err)
					}

					if err := forest.Flush(); err != nil {
						t.Fatalf("cannot flush: %v", err)
					}

					// all nodes must be clean
					forest.nodeCache.ForEach(func(id NodeId, s *shared.Shared[Node]) {
						handle := s.GetReadHandle()
						defer handle.Release()
						if handle.Get().IsDirty() {
							t.Errorf("node %v is dirty", id)
						}
					})
				})
			}
		}
	}
}

func TestForest_flushNode_EmptyId(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					if err := forest.flushNode(EmptyId(), nil); err != nil {
						t.Errorf("cannot flush empty node: %s", err)
					}
				})
			}
		}
	}
}

func TestForest_getMutableNodeByPath_CannotReadNode(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					var injectedErr = errors.New("failed to call Get")
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, injectedErr)
					forest.accounts = accounts
					root := NewNodeReference(AccountId(123))

					if _, err := forest.getMutableNodeByPath(&root, CreateNodePath([]Nibble{Nibble(1), Nibble(2)}...)); !errors.Is(err, injectedErr) {
						t.Errorf("getting node should fail")
					}
				})
			}
		}
	}
}

func TestForest_getMutableNodeByPath_TypeOfNodesReachable(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// inject failing stock to trigger an error applying the update
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, nil)

					values := stock.NewMockStock[uint64, ValueNode](ctrl)
					values.EXPECT().Get(gomock.Any()).AnyTimes().Return(ValueNode{}, nil)

					branches := stock.NewMockStock[uint64, BranchNode](ctrl)
					branches.EXPECT().Get(gomock.Any()).AnyTimes().Return(BranchNode{}, nil)

					extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
					extensions.EXPECT().Get(gomock.Any()).AnyTimes().Return(ExtensionNode{}, nil)

					forest.accounts = accounts
					forest.values = values
					forest.branches = branches
					forest.extensions = extensions

					var root NodeReference

					root = NewNodeReference(ExtensionId(123))
					if _, err := forest.getMutableNodeByPath(&root, CreateNodePath([]Nibble{Nibble(1), Nibble(2)}...)); err == nil {
						t.Errorf("getting node should fail")
					}
					root = NewNodeReference(AccountId(123))
					if _, err := forest.getMutableNodeByPath(&root, CreateNodePath([]Nibble{Nibble(1), Nibble(2)}...)); err == nil {
						t.Errorf("getting node should fail")
					}
					root = NewNodeReference(ValueId(123))
					if _, err := forest.getMutableNodeByPath(&root, CreateNodePath([]Nibble{Nibble(1), Nibble(2)}...)); err == nil {
						t.Errorf("getting node should fail")
					}
					root = NewNodeReference(BranchId(123))
					if _, err := forest.getMutableNodeByPath(&root, CreateNodePath([]Nibble{Nibble(1), Nibble(2)}...)); err == nil {
						t.Errorf("getting node should fail")
					}
				})
			}
		}
	}
}

func TestForest_Dump(t *testing.T) {
	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()

					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					root := NewNodeReference(AccountId(1))
					forest.Dump(&root) // ok case

					// inject failing stock to trigger an error applying the update
					ctrl := gomock.NewController(t)
					accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
					accounts.EXPECT().Get(gomock.Any()).AnyTimes().Return(AccountNode{}, errors.New("failed to call Get"))
					forest.accounts = accounts

					root2 := NewNodeReference(AccountId(2))
					forest.Dump(&root2) // trigger error case
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
				forest, err := variant.factory(t.TempDir(), config, ForestConfig{Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}})
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
				forest, err := variant.factory(t.TempDir(), config, ForestConfig{Mode: Immutable, NodeCacheConfig: NodeCacheConfig{Capacity: 1024}})
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
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{NodeCacheConfig: NodeCacheConfig{Capacity: 1}},
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
	return makeForest(mptConfig, branches, extensions, accounts, values, forestConfig)
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
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{
			NodeCacheConfig: NodeCacheConfig{
				Capacity:               1,
				writeBufferChannelSize: 1,
			},
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

func TestForest_CheckPassesAfterReopeningDirectory(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			for _, mode := range []StorageMode{Immutable, Mutable} {
				t.Run(mode.String(), func(t *testing.T) {
					// Create a forest, fill it with data, check it, close it, reopen
					// it, and check that the consistency test still passes.
					dir := t.TempDir()
					forestConfig := ForestConfig{
						Mode:            mode,
						NodeCacheConfig: NodeCacheConfig{Capacity: 1024},
					}
					forest, err := OpenFileForest(dir, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// Fill the forest with some data.
					const N = 10
					root := NewNodeReference(EmptyId())
					for a := 0; a < N; a++ {
						addr := common.Address{byte(a)}
						root, err = forest.SetAccountInfo(&root, addr, AccountInfo{Nonce: common.Nonce{1}})
						if err != nil {
							t.Fatalf("failed to create account %v: %v", addr, err)
						}
						for k := 0; k < N; k++ {
							root, err = forest.SetValue(&root, addr, common.Key{byte(k)}, common.Value{byte(k)})
							if err != nil {
								t.Fatalf("failed to update value %v: %v", addr, err)
							}
						}
					}
					_, _, err = forest.updateHashesFor(&root)
					if err != nil {
						t.Fatalf("failed to update hashes: %v", err)
					}
					if err := forest.Check(&root); err != nil {
						t.Fatalf("check for initial forest failed: %v", err)
					}

					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}

					// Reopen the forest and run check again.
					forest, err = OpenFileForest(dir, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to re-open forest: %v", err)
					}
					if err := forest.Check(&root); err != nil {
						t.Fatalf("check for re-opened forest failed: %v", err)
					}
					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}
				})
			}
		})
	}
}

func TestForest_String(t *testing.T) {
	if got, want := Immutable.String(), "Immutable"; got != want {
		t.Errorf("names do not match: got: %v != want: %v", got, want)
	}
	if got, want := Mutable.String(), "Mutable"; got != want {
		t.Errorf("names do not match: got: %v != want: %v", got, want)
	}
}

func TestForest_ReadForestMetadata_Fails(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := ReadForestMetadata(dir); err == nil {
		t.Errorf("reading file which is directory should fail")
	}
}

func TestForest_ErrorsAreForwardedAndCollected(t *testing.T) {
	type mocks struct {
		branches   *stock.MockStock[uint64, BranchNode]
		extensions *stock.MockStock[uint64, ExtensionNode]
		accounts   *stock.MockStock[uint64, AccountNode]
		values     *stock.MockStock[uint64, ValueNode]
	}

	injectedError := fmt.Errorf("injected error")

	addr := common.Address{}
	key := common.Key{}

	accountId := AccountId(10)
	accountNode := &AccountNode{}

	rootId := BranchId(12)
	rootRef := NewNodeReference(rootId)
	rootNode := &BranchNode{}
	rootNode.children[0] = NewNodeReference(accountId)

	prepareRootLookupFailure := func(m *mocks) {
		m.branches.EXPECT().Get(rootId.Index()).Return(BranchNode{}, injectedError)
	}

	prepareTreeNavigationFailure := func(m *mocks) {
		m.branches.EXPECT().Get(rootId.Index()).Return(*rootNode, nil)
		m.accounts.EXPECT().Get(accountId.Index()).Return(*accountNode, injectedError)
	}

	tests := map[string]struct {
		setExpectations func(*mocks)
		runOperation    func(*Forest) error
	}{
		"GetAccountInfo-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, _, err := f.GetAccountInfo(&rootRef, addr)
				return err
			},
		},
		"GetAccountInfo-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				_, _, err := f.GetAccountInfo(&rootRef, addr)
				return err
			},
		},
		"SetAccountInfo-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, err := f.SetAccountInfo(&rootRef, addr, AccountInfo{})
				return err
			},
		},
		"SetAccountInfo-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				_, err := f.SetAccountInfo(&rootRef, addr, AccountInfo{})
				return err
			},
		},
		"GetValue-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, err := f.GetValue(&rootRef, addr, key)
				return err
			},
		},
		"GetValue-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				_, err := f.GetValue(&rootRef, addr, key)
				return err
			},
		},
		"SetValue-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, err := f.SetValue(&rootRef, addr, key, common.Value{})
				return err
			},
		},
		"SetValue-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				_, err := f.SetValue(&rootRef, addr, key, common.Value{})
				return err
			},
		},
		"ClearStorage-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, err := f.ClearStorage(&rootRef, addr)
				return err
			},
		},
		"ClearStorage-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				_, err := f.ClearStorage(&rootRef, addr)
				return err
			},
		},
		"Freeze-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				return f.Freeze(&rootRef)
			},
		},
		"Freeze-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				// We need to unfreeze the root node after loading to avoid stopping the freeze
				// right at the root node.
				handle, _ := f.getWriteAccess(&rootRef)
				handle.Get().(*BranchNode).frozen = false
				handle.Get().(*BranchNode).frozenChildren = 0
				handle.Release()
				return f.Freeze(&rootRef)
			},
		},
		"VisitTrie-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				return f.VisitTrie(&rootRef, nil)
			},
		},
		"VisitTrie-Failed-TreeNavigation": {
			prepareTreeNavigationFailure,
			func(f *Forest) error {
				return f.VisitTrie(&rootRef, MakeVisitor(func(Node, NodeInfo) VisitResponse { return VisitResponseContinue }))
			},
		},
		"updateHashesFor-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, _, err := f.updateHashesFor(&rootRef)
				return err
			},
		},
		"setHashesFor-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				hashes := &NodeHashes{hashes: []NodeHash{{Path: EmptyPath()}}}
				return f.setHashesFor(&rootRef, hashes)
			},
		},
		"getHashFor-Failed-RootLookup": {
			prepareRootLookupFailure,
			func(f *Forest) error {
				_, err := f.getHashFor(&rootRef)
				return err
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			branches := stock.NewMockStock[uint64, BranchNode](ctrl)
			extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
			accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
			values := stock.NewMockStock[uint64, ValueNode](ctrl)

			forest, err := makeForest(
				MptConfig{Hashing: DirectHashing},
				branches,
				extensions,
				accounts,
				values,
				ForestConfig{
					Mode: Immutable,
				},
			)
			if err != nil {
				t.Fatalf("failed to create test forest: %v", err)
			}

			test.setExpectations(&mocks{branches, extensions, accounts, values})
			err = test.runOperation(forest)
			if !errors.Is(err, injectedError) {
				t.Errorf("missing forwarded error, wanted %v, got %v", injectedError, err)
			}

			if got, want := forest.CheckErrors(), injectedError; !errors.Is(got, want) {
				t.Errorf("missing injected error, got %v", got)
			}
		})
	}
}

func TestForest_MultipleErrorsCanBeCollected(t *testing.T) {
	injectedErrorA := fmt.Errorf("injected error A")
	injectedErrorB := fmt.Errorf("injected error B")

	ctrl := gomock.NewController(t)

	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
	accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
	values := stock.NewMockStock[uint64, ValueNode](ctrl)

	forest, err := makeForest(
		MptConfig{Hashing: DirectHashing},
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{
			Mode: Immutable,
		},
	)
	if err != nil {
		t.Fatalf("failed to create test forest: %v", err)
	}

	gomock.InOrder(
		branches.EXPECT().Get(gomock.Any()).Return(BranchNode{}, injectedErrorA),
		branches.EXPECT().Get(gomock.Any()).Return(BranchNode{}, injectedErrorB),
	)

	rootRef := NewNodeReference(BranchId(12))
	_, _, err = forest.GetAccountInfo(&rootRef, common.Address{})
	if !errors.Is(err, injectedErrorA) {
		t.Errorf("unexpected error, wanted %v, got %v", injectedErrorA, err)
	}

	_, _, err = forest.GetAccountInfo(&rootRef, common.Address{})
	if !errors.Is(err, injectedErrorB) {
		t.Errorf("unexpected error, wanted %v, got %v", injectedErrorB, err)
	}

	issues := forest.CheckErrors().(interface{ Unwrap() []error }).Unwrap()
	if len(issues) != 2 {
		t.Fatalf("missing issues, got %v", issues)
	}

	if want, got := issues[0], injectedErrorA; !errors.Is(want, got) {
		t.Errorf("unexpected error, wanted %v, got %v", want, got)
	}
	if want, got := issues[1], injectedErrorB; !errors.Is(want, got) {
		t.Errorf("unexpected error, wanted %v, got %v", want, got)
	}
}

func TestForest_CollectedErrorsAreReportedInFlushAndClose(t *testing.T) {
	injectedError := fmt.Errorf("injected error A")

	ctrl := gomock.NewController(t)

	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
	accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
	values := stock.NewMockStock[uint64, ValueNode](ctrl)

	forest, err := makeForest(
		MptConfig{Hashing: DirectHashing},
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{
			Mode: Immutable,
		},
	)
	if err != nil {
		t.Fatalf("failed to create test forest: %v", err)
	}

	branches.EXPECT().Get(gomock.Any()).Return(BranchNode{}, injectedError)

	branches.EXPECT().Flush().AnyTimes()
	accounts.EXPECT().Flush().AnyTimes()
	extensions.EXPECT().Flush().AnyTimes()
	values.EXPECT().Flush().AnyTimes()

	branches.EXPECT().Close().AnyTimes()
	accounts.EXPECT().Close().AnyTimes()
	extensions.EXPECT().Close().AnyTimes()
	values.EXPECT().Close().AnyTimes()

	rootRef := NewNodeReference(BranchId(12))
	_, _, err = forest.GetAccountInfo(&rootRef, common.Address{})
	if !errors.Is(err, injectedError) {
		t.Errorf("unexpected error, wanted %v, got %v", injectedError, err)
	}

	if want, got := injectedError, forest.Flush(); !errors.Is(got, want) {
		t.Errorf("missing operation error in flush, wanted %v, got %v", want, got)
	}

	if want, got := injectedError, forest.Close(); !errors.Is(got, want) {
		t.Errorf("missing operation error in close, wanted %v, got %v", want, got)
	}
}

func TestForest_FailingFlush_Invalidates_Forest(t *testing.T) {
	injectedError := fmt.Errorf("injected error A")

	ctrl := gomock.NewController(t)

	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	extensions := stock.NewMockStock[uint64, ExtensionNode](ctrl)
	accounts := stock.NewMockStock[uint64, AccountNode](ctrl)
	values := stock.NewMockStock[uint64, ValueNode](ctrl)

	forest, err := makeForest(
		MptConfig{Hashing: DirectHashing},
		branches,
		extensions,
		accounts,
		values,
		ForestConfig{
			Mode: Immutable,
		},
	)
	if err != nil {
		t.Fatalf("failed to create test forest: %v", err)
	}

	// only call fails, but the forest must be invalidated
	gomock.InOrder(
		branches.EXPECT().Flush().Return(injectedError),
		branches.EXPECT().Flush().Return(nil),
	)
	extensions.EXPECT().Flush().Return(nil).AnyTimes()
	accounts.EXPECT().Flush().Return(nil).AnyTimes()
	values.EXPECT().Flush().Return(nil).AnyTimes()

	if want, got := injectedError, forest.Flush(); !errors.Is(got, want) {
		t.Errorf("missing operation error in flush, wanted %v, got %v", want, got)
	}

	// next time it must fail as well
	if want, got := injectedError, forest.Flush(); !errors.Is(got, want) {
		t.Errorf("missing operation error in flush, wanted %v, got %v", want, got)
	}
}

func TestForest_CloseMultipleTimes(t *testing.T) {
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
					if err := forest.Close(); err == nil {
						t.Fatalf("closing forest multiple times should fail")
					} else if !errors.Is(err, forestClosedErr) {
						t.Fatalf("closing forest multiple times should return forestClosedErr, got %v", err)
					}
				})
			}
		}
	}
}

func TestForest_AsyncDelete_CacheIsNotExhausted(t *testing.T) {
	const num = 100

	keys := getTestKeys(num)
	for _, variant := range fileAndMemVariants {
		// test for live configs only as archive configs even write deleted
		// nodes and cache needs to be allocated for those nodes as well.
		for _, config := range []MptConfig{S5LiveConfig, S4LiveConfig} {
			config := config
			t.Run(config.Name, func(t *testing.T) {
				t.Parallel()

				// The tree is shallow - 1 account + 4byte storage addresses.
				// The size of the cache here is estimated considering the numbers above, but it is not set
				// exactly as some nodes are created and then deleted while building the tree.
				forest, err := variant.factory(t.TempDir(), config, ForestConfig{Mode: Mutable, NodeCacheConfig: NodeCacheConfig{Capacity: 10}})
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}

				rootRef := NewNodeReference(EmptyId())
				accountWithStorage := common.Address{0xA}
				root, err := forest.SetAccountInfo(&rootRef, accountWithStorage, AccountInfo{Balance: amount.New(1)})
				if err != nil {
					t.Fatalf("cannot create an account ")
				}
				rootRef = root

				for i, key := range keys {
					root, err := forest.SetValue(&rootRef, accountWithStorage, key, common.Value{byte(i)})
					if err != nil {
						t.Fatalf("failed to insert value: %v", err)
					}
					rootRef = root
					// trigger update of dirty hashes
					if _, _, err := forest.updateHashesFor(&rootRef); err != nil {
						t.Fatalf("failed to compute hash: %v", err)
					}
				}

				// flush the write buffer by flushing everything
				if err := forest.Flush(); err != nil {
					t.Fatalf("cannot flush db: %v", err)
				}

				// create a dirty path, which must not be impacted by the delete below
				root, err = forest.SetAccountInfo(&rootRef, common.Address{0xA, 0xB, 0xC, 0xD, 0xE, 0xF}, AccountInfo{Balance: amount.New(1)})
				if err != nil {
					t.Fatalf("cannot create an account ")
				}
				rootRef = root

				// delete the account's storage, it removes the subtries,
				// but it cannot evict the account created above.
				root, err = forest.ClearStorage(&rootRef, accountWithStorage)
				if err != nil {
					t.Fatalf("cannot delete account")
				}
				rootRef = root
				if err := forest.writeBuffer.Flush(); err != nil {
					t.Fatalf("cannot flush write buffer: %v", err)
				}

				// wait for accounts to be deleted
				forest.releaseQueue <- EmptyId()
				<-forest.releaseSync

				// trigger update of dirty hashes - ongoing change of the account addr. 0xABCDEF should not fail
				if _, _, err := forest.updateHashesFor(&rootRef); err != nil {
					t.Fatalf("failed to compute hash: %v", err)
				}

				// trigger close - ongoing change of the account addr. 0xABCDEF should not fail
				if err := forest.Close(); err != nil {
					t.Fatalf("cannot close db: %v", err)
				}
			})
		}
	}
}

func TestForest_VisitPathToAccount(t *testing.T) {
	const num = 100

	addresses := getTestAddresses(num)
	for _, variant := range fileAndMemVariants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}
					defer func() {
						if err := forest.Close(); err != nil {
							t.Fatalf("cannot close db: %v", err)
						}
					}()

					rootRef := NewNodeReference(EmptyId())
					for i, addr := range addresses {
						root, err := forest.SetAccountInfo(&rootRef, addr, AccountInfo{Balance: amount.New(uint64(i) + 1), Nonce: common.Nonce{1}})
						if err != nil {
							t.Fatalf("cannot create an account: %v", err)
						}
						rootRef = root
					}

					// trigger update of dirty hashes
					if _, _, err := forest.updateHashesFor(&rootRef); err != nil {
						t.Errorf("failed to compute hash: %v", err)
					}

					var lastNode Node
					ctrl := gomock.NewController(t)
					nodeVisitor := NewMockNodeVisitor(ctrl)
					nodeVisitor.EXPECT().Visit(gomock.Any(), gomock.Any()).Do(func(node Node, info NodeInfo) {
						lastNode = node
					}).AnyTimes()

					for i, addr := range addresses {
						lastNode = nil
						if found, err := VisitPathToAccount(forest, &rootRef, addr, nodeVisitor); err != nil || !found {
							t.Errorf("failed to iterate nodes by address: %v", err)
						}
						switch account := lastNode.(type) {
						case *AccountNode:
							want := amount.New(uint64(i) + 1)
							if got, want := account.info.Balance, want; got != want {
								t.Errorf("last iterated node is a wrong account node: got %v, want %v", got, want)
							}
						default:
							t.Errorf("unexpected node type, got %T, want *AccountNode", account)
						}
					}
				})
			}
		}
	}
}

func TestForest_VisitPathToStorage(t *testing.T) {
	const num = 100

	keys := getTestKeys(num)
	for _, variant := range fileAndMemVariants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					forest, err := variant.factory(t.TempDir(), config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}
					defer func() {
						if err := forest.Close(); err != nil {
							t.Fatalf("cannot close db: %v", err)
						}
					}()

					rootRef := NewNodeReference(EmptyId())
					addresses := getTestAddresses(num)
					for _, address := range addresses {
						root, err := forest.SetAccountInfo(&rootRef, address, AccountInfo{Balance: amount.New(0xB), Nonce: common.Nonce{1}})
						if err != nil {
							t.Fatalf("cannot create an account: %v", err)
						}
						rootRef = root

						for i, key := range keys {
							root, err := forest.SetValue(&rootRef, address, key, common.Value{byte(i + 1)})
							if err != nil {
								t.Fatalf("cannot create an account: %v", err)
							}
							rootRef = root
						}

						// trigger update of dirty hashes
						if _, _, err := forest.updateHashesFor(&rootRef); err != nil {
							t.Errorf("failed to compute hash: %v", err)
						}
					}

					ctrl := gomock.NewController(t)
					nodeVisitor := NewMockNodeVisitor(ctrl)
					nodeVisitor.EXPECT().Visit(gomock.Any(), gomock.Any()).Do(func(node Node, info NodeInfo) {
						// trigger storage search from the last account node
						if account, ok := node.(*AccountNode); ok {
							testVisitPathToStorage(t, forest, keys, account.storage)
						}
					}).AnyTimes()

					for _, address := range addresses {
						// find out account node to start storage search from it.
						if found, err := VisitPathToAccount(forest, &rootRef, address, nodeVisitor); err != nil || !found {
							t.Errorf("failed to iterate nodes by address: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestForest_HasEmptyStorage(t *testing.T) {
	strategies := []struct {
		delete func(*Forest, *NodeReference, common.Address, []common.Key) (NodeReference, error)
	}{
		{delete: func(forest *Forest, root *NodeReference, address common.Address, keys []common.Key) (NodeReference, error) {
			return forest.ClearStorage(root, address)
		}},
		{delete: func(forest *Forest, root *NodeReference, address common.Address, keys []common.Key) (NodeReference, error) {
			return forest.SetAccountInfo(root, address, AccountInfo{})
		}},
		{delete: func(forest *Forest, root *NodeReference, address common.Address, keys []common.Key) (NodeReference, error) {
			ref := *root
			var err error
			for _, key := range keys {
				ref, err = forest.SetValue(&ref, address, key, common.Value{})
				if err != nil {
					return ref, err
				}
			}
			return ref, err
		}},
	}

	addresses := getTestAddresses(50)
	keys := getTestKeys(15)

	for _, variant := range variants {
		for _, config := range allMptConfigs {
			for forestConfigName, forestConfig := range forestConfigs {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, config.Name, forestConfigName), func(t *testing.T) {
					directory := t.TempDir()
					forest, err := variant.factory(directory, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}
					defer func() {
						if err := forest.Close(); err != nil {
							t.Fatalf("cannot close db: %v", err)
						}
					}()

					root := NewNodeReference(EmptyId())

					checkEmpty := func(address []common.Address, want bool) {
						t.Helper()
						for _, address := range addresses {
							isEmpty, err := forest.HasEmptyStorage(&root, address)
							if err != nil {
								t.Fatalf("unexpected error: %v", err)
							}
							if got := isEmpty; got != want {
								t.Errorf("unexpected result for empty account %x: got %v, want %v", address, got, want)
							}
						}
					}

					// empty for all non existing accounts
					checkEmpty(addresses, true)

					for _, address := range addresses {
						root, err = forest.SetAccountInfo(&root, address, AccountInfo{Balance: amount.New(1)})
						if err != nil {
							t.Fatalf("cannot update account: %v", err)
						}
						if _, _, err := forest.updateHashesFor(&root); err != nil {
							t.Fatalf("cannot update hashes: %v", err)
						}
					}

					// empty for all accounts with empty storage
					checkEmpty(addresses, true)

					for i, address := range addresses {
						for j, key := range keys {
							root, err = forest.SetValue(&root, address, key, common.Value{byte(i + 1), byte(j + 1)})
							if err != nil {
								t.Fatalf("cannot update storage: %v", err)
							}
						}
						if _, _, err := forest.updateHashesFor(&root); err != nil {
							t.Fatalf("cannot update hashes: %v", err)
						}
					}

					// non-empty for existing slots
					checkEmpty(addresses, false)

					// clear by various strategies
					groups := len(strategies)
					for i, address := range addresses {
						strategy := strategies[i%groups]
						root, err = strategy.delete(forest, &root, address, keys)
						if err != nil {
							t.Fatalf("cannot delete storage: %v", err)
						}
						if _, _, err := forest.updateHashesFor(&root); err != nil {
							t.Fatalf("cannot update hashes: %v", err)
						}
					}

					// becomes empty
					checkEmpty(addresses, true)

					_, _, err = forest.updateHashesFor(&root)
					if err != nil {
						t.Fatalf("cannot update hashes: %v", err)
					}
				})
			}
		}
	}
}

func TestVisitPathTo_Node_Hashes_Same_S5_Archive_non_Archive_Config_For_Witness_Proof(t *testing.T) {
	addresses := getTestAddresses(61)
	keys := getTestKeys(63)
	for _, variant := range fileAndMemVariants {
		for forestConfigName, forestConfig := range forestConfigs {
			for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {
				t.Run(fmt.Sprintf("%s-%s-%s", variant.name, forestConfigName, config.Name), func(t *testing.T) {
					dir := t.TempDir()
					forest, err := variant.factory(dir, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					// fill-in the forest with test data
					root := NewNodeReference(EmptyId())
					for i, addr := range addresses {
						info := AccountInfo{Nonce: common.Nonce{byte(i + 1)}}
						root, err = forest.SetAccountInfo(&root, addr, info)
						if err != nil {
							t.Fatalf("failed to set account info: %v", err)
						}
						for j, key := range keys {
							var value common.Value // a short value to embedded
							value[29] = byte(i)
							value[30] = byte(j)
							root, err = forest.SetValue(&root, addr, key, value)
							if err != nil {
								t.Fatalf("failed to update value: %v", err)
							}
						}
						if _, _, err := forest.updateHashesFor(&root); err != nil {
							t.Fatalf("failed to update hashes: %v", err)
						}
					}

					rootHash, err := forest.getHashFor(&root)
					if err != nil {
						t.Fatalf("failed to get hash: %v", err)
					}
					if err := forest.Close(); err != nil {
						t.Fatalf("failed to close forest: %v", err)
					}

					// reopen the forest and create witness proof
					// the witness proof should work both configs
					// i.e. the hashes in the proof must be the same
					forest, err = variant.factory(dir, config, forestConfig)
					if err != nil {
						t.Fatalf("failed to open forest: %v", err)
					}

					for i, addr := range addresses {
						proof, err := CreateWitnessProof(forest, &root, addr, keys...)
						if err != nil {
							t.Fatalf("failed to create witness proof: %v", err)
						}

						nonce, _, err := proof.GetNonce(rootHash, addr)
						if err != nil {
							t.Fatalf("failed to get nonce: %v", err)
						}
						if got, want := nonce, (common.Nonce{byte(i + 1)}); got != want {
							t.Errorf("nonce mismatch: got %d, want %d", got, want)
						}

						for j, key := range keys {
							value, _, err := proof.GetState(rootHash, addr, key)
							if err != nil {
								t.Fatalf("failed to get value: %v", err)
							}
							var wantValue common.Value // a short value to embedded
							wantValue[29] = byte(i)
							wantValue[30] = byte(j)
							if got, want := value, wantValue; got != want {
								t.Errorf("value mismatch: got %v, want %v", got, want)
							}
						}
					}
				})
			}
		}
	}
}

// testVisitPathToStorage iterates over the keys and checks if the value nodes are correct.
func testVisitPathToStorage(t *testing.T, forest *Forest, keys []common.Key, storageRoot NodeReference) {
	var lastNode Node
	ctrl := gomock.NewController(t)
	nodeVisitor := NewMockNodeVisitor(ctrl)
	nodeVisitor.EXPECT().Visit(gomock.Any(), gomock.Any()).Do(func(node Node, info NodeInfo) {
		lastNode = node
	}).AnyTimes()

	for i, key := range keys {
		lastNode = nil
		if found, err := VisitPathToStorage(forest, &storageRoot, key, nodeVisitor); err != nil || !found {
			t.Errorf("failed to iterate nodes by address: %v", err)
		}
		switch value := lastNode.(type) {
		case *ValueNode:
			want := common.Value{byte(i + 1)}
			if got, want := value.value, want; got != want {
				t.Errorf("wrong value node: got %v, want %v", got, want)
			}
		default:
			t.Errorf("unexpected node type, got %T, want *ValueNode", value)
		}
	}
}

func TestForest_RecoversNodesFromWriteBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	branches := stock.NewMockStock[uint64, BranchNode](ctrl)
	buffer := NewMockWriteBuffer(ctrl)

	var branch1 *shared.Shared[Node]
	gomock.InOrder(
		// Branch 1 is created, not found in the buffer, added to the cache, and released.
		branches.EXPECT().New().Return(uint64(1), nil),
		buffer.EXPECT().Cancel(BranchId(1)).Return(nil, false),
		branches.EXPECT().Delete(uint64(1)),

		// Branches 2 and 3 are created, not found in the cache, and remain alive
		branches.EXPECT().New().Return(uint64(2), nil),
		buffer.EXPECT().Cancel(BranchId(2)).Return(nil, false),
		branches.EXPECT().New().Return(uint64(3), nil),
		buffer.EXPECT().Cancel(BranchId(3)).Return(nil, false),

		// As branch 3 is added to the cache, branch 1 is evicted and pushed into the buffer.
		buffer.EXPECT().Add(BranchId(1), gomock.Any()).Do(func(_ NodeId, evicted *shared.Shared[Node]) {
			branch1 = evicted
		}),

		// After branch 1 is in the buffer, its ID is re-used for the next branch.
		branches.EXPECT().New().Return(uint64(1), nil),
		buffer.EXPECT().Cancel(BranchId(1)).DoAndReturn(func(NodeId) (*shared.Shared[Node], bool) {
			return branch1, true
		}),
		buffer.EXPECT().Add(BranchId(2), gomock.Any()),
	)

	forest := &Forest{
		branches:    branches,
		nodeCache:   NewNodeCache(2),
		writeBuffer: buffer,
	}

	// the first node is created, marked dirty, and released again
	ref, handle, _ := forest.createBranch()
	original := handle.Get().(*BranchNode)
	original.markDirty()
	forest.release(&ref)
	handle.Release()

	// At this point, b1 is still in the cache. With the
	// next steps, it is pushed out of the cache into the
	// write buffer (since it is marked dirty).
	_, handle, _ = forest.createBranch()
	handle.Release()
	_, handle, _ = forest.createBranch()
	handle.Release()

	// Now, the node with ID BranchId(1) is reused again.
	// The node in the buffer should be recovered and reused.
	ref, handle, _ = forest.createBranch()

	if want, got := ref.Id(), BranchId(1); want != got {
		t.Fatalf("unexpected node ID: got %v, want %v", got, want)
	}

	restored := handle.Get().(*BranchNode)
	if restored != original {
		t.Fatalf("unexpected node: got %p, want %p", restored, original)
	}
	handle.Release()
}
