package s4

import (
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

var configurations = []struct {
	name    string
	factory func(directory string, mode StorageMode) (*Forest, error)
}{
	{"InMemory", OpenInMemoryForest},
	{"FileBased", OpenFileForest},
	{"FileShadow", OpenFileShadowForest},
}

func TestForest_OpenAndClose(t *testing.T) {
	for _, config := range configurations {
		for _, mode := range []StorageMode{Live, Archive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, mode), func(t *testing.T) {
				forest, err := config.factory(t.TempDir(), mode)
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

func TestForest_ClosedAndReOpened(t *testing.T) {
	for _, config := range configurations {
		for _, mode := range []StorageMode{Live, Archive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, mode), func(t *testing.T) {
				directory := t.TempDir()

				forest, err := config.factory(directory, mode)
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}

				addr := common.Address{1}
				info := AccountInfo{Nonce: common.Nonce{12}}

				root := EmptyId()
				root, err = forest.SetAccountInfo(root, addr, info)
				if err != nil {
					t.Fatalf("failed to set account info: %v", err)
				}

				if err := forest.Close(); err != nil {
					t.Fatalf("failed to close forest: %v", err)
				}

				reopened, err := config.factory(directory, mode)
				if err != nil {
					t.Fatalf("failed to re-open forest: %v", err)
				}

				if got, found, err := reopened.GetAccountInfo(root, addr); info != got || !found || err != nil {
					t.Fatalf("reopened forest does not contain expected value, wanted %v, got %v, found %t, err %v", info, got, found, err)
				}

				if err := reopened.Close(); err != nil {
					t.Fatalf("failed to close forest: %v", err)
				}
			})
		}
	}
}

func TestForest_ArchiveInfoCanBeSetAndRetrieved(t *testing.T) {
	for _, config := range configurations {
		for _, mode := range []StorageMode{Live, Archive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, mode), func(t *testing.T) {
				forest, err := config.factory(t.TempDir(), mode)
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}

				addr := common.Address{1}
				info0 := AccountInfo{}
				info1 := AccountInfo{Nonce: common.Nonce{12}}

				root := EmptyId()
				if info, found, err := forest.GetAccountInfo(root, addr); info != info0 || found || err != nil {
					t.Errorf("empty tree should not contain any info, wanted (%v,%t), got (%v,%t), err %v", info0, false, info, true, err)
				}

				root, err = forest.SetAccountInfo(root, addr, info1)
				if err != nil {
					t.Fatalf("failed to set account info: %v", err)
				}

				if info, found, err := forest.GetAccountInfo(root, addr); info != info1 || !found || err != nil {
					t.Errorf("empty tree should not contain any info, wanted (%v,%t), got (%v,%t), err %v", info1, true, info, true, err)
				}

				if err := forest.Close(); err != nil {
					t.Fatalf("failed to close forest: %v", err)
				}
			})
		}
	}
}

func TestForest_ValueCanBeSetAndRetrieved(t *testing.T) {
	for _, config := range configurations {
		for _, mode := range []StorageMode{Live, Archive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, mode), func(t *testing.T) {
				forest, err := config.factory(t.TempDir(), mode)
				if err != nil {
					t.Fatalf("failed to open forest: %v", err)
				}

				addr := common.Address{1}
				info := AccountInfo{Nonce: common.Nonce{12}}
				key := common.Key{12}
				value0 := common.Value{}
				value1 := common.Value{1}

				// Initially, the value is zero.
				root := EmptyId()
				if value, err := forest.GetValue(root, addr, key); value != value0 || err != nil {
					t.Errorf("empty tree should not contain any info, wanted %v, got %v, err %v", value0, value, err)
				}

				// Setting it without an account does nto have an effect.
				if newRoot, err := forest.SetValue(root, addr, key, value1); newRoot != root || err != nil {
					t.Errorf("setting a value without an account should not change the root, wanted %v, got %v, err %v", root, newRoot, err)
				}

				// Setting the value of an existing account should have an effect.
				root, err = forest.SetAccountInfo(root, addr, info)
				if err != nil {
					t.Fatalf("failed to create an account: %v", err)
				}

				if root, err = forest.SetValue(root, addr, key, value1); err != nil {
					t.Errorf("setting a value failed: %v", err)
				}

				if value, err := forest.GetValue(root, addr, key); value != value1 || err != nil {
					t.Errorf("value should be contained now, wanted %v, got %v, err %v", value1, value, err)
				}

				if err := forest.Check(root); err != nil {
					t.Errorf("inconsistent trie: %v", err)
				}

				if err := forest.Close(); err != nil {
					t.Fatalf("failed to close forest: %v", err)
				}
			})
		}
	}
}

func TestForest_InLiveModeHistoryIsOverridden(t *testing.T) {
	for _, config := range configurations {
		t.Run(config.name, func(t *testing.T) {
			forest, err := config.factory(t.TempDir(), Live)
			if err != nil {
				t.Fatalf("failed to open forest: %v", err)
			}

			addr := common.Address{1}
			info1 := AccountInfo{Nonce: common.Nonce{12}}
			info2 := AccountInfo{Nonce: common.Nonce{14}}

			// Initially, the value is zero.
			root0 := EmptyId()

			// Update the account info in two steps.
			root1, err := forest.SetAccountInfo(root0, addr, info1)
			if err != nil {
				t.Fatalf("failed to create an account: %v", err)
			}

			root2, err := forest.SetAccountInfo(root1, addr, info2)
			if err != nil {
				t.Fatalf("failed to create an account: %v", err)
			}

			// The second update should have not introduced a new root.
			if root1 != root2 {
				t.Errorf("expeted same root, got %v and %v", root1, root2)
			}
			if info, found, err := forest.GetAccountInfo(root1, addr); info != info2 || !found || err != nil {
				t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info2, info, found, err)
			}
		})
	}
}

func TestForest_InArchiveModeHistoryIsPreserved(t *testing.T) {
	for _, config := range configurations {
		t.Run(config.name, func(t *testing.T) {
			forest, err := config.factory(t.TempDir(), Archive)
			if err != nil {
				t.Fatalf("failed to open forest: %v", err)
			}

			addr := common.Address{1}
			info1 := AccountInfo{Nonce: common.Nonce{12}}
			info2 := AccountInfo{Nonce: common.Nonce{14}}

			// Initially, the value is zero.
			root0 := EmptyId()
			if err := forest.Freeze(root0); err != nil {
				t.Errorf("failed to freeze root0: %v", err)
			}

			// Update the account info in two steps.
			root1, err := forest.SetAccountInfo(root0, addr, info1)
			if err != nil {
				t.Fatalf("failed to create an account: %v", err)
			}
			if err := forest.Freeze(root1); err != nil {
				t.Errorf("failed to freeze root1: %v", err)
			}

			root2, err := forest.SetAccountInfo(root1, addr, info2)
			if err != nil {
				t.Fatalf("failed to create an account: %v", err)
			}
			if err := forest.Freeze(root2); err != nil {
				t.Errorf("failed to freeze root2: %v", err)
			}

			// All versions should still be accessable.
			if info, found, err := forest.GetAccountInfo(root1, addr); info != info1 || !found || err != nil {
				t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info1, info, found, err)
			}
			if info, found, err := forest.GetAccountInfo(root1, addr); info != info1 || !found || err != nil {
				t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info1, info, found, err)
			}
			if info, found, err := forest.GetAccountInfo(root2, addr); info != info2 || !found || err != nil {
				t.Errorf("invalid version information, wanted %v, got %v, found %t, err %v", info2, info, found, err)
			}
		})
	}
}
