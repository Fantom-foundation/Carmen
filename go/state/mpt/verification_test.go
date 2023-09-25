package mpt

import (
	"os"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/common"
	"go.uber.org/mock/gomock"
)

var forestFiles = []string{
	"",
	"accounts",
	"accounts/freelist.dat",
	"accounts/meta.json",
	"accounts/values.dat",
	"branches",
	"branches/freelist.dat",
	"branches/meta.json",
	"branches/values.dat",
	"extensions",
	"extensions/freelist.dat",
	"extensions/meta.json",
	"extensions/values.dat",
	"values",
	"values/freelist.dat",
	"values/meta.json",
	"values/values.dat",
}

func TestVerification_VerifyValidForest(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err != nil {
			t.Errorf("found unexpected error in fresh forest: %v", err)
		}
	})
}

func TestVerification_VerificationObserverIsKeptUpdatedOnEvents(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {

		ctrl := gomock.NewController(t)
		observer := NewMockVerificationObserver(ctrl)

		gomock.InOrder(
			observer.EXPECT().StartVerification(),
			observer.EXPECT().Progress(gomock.Any()).MinTimes(1),
			observer.EXPECT().EndVerification(nil),
		)

		if err := VerifyFileForest(dir, config, roots, observer); err != nil {
			t.Errorf("found unexpected error in fresh forest: %v", err)
		}
	})
}

func TestVerification_MissingFileIsDetected(t *testing.T) {
	for _, file := range forestFiles {
		t.Run(file, func(t *testing.T) {
			runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
				if err := os.RemoveAll(dir + "/" + file); err != nil {
					t.Fatalf("failed to delete file %v: %v", file, err)
				}
				if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
					t.Errorf("The missing file %v should have been detected", file)
				}
			})
		})
	}
}

func TestVerification_ModifiedFileIsDetected(t *testing.T) {
	for _, file := range forestFiles {
		t.Run(file, func(t *testing.T) {
			runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
				path := dir + "/" + file
				if isDirectory(path) {
					return
				}

				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("failed to read file %v: %v", path, err)
				}
				if len(data) == 0 {
					return
				}
				// Modify the content of the file a lot since some files contain
				// unused data that is not covered by the validation. Finer-grained
				// changes are checked below.
				for i := range data {
					data[i]++
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatalf("failed to write modified file content: %v", err)
				}

				if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
					t.Errorf("Modified file %v should have been detected", file)
				}
			})
		})
	}
}

func TestVerification_ModifiedRootIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, encoder, _, _ := getEncoder(config)

		root := EmptyId()
		for i := 0; i < len(roots); i++ {
			if roots[i].nodeId.IsBranch() {
				root = roots[i].nodeId
				break
			}
		}
		if !root.IsBranch() {
			t.Fatalf("no root referencing a branch found")
		}

		stock, err := file.OpenStock[uint64](encoder, dir+"/branches")
		if err != nil {
			t.Fatalf("failed to open stock")
		}

		node, err := stock.Get(root.Index())
		if err != nil {
			t.Fatalf("failed to load node from stock: %v", err)
		}

		a := 0
		b := 1
		for node.children[b].IsEmpty() {
			b++
		}
		node.children[a], node.children[b] = node.children[b], node.children[a]
		node.hashes[a], node.hashes[b] = node.hashes[b], node.hashes[a]

		if err := stock.Set(root.Index(), node); err != nil {
			t.Fatalf("failed to update node: %v", err)
		}

		if err := stock.Close(); err != nil {
			t.Fatalf("failed to close stock: %v", err)
		}

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified root node should have been detected")
		}
	})
}

func TestVerification_AccountBalanceModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		encoder, _, _, _ := getEncoder(config)

		modifyNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.info.Balance[2]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_AccountNonceModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		encoder, _, _, _ := getEncoder(config)

		modifyNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.info.Nonce[2]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_AccountCodeHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		encoder, _, _, _ := getEncoder(config)

		modifyNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.info.CodeHash[2]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_AccountStorageModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		encoder, _, _, _ := getEncoder(config)

		modifyNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.storage = ValueId(123456789) // invalid in test forest
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_AccountStorageHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		encoder, _, _, _ := getEncoder(config)

		modifyNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.storageHash[3]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_BranchChildIdModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, encoder, _, _ := getEncoder(config)

		modifyNode(t, dir+"/branches", encoder, func(node *BranchNode) {
			node.children[8] = ValueId(123456789) // does not exist in test forest
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_BranchChildHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, encoder, _, _ := getEncoder(config)

		modifyNode(t, dir+"/branches", encoder, func(node *BranchNode) {
			for i, child := range node.children {
				if !child.IsEmpty() {
					node.hashes[i][4]++
					break
				}
			}
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ExtensionPathModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, _, encoder, _ := getEncoder(config)

		modifyNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
			node.path.path[0] = ^node.path.path[0]
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ExtensionNextModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, _, encoder, _ := getEncoder(config)

		modifyNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
			node.next = BranchId(123456789)
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ExtensionNextHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, _, encoder, _ := getEncoder(config)

		modifyNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
			node.nextHash[24]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ValueKeyModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, _, _, encoder := getEncoder(config)

		modifyNode(t, dir+"/values", encoder, func(node *ValueNode) {
			node.key[5]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ValueModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		_, _, _, encoder := getEncoder(config)

		modifyNode(t, dir+"/values", encoder, func(node *ValueNode) {
			node.value[12]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func runVerificationTest(t *testing.T, verify func(t *testing.T, dir string, config MptConfig, roots []Root)) {
	t.Helper()
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()
			t.Helper()
			dir := t.TempDir()
			roots, err := fillTestForest(dir, config)
			if err != nil {
				t.Fatalf("failed to create example forest: %v", err)
			}

			verify(t, dir, config, roots)
		})
	}
}

func modifyNode[N any](t *testing.T, directory string, encoder stock.ValueEncoder[N], modify func(n *N)) {
	t.Helper()
	stock, err := file.OpenStock[uint64](encoder, directory)
	if err != nil {
		t.Fatalf("failed to open stock")
	}

	ids, err := stock.GetIds()
	if err != nil {
		t.Fatalf("failed to get stock ids: %v", err)
	}

	idx, found := getFirstElementInSet(ids)
	if !found {
		t.SkipNow()
	}

	node, err := stock.Get(idx)
	if err != nil {
		t.Fatalf("failed to load node from stock: %v", err)
	}

	modify(&node)

	if err := stock.Set(idx, node); err != nil {
		t.Fatalf("failed to update node: %v", err)
	}

	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}
}

func fillTestForest(dir string, config MptConfig) (roots []Root, err error) {
	const N = 100
	forest, err := OpenFileForest(dir, config, Archive)
	if err != nil {
		return nil, err
	}

	root := EmptyId()
	for i := 0; i < N; i++ {
		addr := common.Address{byte(i)}
		root, err = forest.SetAccountInfo(root, addr, AccountInfo{Nonce: common.ToNonce(1)})
		if err != nil {
			return nil, err
		}
		for j := 0; j < N; j++ {
			root, err = forest.SetValue(root, addr, common.Key{byte(j)}, common.Value{byte(i + j + 1)})
			if err != nil {
				return nil, err
			}
		}
		err = forest.Freeze(root)
		if err != nil {
			return nil, err
		}
		hash, err := forest.updateHashesFor(root)
		if err != nil {
			return nil, err
		}
		roots = append(roots, Root{
			nodeId: root,
			hash:   hash,
		})
	}

	return roots, forest.Close()
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func getFirstElementInSet(set stock.IndexSet[uint64]) (uint64, bool) {
	for i := set.GetLowerBound(); i < set.GetUpperBound(); i++ {
		if set.Contains(i) {
			return i, true
		}
	}
	return 0, false
}
