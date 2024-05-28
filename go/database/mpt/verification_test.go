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
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
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

		if err := VerifyMptState(dir, config, roots, observer); err != nil {
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

		root := NewNodeReference(EmptyId())
		for i := 0; i < len(roots); i++ {
			if roots[i].NodeRef.Id().IsBranch() {
				root = roots[i].NodeRef
				break
			}
		}
		if !root.Id().IsBranch() {
			t.Fatalf("no root referencing a branch found")
		}

		stock, err := file.OpenStock[uint64](encoder, dir+"/branches")
		if err != nil {
			t.Fatalf("failed to open stock")
		}

		node, err := stock.Get(root.Id().Index())
		if err != nil {
			t.Fatalf("failed to load node from stock: %v", err)
		}

		a := 0
		b := 1
		for node.children[b].Id().IsEmpty() {
			b++
		}
		node.children[a], node.children[b] = node.children[b], node.children[a]
		node.hashes[a], node.hashes[b] = node.hashes[b], node.hashes[a]

		if err := stock.Set(root.Id().Index(), node); err != nil {
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

		modifyFirstNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
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

		modifyFirstNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
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

		modifyFirstNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
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

		modifyFirstNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.storage = NewNodeReference(ValueId(123456789)) // invalid in test forest
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_AccountNodeHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithNode {
			return
		}
		encoder, _, _, _ := getEncoder(config)

		modifyFirstNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
			node.hash[3]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_AccountStorageHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithParent {
			return
		}
		encoder, _, _, _ := getEncoder(config)

		modifyFirstNode(t, dir+"/accounts", encoder, func(node *AccountNode) {
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

		modifyFirstNode(t, dir+"/branches", encoder, func(node *BranchNode) {
			node.children[8] = NewNodeReference(ValueId(123456789)) // does not exist in test forest
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_BranchNodeHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithNode {
			return
		}
		_, encoder, _, _ := getEncoder(config)

		modifyFirstNode(t, dir+"/branches", encoder, func(node *BranchNode) {
			node.hash[4]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_BranchChildHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithParent {
			return
		}
		_, encoder, _, _ := getEncoder(config)

		modifyFirstNode(t, dir+"/branches", encoder, func(node *BranchNode) {
			for i, child := range node.children {
				if !child.Id().IsEmpty() {
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

		modifyFirstNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
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

		modifyFirstNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
			node.next = NewNodeReference(BranchId(123456789))
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ExtensionNodeHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithNode {
			return
		}
		_, _, encoder, _ := getEncoder(config)

		modifyFirstNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
			node.hash[24]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ExtensionNextHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithParent {
			return
		}
		_, _, encoder, _ := getEncoder(config)

		modifyFirstNode(t, dir+"/extensions", encoder, func(node *ExtensionNode) {
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

		modifyFirstNode(t, dir+"/values", encoder, func(node *ValueNode) {
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

		modifyFirstNode(t, dir+"/values", encoder, func(node *ValueNode) {
			node.value[12]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_ValueNodeHashModificationIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if config.HashStorageLocation != HashStoredWithNode {
			return
		}
		_, _, _, encoder := getEncoder(config)

		modifyFirstNode(t, dir+"/values", encoder, func(node *ValueNode) {
			node.hash[12]++
		})

		if err := VerifyFileForest(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("Modified node should have been detected")
		}
	})
}

func TestVerification_MissingCodeHashInCodeFileIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		testHash := common.Keccak256([]byte{1})
		encoder, _, _, _ := getEncoder(config)

		modifyFirstNode(t, filepath.Join(dir, "accounts"), encoder, func(node *AccountNode) {
			node.info.CodeHash = testHash
		})

		if err := VerifyMptState(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("missing hash in code file should have been detected")
		}
	})
}

func TestVerification_DifferentHashInCodeFileIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		testHash := common.Keccak256([]byte{1})
		codes := map[common.Hash][]byte{
			testHash: {2},
		}
		if err := writeCodes(codes, filepath.Join(dir, "codes.dat")); err != nil {
			t.Fatalf("failed to write code file")
		}

		encoder, _, _, _ := getEncoder(config)

		modifyFirstNode(t, filepath.Join(dir, "accounts"), encoder, func(node *AccountNode) {
			node.info.CodeHash = testHash
		})

		if err := VerifyMptState(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("different hash in code file should have been detected")
		}
	})
}

func TestVerification_ExtraCodeHashInCodeFileIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		ctrl := gomock.NewController(t)
		observer := NewMockVerificationObserver(ctrl)

		testHash := common.Keccak256([]byte{1})
		codes := map[common.Hash][]byte{
			testHash: {1},
		}

		gomock.InOrder(
			observer.EXPECT().StartVerification(),
			observer.EXPECT().Progress("Obtaining read access to files ..."),
			observer.EXPECT().Progress(fmt.Sprintf("Checking contract codes ...")),
			observer.EXPECT().Progress(fmt.Sprintf("There are %d contracts not referenced by any accounts:", len(codes))),
			observer.EXPECT().Progress(fmt.Sprintf("%x\n", testHash)),
			observer.EXPECT().Progress(gomock.Any()).MinTimes(1),
			observer.EXPECT().EndVerification(nil),
		)

		if err := writeCodes(codes, dir+"/codes.dat"); err != nil {
			t.Fatalf("failed to write code file")
		}

		if err := VerifyMptState(dir, config, roots, observer); err != nil {
			t.Errorf("found unexpected error in fresh forest: %v", err)
		}
	})
}

func TestVerification_UnreadableCodesReturnError(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		f, err := os.OpenFile(filepath.Join(dir, "codes.dat"), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			t.Fatalf("failed to open codes file: %v", err)
		}

		_, err = f.Write([]byte{1})
		if err != nil {
			t.Fatalf("failed to open write to codes file: %v", err)
		}

		if err = f.Close(); err != nil {
			t.Fatalf("failed to close codes file: %v", err)
		}

		if err = VerifyMptState(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("different hash in code file should have been detected")
		}
	})
}

func TestVerification_PassingNilAsObserverDoesNotPanic(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		if err := VerifyMptState(dir, config, roots, nil); err != nil {
			t.Errorf("found unexpected error in verification: %v", err)
		}
	})
}

func TestVerification_CodesAreNotCheckedTwice(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		testHash := common.Keccak256([]byte{1})
		codes := map[common.Hash][]byte{
			testHash: {1},
		}
		if err := writeCodes(codes, filepath.Join(dir, "codes.dat")); err != nil {
			t.Fatalf("failed to write code file")
		}

		encoder, _, _, _ := getEncoder(config)

		// modify two different nodes to append same hash to two accounts
		modifyFirstNode(t, filepath.Join(dir, "accounts"), encoder, func(node *AccountNode) {
			node.info.CodeHash = testHash
		})

		modifyLastNode(t, filepath.Join(dir, "accounts"), encoder, func(node *AccountNode) {
			node.info.CodeHash = testHash
		})

		if err := VerifyMptState(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("modified node should have been detected")
		}
	})
}

func TestVerification_DifferentExtraHashInCodeFileIsDetected(t *testing.T) {
	runVerificationTest(t, func(t *testing.T, dir string, config MptConfig, roots []Root) {
		testHash := common.Keccak256([]byte{1})
		codes := map[common.Hash][]byte{
			testHash: {2},
		}
		if err := writeCodes(codes, filepath.Join(dir, "codes.dat")); err != nil {
			t.Fatalf("failed to write code file")
		}

		if err := VerifyMptState(dir, config, roots, NilVerificationObserver{}); err == nil {
			t.Errorf("different extra hash in code file should have been detected")
		}
	})
}

func TestVerification_HashesOfEmbeddedNodesAreIgnored(t *testing.T) {
	// Construct an MPT with some embedded nodes. For this we need some keys
	// with their hashes sharing a long common prefix. The hashes of the
	// following keys have a 4-byte long common prefix.
	var key1, key2 common.Key
	data, _ := hex.DecodeString("965866864f3cc23585ad48a3b4b061c5e1d5a471dbb2360538029046ac528d85")
	copy(key1[:], data)
	data, _ = hex.DecodeString("c1bb1e5ab6acf1bef1a125f3d60e0941b9a8624288ffd67282484c25519f9e65")
	copy(key2[:], data)

	var v1 common.Value
	v1[len(v1)-1] = 1

	dir := t.TempDir()
	forestConfig := ForestConfig{Mode: Mutable, CacheCapacity: 1024}
	forest, err := OpenFileForest(dir, S5LiveConfig, forestConfig)
	if err != nil {
		t.Fatalf("failed to start empty forest: %v", err)
	}

	err = writeCodes(nil, dir+"/codes.dat")
	if err != nil {
		t.Fatalf("failed to create codes file: %v", err)
	}

	root := NewNodeReference(EmptyId())

	addr := common.Address{}
	root, err = forest.SetAccountInfo(&root, addr, AccountInfo{Nonce: common.ToNonce(1), CodeHash: emptyCodeHash})
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}

	root, err = forest.SetValue(&root, addr, key1, v1)
	if err != nil {
		t.Fatalf("failed to set value for key1: %v", err)
	}

	root, err = forest.SetValue(&root, addr, key2, v1)
	if err != nil {
		t.Fatalf("failed to set value for key2: %v", err)
	}

	hash, _, err := forest.updateHashesFor(&root)
	if err != nil {
		t.Fatalf("failed to compute hash for trie: %v", err)
	}

	if err := forest.Close(); err != nil {
		t.Fatalf("failed to close trie: %v", err)
	}

	// Run the verification for the trie (which includes embedded nodes).
	if err := VerifyFileForest(dir, S5LiveConfig, []Root{{root, hash}}, NilVerificationObserver{}); err != nil {
		t.Errorf("Unexpected verification error: %v", err)
	}
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

			err = writeCodes(nil, filepath.Join(dir, "codes.dat"))
			if err != nil {
				t.Fatalf("failed to create codes file: %v", err)
			}

			verify(t, dir, config, roots)
		})
	}
}

func modifyFirstNode[N any](t *testing.T, directory string, encoder stock.ValueEncoder[N], modify func(n *N)) {
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

	modifyNode(t, stock, idx, modify)
}
func modifyLastNode[N any](t *testing.T, directory string, encoder stock.ValueEncoder[N], modify func(n *N)) {
	t.Helper()
	stock, err := file.OpenStock[uint64](encoder, directory)
	if err != nil {
		t.Fatalf("failed to open stock")
	}

	ids, err := stock.GetIds()
	if err != nil {
		t.Fatalf("failed to get stock ids: %v", err)
	}

	idx, found := getLastElementInSet(ids)
	if !found {
		t.SkipNow()
	}

	modifyNode(t, stock, idx, modify)
}

func modifyNode[N any](t *testing.T, stock stock.Stock[uint64, N], idx uint64, modify func(n *N)) {
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
	forestConfig := ForestConfig{Mode: Immutable, CacheCapacity: 1024}
	forest, err := OpenFileForest(dir, config, forestConfig)
	if err != nil {
		return nil, err
	}

	root := NewNodeReference(EmptyId())
	for i := 0; i < N; i++ {
		addr := common.Address{byte(i)}
		root, err = forest.SetAccountInfo(&root, addr, AccountInfo{Nonce: common.ToNonce(1), CodeHash: emptyCodeHash})
		if err != nil {
			return nil, err
		}
		for j := 0; j < N; j++ {
			root, err = forest.SetValue(&root, addr, common.Key{byte(j)}, common.Value{byte(i + j + 1)})
			if err != nil {
				return nil, err
			}
		}
		err = forest.Freeze(&root)
		if err != nil {
			return nil, err
		}
		hash, _, err := forest.updateHashesFor(&root)
		if err != nil {
			return nil, err
		}
		roots = append(roots, Root{
			NodeRef: root,
			Hash:    hash,
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

func getLastElementInSet(set stock.IndexSet[uint64]) (uint64, bool) {
	var latestFound uint64
	for i := set.GetLowerBound(); i < set.GetUpperBound(); i++ {
		if set.Contains(i) {
			latestFound = i
		}
	}
	if latestFound == 0 {
		return 0, false
	}

	return latestFound, true
}
