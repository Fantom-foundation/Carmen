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
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/maps"
	"testing"
)

func TestWitnessProof_Extract_and_Merge_Proofs(t *testing.T) {
	ctrl := gomock.NewController(t)

	address1 := common.Address{1}
	address2 := common.Address{2}
	key1 := common.Key{1}
	key2 := common.Key{2}

	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	address1Path := AddressToNibblePath(address1, ctxt)
	address2Path := AddressToNibblePath(address2, ctxt)
	key1Path := KeyToNibblePath(key1, ctxt)
	key2Path := KeyToNibblePath(key2, ctxt)

	// complete tree for the proof
	root, node := ctxt.Build(&Tag{"R", &Branch{
		children: Children{
			address1Path[0]: &Tag{"A_1", &Branch{
				children: Children{
					address1Path[1]: &Tag{"A_2", &Extension{
						path: address1Path[2:50],
						next: &Tag{"A_3", &Account{address: address1, pathLength: 14, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
							storage: &Tag{"K_1", &Branch{
								children: Children{
									key1Path[0]: &Tag{"K_2", &Extension{path: key1Path[1:40],
										next: &Tag{"K_3", &Value{key: key1, length: 24, value: common.Value{0x12}}}}},
									key2Path[0]: &Extension{path: key2Path[1:40],
										next: &Value{key: key2, length: 24, value: common.Value{0x34}}},
								}}},
						}},
					}},
				}}},
			address2Path[0]: &Tag{"B_1", &Branch{
				children: Children{
					address2Path[1]: &Tag{"B_2", &Extension{
						path: address2Path[2:40],
						next: &Tag{"B_3", &Account{address: address2, pathLength: 24, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
							storage: &Branch{
								children: Children{
									key1Path[0]: &Extension{path: key1Path[1:45],
										next: &Value{key: key1, length: 19, value: common.Value{0x12}}},
									key2Path[0]: &Extension{path: key2Path[1:45],
										next: &Value{key: key2, length: 19, value: common.Value{0x34}}},
								},
							},
						}},
					}},
				},
			}},
		}},
	})

	rootHandle := node.GetViewHandle()
	rootHash, dirty := rootHandle.Get().GetHash()
	if dirty {
		t.Fatalf("expected node to be clean")
	}
	rootHandle.Release()

	// create following reference proofs
	// 1. proof that contains only nodes for address1 and key1
	// 2. proof that contains only nodes for address2
	// 3. wide proof that includes both previous proofs
	// 4. total proof that includes all nodes in the tree
	address1Key1Proof := createReferenceProofForLabels(t, ctxt, "R", "A_1", "A_2", "A_3", "K_1", "K_2", "K_3")
	address2Proof := createReferenceProofForLabels(t, ctxt, "R", "B_1", "B_2", "B_3")
	wideProof := createReferenceProofForLabels(t, ctxt, "R", "A_1", "A_2", "A_3", "K_1", "K_2", "K_3", "B_1", "B_2", "B_3")
	totalProof := createReferenceProof(t, ctxt, &root, node)

	// make sure extracted proofs are different
	if address1Key1Proof.Equals(address2Proof) {
		t.Fatalf("address1 and address1Key1 proofs are equal")
	}
	if wideProof.Equals(address2Proof) {
		t.Fatalf("total and address1 proofs are equal")
	}
	if wideProof.Equals(address1Key1Proof) {
		t.Fatalf("total and address1Key1Proof proofs are equal")
	}
	if wideProof.Equals(totalProof) {
		t.Fatalf("total and wide proofs are equal")
	}

	t.Run("Extract", func(t *testing.T) {
		// Test proofs can be extracted and that they match the reference proofs
		extractedProofAddress1Key1, exists := totalProof.Extract(rootHash, address1, key1)
		if !exists {
			t.Errorf("proof for %v %v %v not found", rootHash, address1, key1)
		}
		if got, want := extractedProofAddress1Key1, address1Key1Proof; !got.Equals(want) {
			t.Errorf("unexpected proof: got %v, want %v", got, want)
		}

		extractedProofAddress2, exists := totalProof.Extract(rootHash, address2)
		if !exists {
			t.Errorf("proof for %v %v %v not found", rootHash, address1, key1)
		}
		if got, want := extractedProofAddress2, address2Proof; !got.Equals(want) {
			t.Errorf("unexpected proof: got %v, want %v", got, want)
		}
	})

	t.Run("Merge", func(t *testing.T) {
		mergedProof := MergeProofs(address1Key1Proof, address2Proof)
		if got, want := mergedProof, wideProof; !got.Equals(want) {
			t.Errorf("unexpected proof: got %v, want %v", got, want)
		}
	})
}

func TestWitnessProof_Extract_Various_NodeTypes_NotFoundProofs(t *testing.T) {
	ctrl := gomock.NewController(t)

	address := common.Address{0xAB}
	key := common.Key{0x12}

	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	tests := map[string]struct {
		desc NodeDesc
	}{
		"extensionNode wrong path": {&Extension{
			path: AddressToNibblePath(common.Address{1}, ctxt),
			next: &Empty{},
		}},
		"extensionNode to EmptyNode - path exhausted": {&Extension{
			path: AddressToNibblePath(address, ctxt),
			next: &Empty{},
		}},
		"extensionNode to EmptyNode": {&Extension{
			path: AddressToNibblePath(address, ctxt)[0:30],
			next: &Empty{},
		}},
		"different accountNode": {&Extension{
			path: AddressToNibblePath(address, ctxt)[0:31],
			next: &Branch{
				children: Children{
					AddressToNibblePath(address, ctxt)[31]: &Account{address: common.Address{1}, pathLength: 64, info: AccountInfo{Nonce: common.Nonce{0x01}, Balance: common.Balance{0x02}, CodeHash: common.Hash{0x03}}},
				}},
		}},
		"valueNode key not found": {&Extension{
			path: AddressToNibblePath(address, ctxt)[0:30],
			next: &Account{address: address, pathLength: 34, info: AccountInfo{Nonce: common.Nonce{0x01}, Balance: common.Balance{0x02}, CodeHash: common.Hash{0x03}},
				storage: &Value{key: common.Key{1}, length: 1, value: common.Value{0x01, 0x02, 0x03, 0x04}},
			},
		}},
	}

	extraProof := make(proofDb)
	extraProof[EmptyNodeEthereumHash] = emptyStringRlpEncoded

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			root, node := ctxt.Build(test.desc)
			totalProof := createReferenceProof(t, ctxt, &root, node)

			proofWithEmpty := MergeProofs(totalProof, WitnessProof{extraProof})
			handle := node.GetViewHandle()
			defer handle.Release()

			rootHash, dirty := handle.Get().GetHash()
			if dirty {
				t.Fatalf("expected node to be clean")
			}

			extractedProof, exists := proofWithEmpty.Extract(rootHash, address, key)
			if exists {
				t.Fatalf("proof should not exist")
			}

			// cannot be proven, but the proof must be still complete
			if got, want := extractedProof, totalProof; !got.Equals(want) {
				t.Errorf("unexpected proof: got %v, want %v", got, want)
			}
		})
	}
}

func TestWitnessProof_Extract_Can_Extract_Terminal_Nodes_In_Proof(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)
	address := common.Address{1}
	nibbles := addressToHashedNibbles(address)

	tests := map[string]struct {
		trie NodeDesc // < the structure of the trie
		path []string // < the path to follow to reach the test account
	}{
		"empty": {
			trie: &Tag{"A", &Empty{}},
			path: []string{},
		},
		"wrong account": {
			trie: &Tag{"A", &Account{}},
			path: []string{"A"},
		},
		"branch without account": {
			trie: &Tag{"A", &Branch{children: Children{
				0x1: &Tag{"B", &Empty{}},
				0x2: &Tag{"C", &Account{address: common.Address{2}}},
			}}},
			path: []string{"A"},
		},
		"branch with wrong account": {
			trie: &Tag{"A", &Branch{children: Children{
				nibbles[0]: &Tag{"B", &Account{address: common.Address{2}, pathLength: 1}},
			}}},
			path: []string{"A", "B"},
		},
		"extension with common prefix lead to empty": {
			trie: &Tag{"A", &Extension{
				path: nibbles[0:63],
				next: &Tag{"B", &Branch{children: Children{
					nibbles[63]: &Tag{"C", &Empty{}},
					0x0:         &Tag{"D", &Account{address: address}},
				}},
				}}},
			path: []string{"A", "B"},
		},
		"extension without common prefix": {
			trie: &Tag{"A", &Extension{path: []Nibble{2, 3}}},
			path: []string{"A"},
		},
		"branch node too deep": {
			trie: &Tag{"A", &Extension{
				path: nibbles, // extension node will exhaust the path
				next: &Tag{"B", &Branch{}},
			}},
			path: []string{"A"},
		},
		"nested branch node too deep": {
			trie: &Tag{"A", &Extension{
				path: nibbles[0:63], // branch node will exhaust the path
				next: &Tag{"B", &Branch{children: Children{
					nibbles[63]: &Tag{"C", &Branch{children: Children{
						0: &Tag{"D", &Account{}},
					}}}}},
				}}},
			path: []string{"A", "B", "C"},
		},
		"account node too deep": {
			trie: &Tag{"A", &Extension{
				path: nibbles[0:63], // branch node will exhaust the path
				next: &Tag{"B", &Branch{children: Children{
					nibbles[63]: &Tag{"C", &Account{address: address, pathLength: 1}}},
				}}},
			},
			path: []string{"A", "B", "C"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			root, node := ctxt.Build(test.trie)
			totalProof := createReferenceProof(t, ctxt, &root, node)

			expectedProof := createReferenceProofForLabels(t, ctxt, test.path...)

			handle := node.GetViewHandle()
			defer handle.Release()

			rootHash, _ := handle.Get().GetHash()
			extractedProof, exists := totalProof.Extract(rootHash, address)
			if exists {
				t.Fatalf("proof should not exist")
			}

			// cannot be proven, but the proof must be still complete
			if got, want := extractedProof, expectedProof; !got.Equals(want) {
				t.Errorf("unexpected proof: got %v, want %v", got, want)
			}
		})
	}
}

func TestWitnessProof_Extract_MissingNode_In_Proof(t *testing.T) {
	ctrl := gomock.NewController(t)

	address := common.Address{0xAB, 0xCD, 0xEF}

	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	desc := &Extension{
		path: AddressToNibblePath(address, ctxt)[0:30],
		next: &Account{address: address, pathLength: 10, info: AccountInfo{Nonce: common.Nonce{0x01}, Balance: common.Balance{0x02}, CodeHash: common.Hash{0x03}}},
	}

	root, node := ctxt.Build(desc)
	totalProof := createReferenceProof(t, ctxt, &root, node)
	handle := node.GetViewHandle()
	defer handle.Release()
	rootHash, dirty := handle.Get().GetHash()
	if dirty {
		t.Fatalf("expected node to be clean")
	}
	// remove a non-root node from the proof
	for k := range totalProof.proofDb {
		if k != rootHash {
			delete(totalProof.proofDb, k)
			break
		}
	}

	if _, exists := totalProof.Extract(rootHash, address); exists {
		t.Fatalf("proof should not exist")
	}
}

func TestWitnessProof_Extract_CorruptedRlp_In_Proof(t *testing.T) {
	ctrl := gomock.NewController(t)

	address := common.Address{0xAB, 0xCD, 0xEF}

	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	desc := &Extension{
		path: AddressToNibblePath(address, ctxt)[0:30],
		next: &Account{address: address, pathLength: 10, info: AccountInfo{Nonce: common.Nonce{0x01}, Balance: common.Balance{0x02}, CodeHash: common.Hash{0x03}}},
	}

	root, node := ctxt.Build(desc)
	totalProof := createReferenceProof(t, ctxt, &root, node)
	handle := node.GetViewHandle()
	defer handle.Release()
	rootHash, dirty := handle.Get().GetHash()
	if dirty {
		t.Fatalf("expected node to be clean")
	}
	// corrupt non-root node in the proof
	for k := range totalProof.proofDb {
		if k != rootHash {
			totalProof.proofDb[k] = []byte{0xAA, 0xBB, 0xCC, 0xDD}
			break
		}
	}

	if _, exists := totalProof.Extract(rootHash, address); exists {
		t.Fatalf("proof should not exist")
	}
}

func TestWitnessProof_Extract_EmbeddedNode_In_Proof(t *testing.T) {
	ctrl := gomock.NewController(t)

	address := common.Address{0xAB, 0xCD, 0xEF}
	key := common.Key{0x12, 0x34, 0x56, 0x78}
	var value common.Value
	value[20] = 0x02
	value[21] = 0x04

	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	desc := &Tag{label: "A", nested: &Extension{
		path: AddressToNibblePath(address, ctxt)[0:30],
		next: &Tag{label: "B", nested: &Account{address: address, pathLength: 34, info: AccountInfo{Nonce: common.Nonce{0x01}, Balance: common.Balance{0x02}, CodeHash: common.Hash{0x03}},
			storage: &Tag{label: "C", nested: &Extension{
				path:         KeyToNibblePath(key, ctxt)[0:40],
				nextEmbedded: true,
				next:         &Value{key: key, length: 24, value: value},
			}}},
		}}}

	_, node := ctxt.Build(desc)
	// proof excludes the embedded node
	totalProof := createReferenceProofForLabels(t, ctxt, "A", "B", "C")
	handle := node.GetViewHandle()
	defer handle.Release()

	rootHash, dirty := handle.Get().GetHash()
	if dirty {
		t.Fatalf("expected node to be clean")
	}

	proof, exists := totalProof.Extract(rootHash, address, key)
	if !exists {
		t.Errorf("proof should exist")
	}
	if got, want := proof, totalProof; !got.Equals(want) {
		t.Errorf("unexpected proof: got %v, want %v", got, want)
	}
}

func TestWitnessProof_String(t *testing.T) {
	proof := proofDb{
		common.Hash{0x04}: []byte{0x0D},
		common.Hash{0x02}: []byte{0x0B},
		common.Hash{0x01}: []byte{0x0A},
		common.Hash{0x03}: []byte{0x0C},
	}

	str := "0x0100000000000000000000000000000000000000000000000000000000000000->0x0a\n" +
		"0x0200000000000000000000000000000000000000000000000000000000000000->0x0b\n" +
		"0x0300000000000000000000000000000000000000000000000000000000000000->0x0c\n" +
		"0x0400000000000000000000000000000000000000000000000000000000000000->0x0d\n"

	if got, want := fmt.Sprintf("%s", WitnessProof{proof}), str; got != want {
		t.Errorf("unexpected string: got %v, want %v", got, want)
	}
}

func TestWitnessProof_Is_Valid(t *testing.T) {
	ctrl := gomock.NewController(t)

	address := common.Address{0xAB, 0xCD, 0xEF}

	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	desc := &Extension{
		path: AddressToNibblePath(address, ctxt)[0:30],
		next: &Account{address: address, pathLength: 10, info: AccountInfo{Nonce: common.Nonce{0x01}, Balance: common.Balance{0x02}, CodeHash: common.Hash{0x03}}},
	}

	root, node := ctxt.Build(desc)
	totalProof := createReferenceProof(t, ctxt, &root, node)

	t.Run("valid proof", func(t *testing.T) {
		proof := WitnessProof{maps.Clone(totalProof.proofDb)}
		if !proof.IsValid() {
			t.Fatalf("proof should be valid")
		}
	})

	t.Run("hash mismatch", func(t *testing.T) {
		proof := WitnessProof{maps.Clone(totalProof.proofDb)}
		for k := range proof.proofDb {
			proof.proofDb[k] = []byte{0xAA, 0xBB, 0xCC, 0xDD}
		}
		if proof.IsValid() {
			t.Fatalf("proof should be invalid")
		}
	})
	t.Run("corruptedRlp", func(t *testing.T) {
		proof := WitnessProof{maps.Clone(totalProof.proofDb)}
		corruptRlp := []byte{0xAA, 0xBB, 0xCC, 0xDD}
		hash := common.Keccak256(corruptRlp)
		proof.proofDb[hash] = corruptRlp
		if proof.IsValid() {
			t.Fatalf("proof should be invalid")
		}
	})
}

// createReferenceProofForLabels creates a reference witness proof for the given root node.
// The proof is created simply that all nodes in the MPT subtree are stored in the proof.
// Only Empty nodes are excluded.
func createReferenceProofForLabels(t *testing.T, ctxt *nodeContext, labels ...string) WitnessProof {
	t.Helper()
	proof := proofDb{}
	for _, label := range labels {
		_, shared := ctxt.Get(label)
		handle := shared.GetViewHandle()
		node := handle.Get()
		rlp, err := encodeToRlp(node, ctxt, []byte{})
		if err != nil {
			t.Fatalf("failed to encode node: %v", err)
		}
		hash := common.Keccak256(rlp)
		proof[hash] = rlp
		handle.Release()
	}
	return WitnessProof{proof}
}

// createReferenceProof creates a reference witness proof for the given root node.
// The proof is created simply that all nodes in the MPT subtree are stored in the proof.
func createReferenceProof(t *testing.T, ctxt *nodeContext, root *NodeReference, node *shared.Shared[Node]) WitnessProof {
	t.Helper()
	proof := proofDb{}
	handle := node.GetViewHandle()
	_, err := handle.Get().Visit(ctxt, root, 0, MakeVisitor(func(node Node, info NodeInfo) VisitResponse {
		if _, ok := node.(EmptyNode); ok {
			// nodes that are not correct terminal values are not present in the proof
			return VisitResponseContinue
		}
		rlp, err := encodeToRlp(node, ctxt, []byte{})
		if err != nil {
			t.Fatalf("failed to encode node: %v", err)
		}
		hash := common.Keccak256(rlp)
		proof[hash] = rlp
		return VisitResponseContinue
	}))
	handle.Release()

	if err != nil {
		t.Fatalf("failed to create proof: %v", err)
	}
	return WitnessProof{proof}
}
