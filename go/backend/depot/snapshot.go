//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.
//

package depot

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// ---------------------------------- Proof -----------------------------------

// DepotProof is the type of proof used by depot snapshots. For indiviudal
// pages, this is merely the hash of its content, while for the full snapshot,
// this is the result of the hash reduction using the depot's hash-tree
// reduction algorithm.
type DepotProof struct {
	hash common.Hash
}

func NewProof(hash common.Hash) *DepotProof {
	return &DepotProof{
		hash: hash,
	}
}

func createDepotProofFromData(data []byte) (*DepotProof, error) {
	if len(data) != common.HashSize {
		return nil, fmt.Errorf("invalid encoding of depot proof, invalid number of bytes")
	}
	var hash common.Hash
	copy(hash[:], data[0:])
	return &DepotProof{hash}, nil
}

func (p *DepotProof) Equal(proof backend.Proof) bool {
	other, ok := proof.(*DepotProof)
	return ok && other.hash == p.hash
}

func (p *DepotProof) ToBytes() []byte {
	return p.hash.ToBytes()
}

// ----------------------------------- Part -----------------------------------

// DepotPart is a part of a store snapshot covering exactly one page of values.
// A proof of a part is the hash of the page content, which can be effectively
// obtained from depot implementations.
type DepotPart struct {
	data []byte
}

func createDepotPartFromData(encoded []byte) (*DepotPart, error) {
	return &DepotPart{encoded}, nil
}

func (p *DepotPart) ToBytes() []byte {
	return p.data
}

// --------------------------------- Snapshot ---------------------------------

// DepotSnapshotSource is the interface to be implemented by Depot implementations
// to provide snapshot data. It is a reduced version of the full Snapshot
// interface, freeing implementations from common Depot Snapshot requirements.
type DepotSnapshotSource interface {
	GetHash(page int) (common.Hash, error)
	GetPage(page int) ([]byte, error)
	Release() error
}

// DepotSnapshot is the snapshot format used by all depot implementations. Each
// part of the snapshot contains a page of the depot. Proofs of parts are page
// hashes, and the root proof is the result of the hierarchical reduction of
// the page hashes using the depot's hash-tree algorithm.
type DepotSnapshot struct {
	branchingFactor int                 // The branching factor used in the hash computation.
	proof           *DepotProof         // The root proof of the snapshot.
	numPages        int                 // The number of pages (= parts) in this snapshot.
	source          DepotSnapshotSource // Abstract access to the depot type to support alternative SnapshotData sources.
}

// CreateDepotSnapshotFromDepot creates a new depot snapshot utilizing the provided
// source. This factory is intended to be used by Depot implementations when creating
// a new snapshot.
func CreateDepotSnapshotFromDepot(branchingFactor int, hash common.Hash, numPages int, source DepotSnapshotSource) *DepotSnapshot {
	return &DepotSnapshot{branchingFactor, &DepotProof{hash}, numPages, source}
}

// CreateDepotSnapshotFromData creates a new depot snapshot utilizing the provided
// snapshot data. This factory is intended to be used by Depot implementations to wrap
// snapshot data into a DepotSnapshot to facilitate data restoration.
func CreateDepotSnapshotFromData(data backend.SnapshotData) (*DepotSnapshot, error) {
	metadata, err := data.GetMetaData()
	if err != nil {
		return nil, err
	}

	// Metadata contains the root hash/proof, 2 bytes for the branching factor, and 8 bytes for the number of pages.
	if len(metadata) != common.HashSize+2+8 {
		return nil, fmt.Errorf("invalid depot snapshot metadata encoding, invalid number of bytes")
	}

	var hash common.Hash
	copy(hash[:], metadata[0:common.HashSize])
	metadata = metadata[common.HashSize:]
	branching := int(binary.LittleEndian.Uint16(metadata[0:]))
	metadata = metadata[2:]
	numPages := int(binary.LittleEndian.Uint64(metadata[:]))

	return &DepotSnapshot{branching, &DepotProof{hash}, numPages, &depotSourceFromData{numPages, data}}, nil
}

func (s *DepotSnapshot) GetRootProof() backend.Proof {
	return s.proof
}

func (s *DepotSnapshot) GetNumParts() int {
	return s.numPages
}

func (s *DepotSnapshot) GetBranchingFactor() int {
	return s.branchingFactor
}

func (s *DepotSnapshot) GetProof(partNumber int) (backend.Proof, error) {
	hash, err := s.source.GetHash(partNumber)
	if err != nil {
		return nil, err
	}
	return &DepotProof{hash}, nil
}

func (s *DepotSnapshot) GetPart(partNumber int) (backend.Part, error) {
	data, err := s.source.GetPage(partNumber)
	if err != nil {
		return nil, err
	}
	return createDepotPartFromData(data)
}

func (s *DepotSnapshot) computeRootHash() (common.Hash, error) {
	// Note: This should not use the lazy hash tree infrastructure, since this
	// would require to fetch all the data from the pages. Instead, it should
	// only verify that the proofs of the pages are consistent with the root.
	return hashtree.ReduceHashes(s.branchingFactor, s.numPages, func(page int) (common.Hash, error) {
		proof, err := s.GetProof(page)
		if err != nil {
			return common.Hash{}, err
		}
		return proof.(*DepotProof).hash, nil
	})
}

func (s *DepotSnapshot) GetData() backend.SnapshotData {
	return s
}

func (s *DepotSnapshot) Release() error {
	return s.source.Release()
}

func (s *DepotSnapshot) GetMetaData() ([]byte, error) {
	res := make([]byte, 0, common.HashSize+2+8)
	res = append(res, s.proof.hash[:]...)
	res = binary.LittleEndian.AppendUint16(res, uint16(s.branchingFactor))
	res = binary.LittleEndian.AppendUint64(res, uint64(s.numPages))
	return res, nil
}

func (s *DepotSnapshot) GetProofData(partNumber int) ([]byte, error) {
	proof, err := s.GetProof(partNumber)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

func (s *DepotSnapshot) GetPartData(partNumber int) ([]byte, error) {
	proof, err := s.GetPart(partNumber)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

// depotSourceFromData is an implementation of the DepotSnapshotSource adapting
// a SnapshotData to the interface required by the DepotSnapshot implementation.
type depotSourceFromData struct {
	numPages int
	source   backend.SnapshotData
}

func (s *depotSourceFromData) GetHash(pageNumber int) (common.Hash, error) {
	data, err := s.source.GetProofData(pageNumber)
	if err != nil {
		return common.Hash{}, err
	}
	proof, err := createDepotProofFromData(data)
	if err != nil {
		return common.Hash{}, err
	}
	return proof.hash, nil
}

func (s *depotSourceFromData) GetPage(pageNumber int) ([]byte, error) {
	data, err := s.source.GetPartData(pageNumber)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *depotSourceFromData) Release() error {
	return nil
}

// ----------------------------- SnapshotVerifier -----------------------------

type depotSnapshotVerifier struct {
}

func CreateDepotSnapshotVerifier() *depotSnapshotVerifier {
	return &depotSnapshotVerifier{}
}

func (i *depotSnapshotVerifier) VerifyRootProof(data backend.SnapshotData) (backend.Proof, error) {
	snapshot, err := CreateDepotSnapshotFromData(data)
	if err != nil {
		return nil, err
	}

	hash, err := snapshot.computeRootHash()
	if err != nil {
		return nil, err
	}
	if snapshot.proof.hash != hash {
		return nil, fmt.Errorf("inconsistent root proof encountered")
	}
	return snapshot.proof, nil
}

func (i *depotSnapshotVerifier) VerifyPart(_ int, proof, part []byte) error {
	depotProof, err := createDepotProofFromData(proof)
	if err != nil {
		return err
	}

	h := sha256.New()
	h.Write(part)
	var hash common.Hash
	h.Sum(hash[0:0])
	if hash != depotProof.hash {
		return fmt.Errorf("invalid proof for depot part content")
	}
	return nil
}
