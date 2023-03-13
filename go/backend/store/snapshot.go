package store

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// ---------------------------------- Proof -----------------------------------

// StoreProof is the type of proof used by store snapshots. For indiviudal
// pages, this is merely the hash of its content, while for the full snapshot,
// this is the result of the hash reduction using the store's hash-tree
// reduction algorithm.
type StoreProof struct {
	hash common.Hash
}

func createStoreProofFromData(data []byte) (*StoreProof, error) {
	if len(data) != common.HashSize {
		return nil, fmt.Errorf("invalid encoding of store proof, invalid number of bytes")
	}
	var hash common.Hash
	copy(hash[:], data[0:])
	return &StoreProof{hash}, nil
}

func (p *StoreProof) Equal(proof backend.Proof) bool {
	other, ok := proof.(*StoreProof)
	return ok && other.hash == p.hash
}

func (p *StoreProof) ToBytes() []byte {
	return p.hash.ToBytes()
}

// ----------------------------------- Part -----------------------------------

// StorePart is a part of a store snapshot covering exactly one page of values.
// A proof of a part is the hash of the page content, which can be effectively
// obtained from store implementations.
type StorePart[V any] struct {
	serializer common.Serializer[V]
	proof      *StoreProof
	values     []V
}

func createStorePartFromData[V any](serializer common.Serializer[V], data []byte) (*StorePart[V], error) {
	if len(data) < common.HashSize {
		return nil, fmt.Errorf("invalid encoding of store part, invalid number of bytes")
	}

	proof, err := createStoreProofFromData(data[0:common.HashSize])
	if err != nil {
		return nil, err
	}
	data = data[common.HashSize:]
	if len(data)%serializer.Size() != 0 {
		return nil, fmt.Errorf("invalid encoding of store part, invalid encoding of values")
	}

	values := make([]V, 0, len(data)/serializer.Size())
	for len(data) > 0 {
		values = append(values, serializer.FromBytes(data[0:serializer.Size()]))
		data = data[serializer.Size():]
	}

	return &StorePart[V]{serializer, proof, values}, nil
}

func (p *StorePart[K]) GetProof() backend.Proof {
	return p.proof
}

func (p *StorePart[K]) Verify() bool {
	h := sha256.New()
	for _, value := range p.values {
		h.Write(p.serializer.ToBytes(value))
	}
	var hash common.Hash
	h.Sum(hash[0:0])
	return hash == p.proof.hash
}

func (p *StorePart[V]) ToBytes() []byte {
	res := p.proof.ToBytes()
	for _, value := range p.values {
		res = append(res, p.serializer.ToBytes(value)...)
	}
	return res
}

func (p *StorePart[V]) GetValues() []V {
	return p.values
}

// --------------------------------- Snapshot ---------------------------------

// StoreSnapshotSource is the interface to be implemented by Store implementations
// to provide snapshot data. It is a reduced version of the full Snapshot
// interface, freeing implementations from common Store Snapshot requirements.
type StoreSnapshotSource[V any] interface {
	GetHash(page int) (common.Hash, error)
	GetValues(page int) ([]V, error)
	Release() error
}

// StoreSnapshot is the snapshot format used by all store implementations. Each
// part of the snapshot contains a page of the store. Proofs of parts are page
// hashes, and the root proof is the result of the hierarchical reduction of
// the page hashes using the store's hash-tree algorithm.
type StoreSnapshot[V any] struct {
	serializer      common.Serializer[V]
	branchingFactor int                    // The branching factor used in the hash computation.
	proof           *StoreProof            // The root proof of the snapshot.
	numPages        int                    // The number of pages (= parts) in this snapshot.
	source          StoreSnapshotSource[V] // Abstract access to the store type to support alternative SnapshotData sources.
}

// CreateStoreSnapshotFromStore creates a new store snapshot utilizing the provided
// source. This factory is intended to be used by Store implementations when creating
// a new snapshot.
func CreateStoreSnapshotFromStore[V any](serializer common.Serializer[V], branchingFactor int, hash common.Hash, numPages int, source StoreSnapshotSource[V]) *StoreSnapshot[V] {
	return &StoreSnapshot[V]{serializer, branchingFactor, &StoreProof{hash}, numPages, source}
}

// CreateStoreSnapshotFromData creates a new store snapshot utilizing the provided
// snapshot data. This factory is intended to be used by Store implementations to wrap
// snapshot data into a StoreSnapshot to facilitate data restoration.
func CreateStoreSnapshotFromData[V any](serializer common.Serializer[V], data backend.SnapshotData) (*StoreSnapshot[V], error) {
	metadata, err := data.GetMetaData()
	if err != nil {
		return nil, err
	}

	// Metadata contains the root hash/proof, 2 byte for the branching factor, and 8 bytes for the number of pages.
	if len(metadata) != common.HashSize+2+8 {
		return nil, fmt.Errorf("invalid store snapshot metadata encoding, invalid number of bytes")
	}

	var hash common.Hash
	copy(hash[:], metadata[0:common.HashSize])
	metadata = metadata[common.HashSize:]
	branching := int(binary.LittleEndian.Uint16(metadata[0:]))
	metadata = metadata[2:]
	numPages := int(binary.LittleEndian.Uint64(metadata[:]))

	return &StoreSnapshot[V]{serializer, branching, &StoreProof{hash}, numPages, &storeSourceFromData[V]{serializer, numPages, data}}, nil
}

func (s *StoreSnapshot[V]) GetRootProof() backend.Proof {
	return s.proof
}

func (s *StoreSnapshot[V]) GetNumParts() int {
	return s.numPages
}

func (s *StoreSnapshot[V]) GetProof(partNumber int) (backend.Proof, error) {
	hash, err := s.source.GetHash(partNumber)
	if err != nil {
		return nil, err
	}
	return &StoreProof{hash}, nil
}

func (s *StoreSnapshot[V]) GetPart(partNumber int) (backend.Part, error) {
	proof, err := s.GetProof(partNumber)
	if err != nil {
		return nil, err
	}

	values, err := s.source.GetValues(partNumber)
	if err != nil {
		return nil, err
	}
	return &StorePart[V]{s.serializer, proof.(*StoreProof), values}, nil
}

func (s *StoreSnapshot[V]) VerifyRootProof() error {
	hash, err := s.computeRootHash()
	if err != nil {
		return err
	}
	if s.proof.hash != hash {
		return fmt.Errorf("inconsistent root proof encountered")
	}
	return nil
}

func (s *StoreSnapshot[V]) computeRootHash() (common.Hash, error) {
	// Note: This should not use the lazy hash tree infrastructure, since this
	// would require to fetch all the data from the pages. Instead, it should
	// only verify that the proofs of the pages are consistent with the root.
	return hashtree.ReduceHashes(s.branchingFactor, s.numPages, func(page int) (common.Hash, error) {
		proof, err := s.GetProof(page)
		if err != nil {
			return common.Hash{}, err
		}
		return proof.(*StoreProof).hash, nil
	})
}

func (s *StoreSnapshot[V]) GetData() backend.SnapshotData {
	return s
}

func (s *StoreSnapshot[V]) Release() error {
	return s.source.Release()
}

func (s *StoreSnapshot[V]) GetMetaData() ([]byte, error) {
	res := make([]byte, 0, 32+2+8)
	res = append(res, s.proof.hash[:]...)
	res = binary.LittleEndian.AppendUint16(res, uint16(s.branchingFactor))
	res = binary.LittleEndian.AppendUint64(res, uint64(s.numPages))
	return res, nil
}

func (s *StoreSnapshot[V]) GetProofData(partNumber int) ([]byte, error) {
	proof, err := s.GetProof(partNumber)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

func (s *StoreSnapshot[V]) GetPartData(partNumber int) ([]byte, error) {
	proof, err := s.GetPart(partNumber)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

// storeSourceFromData is an implementation of the StoreSnapshotSource adapting
// a SnapshotData to the interface required by the StoreSnapshot implementation.
type storeSourceFromData[V any] struct {
	serializer common.Serializer[V]
	numPages   int
	source     backend.SnapshotData
}

func (s *storeSourceFromData[V]) GetHash(pageNumber int) (common.Hash, error) {
	data, err := s.source.GetProofData(pageNumber)
	if err != nil {
		return common.Hash{}, err
	}
	proof, err := createStoreProofFromData(data)
	if err != nil {
		return common.Hash{}, err
	}
	return proof.hash, nil
}

func (s *storeSourceFromData[V]) GetValues(pageNumber int) ([]V, error) {
	data, err := s.source.GetPartData(pageNumber)
	if err != nil {
		return nil, err
	}
	part, err := createStorePartFromData(s.serializer, data)
	if err != nil {
		return nil, err
	}
	return part.GetValues(), nil
}

func (s *storeSourceFromData[V]) Release() error {
	return nil
}
