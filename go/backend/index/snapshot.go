//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package index

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// ---------------------------------- Proof -----------------------------------

// IndexProof is the type of proof used by index snapshots. Each part of an
// index snapshot covers a range of keys in insertion order. The proof contains
// the hash of the index before the first of those keys and after the last of
// those keys has been added. The full snapshot proof is the hash before the
// first key (by definition Hash = 0) and the hash after the last added key.
type IndexProof struct {
	before, after common.Hash
}

func NewIndexProof(before, after common.Hash) *IndexProof {
	return &IndexProof{before, after}
}

func createIndexProofFromData(data []byte) (*IndexProof, error) {
	if len(data) != common.HashSize*2 {
		return nil, fmt.Errorf("invalid encoding of index proof, invalid number of bytes")
	}
	var before, after common.Hash
	copy(before[:], data[0:])
	copy(after[:], data[common.HashSize:])
	return &IndexProof{before, after}, nil
}

func (p *IndexProof) Equal(proof backend.Proof) bool {
	other, ok := proof.(*IndexProof)
	return ok && other.before == p.before && other.after == p.after
}

func (p *IndexProof) ToBytes() []byte {
	res := make([]byte, 0, common.HashSize*2)
	res = append(res, p.before.ToBytes()...)
	res = append(res, p.after.ToBytes()...)
	return res
}

func (p *IndexProof) GetBeforeHash() common.Hash {
	return p.before
}

func (p *IndexProof) GetAfterHash() common.Hash {
	return p.after
}

// ----------------------------------- Part -----------------------------------

// maxBytesPerPart the approximate size aimed for per part.
const maxBytesPerPart = 4096

// GetKeysPerPart computes the number of keys to be stored per part.
func GetKeysPerPart[K any](serializer common.Serializer[K]) int {
	return maxBytesPerPart / serializer.Size()
}

// IndexPart is a part of an index snapshot covering a consecutive range of
// keys in an index, in their insertion order. The keys stored in a part should
// total up to approximately maxBytesPerPart for efficiency reasons. Proofs
// are composed by the hash of the index before the first key and after the
// last key of the range of keys of a part.
type IndexPart[K comparable] struct {
	serializer common.Serializer[K]
	keys       []K
}

func createIndexPartFromData[K comparable](serializer common.Serializer[K], data []byte) (*IndexPart[K], error) {
	if len(data)%serializer.Size() != 0 {
		return nil, fmt.Errorf("invalid encoding of index part, invalid encoding of keys")
	}

	keys := []K{}
	for len(data) > 0 {
		keys = append(keys, serializer.FromBytes(data[0:serializer.Size()]))
		data = data[serializer.Size():]
	}

	return &IndexPart[K]{serializer, keys}, nil
}

func (p *IndexPart[K]) ToBytes() []byte {
	res := make([]byte, 0, len(p.keys)*p.serializer.Size())
	for _, key := range p.keys {
		res = append(res, p.serializer.ToBytes(key)...)
	}
	return res
}

func (p *IndexPart[K]) GetKeys() []K {
	return p.keys
}

// --------------------------------- Snapshot ---------------------------------

// IndexSnapshotSource is the interface to be implemented by Index implementations
// to provide snapshot data. It is a reduced version of the full Snapshot
// interface, freeing implementations from common Index Snapshot requirements.
type IndexSnapshotSource[K comparable] interface {
	GetHash(key_height int) (common.Hash, error)
	GetKeys(from, to int) ([]K, error)
	Release() error
}

// IndexSnapshot is the snapshot format used by all index implementations. Each
// part of the snapshot contains a fixed-length range of keys to improve the
// efficiency of the snapshot processing -- in terms of computation, memory,
// network, and storage costs.
type IndexSnapshot[K comparable] struct {
	serializer common.Serializer[K]
	proof      *IndexProof            // The root proof of the snapshot.
	numKeys    int                    // The number of keys contained in the index during snapshot creation.
	source     IndexSnapshotSource[K] // Abstract access to the index type to support alternative SnapshotData sources.
}

// CreateIndexSnapshotFromIndex creates a new index snapshot utilizing the provided
// source. This factory is intended to be used by Index implementations when creating
// a new snapshot.
func CreateIndexSnapshotFromIndex[K comparable](serializer common.Serializer[K], hash common.Hash, num_keys int, source IndexSnapshotSource[K]) *IndexSnapshot[K] {
	return &IndexSnapshot[K]{serializer, &IndexProof{common.Hash{}, hash}, num_keys, source}
}

// CreateIndexSnapshotFromData creates a new index snapshot utilizing the provided
// snapshot data. This factory is intended to be used by Index implementations to wrap
// snapshot data into a IndexSnapshot to facilitate data Restoration.
func CreateIndexSnapshotFromData[K comparable](serializer common.Serializer[K], data backend.SnapshotData) (*IndexSnapshot[K], error) {
	metadata, err := data.GetMetaData()
	if err != nil {
		return nil, err
	}

	// Metadata contains an after-hash of root proof and 8 bytes for the number of keys.
	if len(metadata) != common.HashSize+8 {
		return nil, fmt.Errorf("invalid index snapshot metadata encoding, invalid number of bytes")
	}

	var after common.Hash
	copy(after[:], metadata[0:common.HashSize])
	numKeys := int(binary.LittleEndian.Uint64(metadata[common.HashSize:]))

	return &IndexSnapshot[K]{serializer, &IndexProof{common.Hash{}, after}, numKeys, &indexSourceFromData[K]{serializer, numKeys, after, data}}, nil
}

func (s *IndexSnapshot[K]) GetRootProof() backend.Proof {
	return s.proof
}

func (s *IndexSnapshot[K]) GetNumParts() int {
	keysPerPart := GetKeysPerPart(s.serializer)
	res := s.numKeys / keysPerPart
	if s.numKeys%keysPerPart > 0 {
		res += 1
	}
	return res
}

func (s *IndexSnapshot[K]) GetProof(part_number int) (backend.Proof, error) {
	keysPerPart := maxBytesPerPart / s.serializer.Size()
	if part_number*keysPerPart > s.numKeys {
		return nil, fmt.Errorf("no such part")
	}

	before, err := s.source.GetHash(part_number * keysPerPart)
	if err != nil {
		return nil, err
	}
	end := (part_number + 1) * keysPerPart
	if end > s.numKeys {
		end = s.numKeys
	}
	after, err := s.source.GetHash(end)
	if err != nil {
		return nil, err
	}

	return &IndexProof{before, after}, nil
}

func (s *IndexSnapshot[K]) GetPart(part_number int) (backend.Part, error) {
	keysPerPart := maxBytesPerPart / s.serializer.Size()
	from := keysPerPart * part_number
	to := keysPerPart * (part_number + 1)
	if to > s.numKeys {
		to = s.numKeys
	}

	keys, err := s.source.GetKeys(from, to)
	if err != nil {
		return nil, err
	}

	return &IndexPart[K]{s.serializer, keys}, nil
}

func (s *IndexSnapshot[K]) GetData() backend.SnapshotData {
	return s
}

func (s *IndexSnapshot[K]) Release() error {
	return s.source.Release()
}

func (s *IndexSnapshot[K]) GetMetaData() ([]byte, error) {
	res := []byte{}
	res = append(res, s.proof.after[:]...)
	res = binary.LittleEndian.AppendUint64(res, uint64(s.numKeys))
	return res, nil
}

func (s *IndexSnapshot[K]) GetProofData(part_number int) ([]byte, error) {
	proof, err := s.GetProof(part_number)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

func (s *IndexSnapshot[K]) GetPartData(part_number int) ([]byte, error) {
	proof, err := s.GetPart(part_number)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

// indexSourceFromData is an implementation of the IndexSnapshotSource adapting
// a SnapshotDataSource to the interface required by the IndexSnapshot implementation.
type indexSourceFromData[K comparable] struct {
	serializer common.Serializer[K]
	numKeys    int
	endHash    common.Hash
	source     backend.SnapshotData
}

func (s *indexSourceFromData[K]) GetHash(key_height int) (common.Hash, error) {
	if key_height == 0 {
		return common.Hash{}, nil
	}

	if key_height == s.numKeys {
		return s.endHash, nil
	}

	if key_height > s.numKeys {
		return common.Hash{}, fmt.Errorf("invalid key height %d, larger than source height %d", key_height, s.numKeys)
	}

	keysPerPart := GetKeysPerPart(s.serializer)
	if key_height%keysPerPart != 0 {
		return common.Hash{}, fmt.Errorf("invalid key height %d, can only reproduce hash at part boundary", key_height)
	}

	part := key_height / keysPerPart
	data, err := s.source.GetProofData(part)
	if err != nil {
		return common.Hash{}, err
	}

	proof, err := createIndexProofFromData(data)
	if err != nil {
		return common.Hash{}, err
	}
	return proof.before, nil
}

func (s *indexSourceFromData[K]) GetKeys(from, to int) ([]K, error) {
	keysPerPart := maxBytesPerPart / s.serializer.Size()
	if from%keysPerPart != 0 {
		return nil, fmt.Errorf("invalid key range, can only start at part boundary")
	}
	if to < from {
		return nil, fmt.Errorf("invalid key range, to smaller than from")
	}
	if to-from > keysPerPart {
		return nil, fmt.Errorf("invalid key range, must fit in a single part")
	}

	partNumber := from / keysPerPart
	data, err := s.source.GetPartData(partNumber)
	if err != nil {
		return nil, err
	}

	part, err := createIndexPartFromData(s.serializer, data)
	if err != nil {
		return nil, err
	}
	if len(part.keys) < to-from {
		return nil, fmt.Errorf("invalid key range, not enough keys in part")
	}
	return part.keys[0 : to-from], nil
}

func (s *indexSourceFromData[K]) Release() error {
	return nil
}

// ----------------------------- SnapshotVerifier -----------------------------

type indexSnapshotVerifier[K comparable] struct {
	serializer common.Serializer[K]
}

func CreateIndexSnapshotVerifier[K comparable](serializer common.Serializer[K]) *indexSnapshotVerifier[K] {
	return &indexSnapshotVerifier[K]{serializer}
}

func (i *indexSnapshotVerifier[K]) VerifyRootProof(data backend.SnapshotData) (backend.Proof, error) {
	snapshot, err := CreateIndexSnapshotFromData(i.serializer, data)
	if err != nil {
		return nil, err
	}

	// Check that proofs are properly chained.
	cur := common.Hash{}
	if cur != snapshot.proof.before {
		return nil, fmt.Errorf("broken proof chain start encountered, wanted %v, got %v", cur, snapshot.proof.after)
	}
	for i := 0; i < snapshot.GetNumParts(); i++ {
		proof, err := snapshot.GetProof(i)
		if err != nil {
			return nil, err
		}
		indexProof := proof.(*IndexProof)
		if indexProof.before != cur {
			return nil, fmt.Errorf("broken proof chain link encountered at step %d, wanted %v, got %v", i, cur, indexProof.before)
		}
		cur = indexProof.after
	}
	if cur != snapshot.proof.after {
		return nil, fmt.Errorf("broken proof chain end encountered, wanted %v, got %v", cur, snapshot.proof.after)
	}
	return snapshot.proof, nil
}

func (i *indexSnapshotVerifier[K]) VerifyPart(_ int, proof, part []byte) error {
	indexProof, err := createIndexProofFromData(proof)
	if err != nil {
		return err
	}
	indexPart, err := createIndexPartFromData(i.serializer, part)
	if err != nil {
		return err
	}

	h := sha256.New()
	cur := indexProof.before
	for _, key := range indexPart.keys {
		h.Reset()
		h.Write(cur[:])
		h.Write(i.serializer.ToBytes(key))
		h.Sum(cur[0:0])
	}
	if cur != indexProof.after {
		return fmt.Errorf("proof does not certify part content")
	}
	return nil
}
