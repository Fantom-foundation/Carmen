// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package backend

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// This file provides a snapshot utility class that enables the composition of
// a list of snapshots into a single, global snapshot. This enables composed
// data structures like Carmen state implementations to leverage the
// implementation of snapshot features of their components without revealing
// its details.

// ComposedSnapshot implements a Snapshot comprised of a list of sub-snapshots.
type ComposedSnapshot struct {
	// The sub-snapshots combined in this snapshot.
	snapshots []Snapshot
	// The overall root proof of the composed snapshot.
	proof *composedSnapshotProof
}

// NewComposedSnapshot creates a snapshot by combining the provided snapshots.
// The resulting snapshot takes ownership of the provided snapshots and will
// release them in case the resulting snapshot is released.
func NewComposedSnapshot(snapshots []Snapshot) *ComposedSnapshot {
	proofs := make([]Proof, 0, len(snapshots))
	for _, snapshot := range snapshots {
		proofs = append(proofs, snapshot.GetRootProof())
	}
	return &ComposedSnapshot{snapshots: snapshots, proof: GetComposedProof(proofs)}
}

func (s *ComposedSnapshot) GetNumParts() int {
	sum := 0
	for _, cur := range s.snapshots {
		sum += cur.GetNumParts()
	}
	return sum
}

func (s *ComposedSnapshot) GetRootProof() Proof {
	return s.proof
}

func (s *ComposedSnapshot) GetProof(part_number int) (Proof, error) {
	for _, snapshot := range s.snapshots {
		if part_number < snapshot.GetNumParts() {
			return snapshot.GetProof(part_number)
		}
		part_number -= snapshot.GetNumParts()
	}
	return nil, fmt.Errorf("no such part")
}

func (s *ComposedSnapshot) GetPart(part_number int) (Part, error) {
	for _, snapshot := range s.snapshots {
		if part_number < snapshot.GetNumParts() {
			return snapshot.GetPart(part_number)
		}
		part_number -= snapshot.GetNumParts()
	}
	return nil, fmt.Errorf("no such part")
}

func (s *ComposedSnapshot) GetData() SnapshotData {
	return s
}

func (e *ComposedSnapshot) Release() error {
	for _, snapshot := range e.snapshots {
		if err := snapshot.Release(); err != nil {
			return err
		}
	}
	return nil
}

func (d *ComposedSnapshot) GetProofData(part_number int) ([]byte, error) {
	proof, err := d.GetProof(part_number)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

func (d *ComposedSnapshot) GetPartData(part_number int) ([]byte, error) {
	part, err := d.GetPart(part_number)
	if err != nil {
		return nil, err
	}
	return part.ToBytes(), nil
}

// GetSnapshots provides access to the list of sub-snapshots.
func (s *ComposedSnapshot) GetSnapshots() []Snapshot {
	return s.snapshots
}

func (d *ComposedSnapshot) GetMetaData() ([]byte, error) {
	if len(d.snapshots) > 255 {
		return nil, fmt.Errorf("currently only up to 255 snapshots are supported in a single composed snapshot")
	}

	// Encode the metadata using the following format:
	//   - 1-byte ... number of sub-snapshots
	//   - 4-byte * num sub-snapshots ... length of metadata of sub-snapshots
	//   - *-bytes ... concatenation of metadata of sub-snapshots
	//   - 8-byte * num sub-snapshots ... the size of the sub-snapshots

	// Collect the meta-data of the sub-snapshots.
	metadata := make([][]byte, 0, len(d.snapshots))
	metadataSize := 0
	for _, snapshot := range d.snapshots {
		cur, err := snapshot.GetData().GetMetaData()
		if err != nil {
			return nil, err
		}
		metadata = append(metadata, cur)
		metadataSize += len(cur)
	}

	// Perform the encoding of the various bits of information.
	res := make([]byte, 0, 1+len(d.snapshots)*12+metadataSize)
	res = append(res, byte(len(d.snapshots)))
	for _, cur := range metadata {
		res = binary.LittleEndian.AppendUint32(res, uint32(len(cur)))
	}
	for _, cur := range metadata {
		res = append(res, cur...)
	}
	for _, snapshot := range d.snapshots {
		res = binary.LittleEndian.AppendUint64(res, uint64(snapshot.GetNumParts()))
	}

	return res, nil
}

// SplitCompositeMetaData divides the provided meta data in a list of meta data for sub-snapshots.
func SplitCompositeMetaData(metadata []byte) ([][]byte, []int, error) {
	// This is the inverse operation to the GetMetaData() encoding above.
	if len(metadata) < 1 {
		return nil, nil, fmt.Errorf("invalid metadata encoding, not enough bytes")
	}
	numEntries := metadata[0]
	metadata = metadata[1:]
	if len(metadata) < 4*int(numEntries) {
		return nil, nil, fmt.Errorf("invalid metadata encoding, invalid metadata length")
	}

	lengths := []uint32{}
	for i := byte(0); i < numEntries; i++ {
		lengths = append(lengths, binary.LittleEndian.Uint32(metadata))
		metadata = metadata[4:]
	}

	splitMetadata := [][]byte{}
	for _, length := range lengths {
		if len(metadata) < int(length) {
			return nil, nil, fmt.Errorf("invalid metadata encoding, data truncated")
		}
		splitMetadata = append(splitMetadata, metadata[0:length])
		metadata = metadata[length:]
	}

	sizes := []int{}
	if len(metadata) < int(numEntries)*8 {
		return nil, nil, fmt.Errorf("invalid metadata encoding, snapshot sizes truncated")
	}
	for i := byte(0); i < numEntries; i++ {
		sizes = append(sizes, int(binary.LittleEndian.Uint64(metadata)))
		metadata = metadata[8:]
	}

	return splitMetadata, sizes, nil
}

// SplitCompositeData divides the provided view of snapshot data in a list of views of
// the sub-snapshots contained in a composed snapshot and their respective part counts.
func SplitCompositeData(data SnapshotData) ([]SnapshotData, []int, error) {
	metadata, err := data.GetMetaData()
	if err != nil {
		return nil, nil, err
	}

	splitMetadata, sizes, err := SplitCompositeMetaData(metadata)
	if err != nil {
		return nil, nil, err
	}

	offset := int(0)
	res := []SnapshotData{}
	for i := 0; i < len(splitMetadata); i++ {
		res = append(res, &offsettedSnapshotData{splitMetadata[i], offset, data})
		offset += sizes[i]
	}

	return res, sizes, nil
}

// offsettedSnapshotData is a utility type to produce sub-snapshot views of
// data based on a snapshot data view of a composed snapshot.
type offsettedSnapshotData struct {
	metadata []byte
	offset   int
	source   SnapshotData
}

func (d *offsettedSnapshotData) GetMetaData() ([]byte, error) {
	return d.metadata, nil
}

func (d *offsettedSnapshotData) GetProofData(part_number int) ([]byte, error) {
	return d.source.GetProofData(d.offset + part_number)
}

func (d *offsettedSnapshotData) GetPartData(part_number int) ([]byte, error) {
	return d.source.GetPartData(d.offset + part_number)
}

// composedSnapshotProof is the proof type used for the root of a composed snapshot.
type composedSnapshotProof struct {
	hash common.Hash
}

// GetComposedProof computes the root proof a composed snapshot would have if
// its sub-snapshots would exhibit the provided proofs.
func GetComposedProof(proofs []Proof) *composedSnapshotProof {
	h := sha256.New()
	for _, proof := range proofs {
		h.Write(proof.ToBytes())
	}
	hash := *(*common.Hash)(h.Sum(nil))
	return &composedSnapshotProof{hash}
}

func (p *composedSnapshotProof) Equal(other Proof) bool {
	ref, ok := other.(*composedSnapshotProof)
	return ok && ref.hash == p.hash
}

func (p *composedSnapshotProof) ToBytes() []byte {
	return p.hash[:]
}

type composedSnapshotVerifier struct {
	numParts []int              // the number of parts of the individual snapshots merged into the composed snapshot
	verifier []SnapshotVerifier // the verifiers for the individual snapshots
}

func NewComposedSnapshotVerifier(verifier []SnapshotVerifier, numParts []int) SnapshotVerifier {
	if len(verifier) != len(numParts) {
		panic("must provide the same number of verifiers and part counts")
	}
	return &composedSnapshotVerifier{numParts, verifier}
}

func (v *composedSnapshotVerifier) VerifyRootProof(data SnapshotData) (Proof, error) {
	subData, _, err := SplitCompositeData(data)
	if err != nil {
		return nil, err
	}

	if len(subData) != len(v.verifier) {
		return nil, fmt.Errorf("invalid snapshot data format")
	}

	// Verify proofs of individual snapshots and collect their root proofs.
	proofs := make([]Proof, 0, len(subData))
	for i, verifier := range v.verifier {
		if proof, err := verifier.VerifyRootProof(subData[i]); err != nil {
			return nil, err
		} else {
			proofs = append(proofs, proof)
		}
	}

	// Compute the root proof of the composit.
	return GetComposedProof(proofs), nil
}

func (v *composedSnapshotVerifier) VerifyPart(number int, proof, part []byte) error {
	for i := 0; i < len(v.verifier); i++ {
		if number < v.numParts[i] {
			return v.verifier[i].VerifyPart(number, proof, part)
		}
		number -= v.numParts[i]
	}
	return fmt.Errorf("no part with number %d", number)
}
