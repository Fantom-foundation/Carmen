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

package hashtree

import (
	"crypto/sha256"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// ReduceHashes computes the hash of a tree with the given branching factor
// and numPages hashes on the leaf level. Hashes for leaves are fetched on
// demand through the source function.
func ReduceHashes(branchingFactor int, numPages int, source func(int) (common.Hash, error)) (common.Hash, error) {
	if branchingFactor <= 0 {
		return common.Hash{}, fmt.Errorf("invalid branching factor: %v", branchingFactor)
	}

	// If there are no pages, the procedure is simple.
	if numPages <= 0 {
		return common.Hash{}, nil
	}

	if numPages == 1 {
		return source(0)
	}

	paddedSize := numPages
	if paddedSize%branchingFactor != 0 {
		paddedSize = paddedSize + branchingFactor - (paddedSize % branchingFactor)
	}

	// Collect all hashes from the individual pages.
	hashes := make([]common.Hash, paddedSize)
	for i := 0; i < numPages; i++ {
		hash, err := source(i)
		if err != nil {
			return common.Hash{}, err
		}
		hashes[i] = hash
	}

	// Perform the hash reduction.
	h := sha256.New()
	for len(hashes) > 1 {
		for i := 0; i < len(hashes); i += branchingFactor {
			h.Reset()
			for j := 0; j < branchingFactor; j++ {
				h.Write(hashes[i+j][:])
			}
			h.Sum(hashes[i/branchingFactor][0:0])
		}
		hashes = hashes[0 : len(hashes)/branchingFactor]
		for len(hashes) > 1 && len(hashes)%branchingFactor != 0 {
			hashes = append(hashes, common.Hash{})
		}
	}

	return hashes[0], nil

}
