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
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"testing"
)

func TestMptConfig_GetEncoders(t *testing.T) {
	for _, config := range allMptConfigs {
		a1, b1, e1, v1 := config.GetEncoders()
		a2, b2, e2, v2 := getExpectedEncoders(config)

		if a1 != a2 {
			t.Errorf("unexpected account node encoder, got %v, want %v", a1, a2)
		}
		if b1 != b2 {
			t.Errorf("unexpected branch node encoder, got %v, want %v", b1, b2)
		}
		if e1 != e2 {
			t.Errorf("unexpected extension node encoder, got %v, want %v", e1, e2)
		}
		if v1 != v2 {
			t.Errorf("unexpected value node encoder, got %v, want %v", v1, v2)
		}
	}
}

func getExpectedEncoders(config MptConfig) (
	stock.ValueEncoder[AccountNode],
	stock.ValueEncoder[BranchNode],
	stock.ValueEncoder[ExtensionNode],
	stock.ValueEncoder[ValueNode],
) {
	switch config.HashStorageLocation {
	case HashStoredWithParent:
		if config.TrackSuffixLengthsInLeafNodes {
			return AccountNodeWithPathLengthEncoderWithChildHash{},
				BranchNodeEncoderWithChildHashes{},
				ExtensionNodeEncoderWithChildHash{},
				ValueNodeWithPathLengthEncoderWithoutNodeHash{}
		}
		return AccountNodeEncoderWithChildHash{},
			BranchNodeEncoderWithChildHashes{},
			ExtensionNodeEncoderWithChildHash{},
			ValueNodeEncoderWithoutNodeHash{}
	case HashStoredWithNode:
		if config.TrackSuffixLengthsInLeafNodes {
			return AccountNodeWithPathLengthEncoderWithNodeHash{},
				BranchNodeEncoderWithNodeHash{},
				ExtensionNodeEncoderWithNodeHash{},
				ValueNodeWithPathLengthEncoderWithNodeHash{}
		}
		return AccountNodeEncoderWithNodeHash{},
			BranchNodeEncoderWithNodeHash{},
			ExtensionNodeEncoderWithNodeHash{},
			ValueNodeEncoderWithNodeHash{}
	default:
		panic(fmt.Sprintf("unknown mode: %v", config.HashStorageLocation))
	}
}
