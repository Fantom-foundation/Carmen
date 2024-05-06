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
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestAccountInfo_EncodingAndDecoding(t *testing.T) {
	infos := []AccountInfo{
		{},
		{common.Nonce{1, 2, 3}, common.Balance{4, 5, 6}, common.Hash{7, 8, 9}},
	}

	encoder := AccountInfoEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	for _, info := range infos {
		encoder.Store(buffer[:], &info)
		restored := AccountInfo{}
		if encoder.Load(buffer[:], &restored); restored != info {
			t.Fatalf("failed to decode info %v: got %v", info, restored)
		}
	}
}
