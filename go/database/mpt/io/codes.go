//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public License v3.
//

package io

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/exp/maps"
)

func writeCodes(codes map[common.Hash][]byte, out io.Writer) error {
	// Sort codes for a stable result.
	hashes := maps.Keys(codes)
	sort.Slice(hashes, func(i, j int) bool { return hashes[i].Compare(&hashes[j]) < 0 })
	for _, hash := range hashes {
		code := codes[hash]
		b := []byte{byte('C'), 0, 0}
		binary.BigEndian.PutUint16(b[1:], uint16(len(code)))
		if _, err := out.Write(b); err != nil {
			return fmt.Errorf("output error: %v", err)
		}
		if _, err := out.Write(code); err != nil {
			return fmt.Errorf("output error: %v", err)
		}
	}
	return nil
}

func readCode(in io.Reader) ([]byte, error) {
	length := []byte{0, 0}
	if _, err := io.ReadFull(in, length[:]); err != nil {
		return nil, err
	}
	code := make([]byte, binary.BigEndian.Uint16(length))
	if _, err := io.ReadFull(in, code); err != nil {
		return nil, err
	}
	return code, nil
}
