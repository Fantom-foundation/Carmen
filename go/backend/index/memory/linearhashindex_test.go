//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"math/rand"
	"testing"
)

func TestLinearHashIndexImplements(t *testing.T) {
	var memory LinearHashIndex[common.Address, uint32]
	var _ index.Index[common.Address, uint32] = &memory
	var _ io.Closer = &memory
}

func TestLinearHashIndexGetAdd(t *testing.T) {
	memory := NewLinearHashIndex[common.Address, uint32](common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})

	if _, err := memory.Get(A); err != index.ErrNotFound {
		t.Errorf("Value must not exists")
	}

	if val, err := memory.GetOrAdd(A); err != nil || val != 0 {
		t.Errorf("Value must exists")
	}

	if val, err := memory.GetOrAdd(B); err != nil || val != 1 {
		t.Errorf("Value must exists")
	}

	if exists := memory.Contains(A); !exists {
		t.Errorf("Value must exists")
	}

	if exists := memory.Contains(B); !exists {
		t.Errorf("Value must exists")
	}
}

func TestLinearHashIndexGetAddManyItemsExceedsNumBuckets(t *testing.T) {
	numBuckets := 2
	memory := NewLinearHashParamsIndex[common.Address, uint32](numBuckets, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})

	data := make(map[common.Address]uint32)
	for i := 0; i < 100000; i++ {
		n := rand.Intn(1000000)
		key := common.AddressFromNumber(n)
		if _, err := memory.GetOrAdd(key); err != nil {
			t.Errorf("Value must exists")
		}

		// track unique values
		data[key], _ = memory.Get(key)
	}

	// check all vales present
	for expectKey, expectVal := range data {
		if actVal, err := memory.Get(expectKey); err != nil || actVal != expectVal {
			t.Errorf("Expected value does not match atual: %v != %v", actVal, expectVal)
		}
	}

	// check +1 index
	key := common.AddressFromNumber(10000009)
	expected := uint32(len(data))
	if idx, err := memory.GetOrAdd(key); err != nil || idx != expected {
		t.Errorf("Unexpected size: %d != %d", idx, expected)
	}
}
