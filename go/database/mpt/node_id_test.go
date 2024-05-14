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
)

func TestNodeId_DefaultIdIsEmptyId(t *testing.T) {
	if !new(NodeId).IsEmpty() {
		t.Errorf("default node ID is not the ID of the empty node")
	}
}

func TestNodeId_AccountIdsAreAccountIds(t *testing.T) {
	for i := uint64(1); i < 1<<61; i <<= 1 {
		id := AccountId(i)
		if !id.IsAccount() {
			t.Errorf("account ID not properly recognized")
		}
		if got, want := id.Index(), i; got != want {
			t.Errorf("failed to recover id, wanted %d, got %d", want, got)
		}

		if id.IsEmpty() {
			t.Errorf("account ID should not be classified as empty")
		}
		if id.IsBranch() {
			t.Errorf("account ID should not be classified as a branch")
		}
		if id.IsExtension() {
			t.Errorf("account ID should not be classified as a extension")
		}
		if id.IsValue() {
			t.Errorf("account ID should not be classified as a value")
		}
	}
}

func TestNodeId_BranchIdsAreBranchIds(t *testing.T) {
	for i := uint64(1); i < 1<<63; i <<= 1 {
		id := BranchId(i)
		if !id.IsBranch() {
			t.Errorf("branch ID not properly recognized")
		}
		if got, want := id.Index(), i; got != want {
			t.Errorf("failed to recover id, wanted %d, got %d", want, got)
		}

		if id.IsEmpty() {
			t.Errorf("branch ID should not be classified as empty")
		}
		if id.IsAccount() {
			t.Errorf("branch ID should not be classified as a account")
		}
		if id.IsExtension() {
			t.Errorf("branch ID should not be classified as a extension")
		}
		if id.IsValue() {
			t.Errorf("branch ID should not be classified as a value")
		}
	}
}

func TestNodeId_ExtensionIdsAreExtensionIds(t *testing.T) {
	for i := uint64(1); i < 1<<61; i <<= 1 {
		id := ExtensionId(i)
		if !id.IsExtension() {
			t.Errorf("extension ID not properly recognized")
		}
		if got, want := id.Index(), i; got != want {
			t.Errorf("failed to recover id, wanted %d, got %d", want, got)
		}

		if id.IsEmpty() {
			t.Errorf("extension ID should not be classified as empty")
		}
		if id.IsAccount() {
			t.Errorf("extension ID should not be classified as a account")
		}
		if id.IsBranch() {
			t.Errorf("extension ID should not be classified as a branch")
		}
		if id.IsValue() {
			t.Errorf("extension ID should not be classified as a value")
		}
	}
}

func TestNodeId_ValueIdsAreValueIds(t *testing.T) {
	for i := uint64(1); i < 1<<62; i <<= 1 {
		id := ValueId(i)
		if !id.IsValue() {
			t.Errorf("value ID not properly recognized")
		}
		if got, want := id.Index(), i; got != want {
			t.Errorf("failed to recover id, wanted %d, got %d", want, got)
		}

		if id.IsEmpty() {
			t.Errorf("value ID should not be classified as empty")
		}
		if id.IsAccount() {
			t.Errorf("value ID should not be classified as a account")
		}
		if id.IsBranch() {
			t.Errorf("value ID should not be classified as a branch")
		}
		if id.IsExtension() {
			t.Errorf("value ID should not be classified as a extension")
		}
	}
}

func TestNodeID_ValueIDOfZeroNodeIsNotEmptyId(t *testing.T) {
	id := ValueId(0)
	if id.IsEmpty() {
		t.Errorf("zero value ID should not be classfied as the empty id")
	}
	if got, want := id.Index(), uint64(0); got != want {
		t.Errorf("failed to recover index from value id, wanted %d, got %d", want, got)
	}
}

func TestNodeID_EncodingAndDecoding(t *testing.T) {
	var buffer [6]byte
	encoder := NodeIdEncoder{}
	for i := uint64(0); i < 100; i++ {
		for _, id := range []NodeId{NodeId(0), ValueId(i), AccountId(i), BranchId(i), ExtensionId(i)} {
			encoder.Store(buffer[:], &id)
			restored := NodeId(12345)
			if encoder.Load(buffer[:], &restored); restored != id {
				t.Fatalf("failed to decode id %v: got %v", id, restored)
			}
		}
	}
}

func TestNodeID_EncodingAndDecodingPowerOfTwos(t *testing.T) {
	var buffer [6]byte
	encoder := NodeIdEncoder{}
	for i := 0; i < 6*8; i++ {
		id := NodeId(uint64(1) << i)
		encoder.Store(buffer[:], &id)
		restored := NodeId(12345)
		if encoder.Load(buffer[:], &restored); restored != id {
			t.Fatalf("failed to decode id %v: got %v", id, restored)
		}
	}
}
