package s4

import (
	"testing"
)

func TestNodeId_DefaultIdIsEmptyId(t *testing.T) {
	if !new(NodeId).IsEmpty() {
		t.Errorf("default node ID is not the ID of the empty node")
	}
}

func TestNodeId_AccountIdsAreAccountIds(t *testing.T) {
	for i := uint32(1); i < 1<<29; i <<= 1 {
		id := AccountId(i)
		if !id.IsAccount() {
			t.Errorf("account ID not properly recognized")
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
	for i := uint32(1); i < 1<<29; i <<= 1 {
		id := BranchId(i)
		if !id.IsBranch() {
			t.Errorf("branch ID not properly recognized")
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
	for i := uint32(1); i < 1<<31; i <<= 1 {
		id := ExtensionId(i)
		if !id.IsExtension() {
			t.Errorf("extension ID not properly recognized")
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
	for i := uint32(1); i < 1<<31; i <<= 1 {
		id := ValueId(i)
		if !id.IsValue() {
			t.Errorf("value ID not properly recognized")
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
	if got, want := id.Index(), uint32(0); got != want {
		t.Errorf("failed to recover index from value id, wanted %d, got %d", want, got)
	}
}

func TestNodeID_EncodingAndDecoding(t *testing.T) {
	var buffer [4]byte
	encoder := NodeIdEncoder{}
	for i := uint32(0); i < 100; i++ {
		for _, id := range []NodeId{NodeId(0), ValueId(i), AccountId(i), BranchId(i), ExtensionId(i)} {
			if err := encoder.Store(buffer[:], &id); err != nil {
				t.Fatalf("failed to encode id: %v", err)
			}
			restored := NodeId(12345)
			if err := encoder.Load(buffer[:], &restored); err != nil || restored != id {
				t.Fatalf("failed to decode id %v: got %v, err %v", id, restored, err)
			}
		}
	}
}