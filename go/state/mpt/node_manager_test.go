package mpt

import (
	"testing"

	"go.uber.org/mock/gomock"
)

func TestNodeManager_ReferenceLifeCycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockNodeStore(ctrl)

	manager := NewNodeManager(10, store)

	id := ValueId(12)
	ref := NewNodeReference(id)

	store.EXPECT().Load(id).Return(EmptyNode{}, nil)

	if want, got := id, ref.Id(); want != got {
		t.Errorf("unexpected ID, wanted %v, got %v", want, got)
	}

	ref = NewNodeReference(id)
	if want, got := id, ref.Id(); want != got {
		t.Errorf("unexpected ID, wanted %v, got %v", want, got)
	}

	read, err := manager.GetReadAccess(&ref)
	if err != nil {
		t.Errorf("failed to get read access: %v", err)
	}
	read.Release()
}

func TestNodeManager_CapacityIsEnforced(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockNodeStore(ctrl)

	store.EXPECT().Load(gomock.Any()).Times(100).Return(EmptyNode{}, nil)

	manager := NewNodeManager(10, store)

	for i := 0; i < 100; i++ {
		id := ValueId(uint64(i))
		ref := NewNodeReference(id)
		read, err := manager.GetReadAccess(&ref)
		if err != nil {
			t.Errorf("failed to get read access: %v", err)
		}
		read.Release()

		size := manager.Size()
		if size > 10 {
			t.Fatalf("manager grow beyond capacity: got %d, capacity %d", size, 10)
		}

		indexSize := len(manager.(*nodeManager).index)
		if indexSize != size {
			t.Fatalf("manager size and index size do not match, pool size is %d and index size is %d", size, indexSize)
		}
	}
}
