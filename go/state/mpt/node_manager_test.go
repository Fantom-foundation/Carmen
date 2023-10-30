package mpt

import (
	"testing"

	"go.uber.org/mock/gomock"
)

func TestNodePool_ReferenceLifeCycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := NewMockNodePoolSource(ctrl)

	pool := NewNodePool(10, source)

	id := ValueId(12)
	ref := NewNodeReference(id)

	source.EXPECT().fetch(id).Return(EmptyNode{}, nil)

	if want, got := id, ref.Id(); want != got {
		t.Errorf("unexpected ID, wanted %v, got %v", want, got)
	}

	ref = NewNodeReference(id)
	if want, got := id, ref.Id(); want != got {
		t.Errorf("unexpected ID, wanted %v, got %v", want, got)
	}

	read, err := ref.GetReadAccess(pool)
	if err != nil {
		t.Errorf("failed to get read access: %v", err)
	}
	read.Release()
}

func TestNodePool_CapacityIsEnforced(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := NewMockNodePoolSource(ctrl)

	source.EXPECT().fetch(gomock.Any()).Times(100).Return(EmptyNode{}, nil)

	pool := NewNodePool(10, source)

	for i := 0; i < 100; i++ {
		id := ValueId(uint64(i))
		ref := NewNodeReference(id)
		read, err := ref.GetReadAccess(pool)
		if err != nil {
			t.Errorf("failed to get read access: %v", err)
		}
		read.Release()

		size := pool.Size()
		if size > 10 {
			t.Fatalf("pool grow beyond capacity: got %d, capacity %d", size, 10)
		}

		indexSize := len(pool.(*nodePool).index)
		if indexSize != size {
			t.Fatalf("pool size and index size do not match, pool size is %d and index size is %d", size, indexSize)
		}
	}
}
