package carmen

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
)

func TestHeadBlockContext_CanCreateSequenceOfTransactions(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := state.NewMockStateDB(ctrl)
	state.EXPECT().Check().AnyTimes().Return(nil)

	block := &headBlockContext{
		db:    &database{},
		state: state,
	}

	for i := 0; i < 10; i++ {
		tx, err := block.BeginTransaction(i)
		if err != nil {
			t.Fatalf("failed to create transaction %d: %v", i, err)
		}
		if err := tx.Abort(); err != nil {
			t.Fatalf("failed to abort transaction %d: %v", i, err)
		}
	}
}

func TestHeadBlockContext_CanOnlyCreateOneTransactionAtATime(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := state.NewMockStateDB(ctrl)
	state.EXPECT().Check().AnyTimes().Return(nil)

	block := &headBlockContext{
		db:    &database{},
		state: state,
	}

	tx, err := block.BeginTransaction(1)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	_, err = block.BeginTransaction(2)
	if err == nil {
		t.Fatalf("expected an error, got nothing")
	}

	if err := tx.Abort(); err != nil {
		t.Fatalf("failed to abort transaction: %v", err)
	}
}
