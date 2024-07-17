package utils

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"go.uber.org/mock/gomock"
)

func TestTwoPhaseCommit_CanHandleSuccessfulCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockCheckpointParticipant(ctrl)
	p2 := NewMockCheckpointParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().IsAvailable(Checkpoint(0)).Return(nil),
		p2.EXPECT().IsAvailable(Checkpoint(0)).Return(nil),
		p1.EXPECT().Prepare(Checkpoint(1)).Return(nil),
		p2.EXPECT().Prepare(Checkpoint(1)).Return(nil),
		p1.EXPECT().Commit(Checkpoint(1)).Return(nil),
		p2.EXPECT().Commit(Checkpoint(1)).Return(nil),
	)

	coordinator, err := NewCheckpointCoordinator(t.TempDir(), p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if want, got := Checkpoint(0), coordinator.GetCurrentCheckpoint(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	commit, err := coordinator.CreateCheckpoint()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want, got := Checkpoint(1), commit; want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}
}

func TestTwoPhaseCommit_CommitIsAbortedIfPreparationFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockCheckpointParticipant(ctrl)
	p2 := NewMockCheckpointParticipant(ctrl)

	injectedError := fmt.Errorf("injected error")
	gomock.InOrder(
		p1.EXPECT().IsAvailable(Checkpoint(0)).Return(nil),
		p2.EXPECT().IsAvailable(Checkpoint(0)).Return(nil),
		p1.EXPECT().Prepare(Checkpoint(1)).Return(nil),
		p2.EXPECT().Prepare(Checkpoint(1)).Return(injectedError),
		p1.EXPECT().Rollback(Checkpoint(1)).Return(nil),
	)

	coordinator, err := NewCheckpointCoordinator(t.TempDir(), p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if want, got := Checkpoint(0), coordinator.GetCurrentCheckpoint(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	_, err = coordinator.CreateCheckpoint()
	if !errors.Is(err, injectedError) {
		t.Errorf("unexpected error: %v", err)
	}

	if want, got := Checkpoint(0), coordinator.GetCurrentCheckpoint(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}
}

func TestTwoPhaseCommit_CommitNumberIsPersisted(t *testing.T) {
	dir := t.TempDir()

	for commit := Checkpoint(0); commit < 10; commit++ {
		coordinator, err := NewCheckpointCoordinator(dir)
		if err != nil {
			t.Fatalf("failed to create coordinator: %v", err)
		}
		if want, got := commit, coordinator.GetCurrentCheckpoint(); want != got {
			t.Errorf("unexpected last commit: want %d, got %d", want, got)
		}
		newCommit, err := coordinator.CreateCheckpoint()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want, got := commit+1, newCommit; want != got {
			t.Errorf("unexpected last commit: want %d, got %d", want, got)
		}
	}
}

func TestTwoPhaseCommit_ParticipantsAreCheckedForLastCommitNumber(t *testing.T) {
	dir := t.TempDir()

	coordinator, err := NewCheckpointCoordinator(dir)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := coordinator.CreateCheckpoint(); err != nil {
			t.Fatalf("failed to run commit: %v", err)
		}
	}

	ctrl := gomock.NewController(t)
	p1 := NewMockCheckpointParticipant(ctrl)
	p2 := NewMockCheckpointParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().IsAvailable(Checkpoint(3)).Return(nil),
		p2.EXPECT().IsAvailable(Checkpoint(3)).Return(nil),
	)

	_, err = NewCheckpointCoordinator(dir, p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
}

type myData interface {
	Get(int) int
	Set(int) int

	GuaranteeCheckpoint(Checkpoint) error // < make sure the state has the given commit it could revert to if needed
	Prepare(Checkpoint) error             // < prepare a commit
	Commit(Checkpoint) error              // < fix a full state that can be restored
	Abort(Checkpoint) error               // < undo a prepared commit
	Restore(Checkpoint) error             // < restore a prepared commit

	Flush() error // < write current state to disk; Problem: this is messing up the last commit
	Close() error // < close the data structure

	// Idea: have 3 sets of files
	//  - the committed configuration (the configuration to be restored; if there was never a commit, this is the empty state)
	//  - the flushed configuration (stored during shutdown, reset during an init)
	//  - the prepared configuration (during a two-phase commit)
}

var _ CheckpointParticipant = myData(nil)
var _ common.FlushAndCloser = myData(nil)
