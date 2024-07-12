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
	p1 := NewMockTwoPhaseCommitParticipant(ctrl)
	p2 := NewMockTwoPhaseCommitParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().Check(TwoPhaseCommit(0)).Return(nil),
		p2.EXPECT().Check(TwoPhaseCommit(0)).Return(nil),
		p1.EXPECT().Prepare(TwoPhaseCommit(1)).Return(nil),
		p2.EXPECT().Prepare(TwoPhaseCommit(1)).Return(nil),
		p1.EXPECT().Commit(TwoPhaseCommit(1)).Return(nil),
		p2.EXPECT().Commit(TwoPhaseCommit(1)).Return(nil),
	)

	coordinator, err := NewTwoPhaseCommitCoordinator(t.TempDir(), p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if want, got := TwoPhaseCommit(0), coordinator.LastCommit(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	commit, err := coordinator.RunCommit()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want, got := TwoPhaseCommit(1), commit; want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}
}

func TestTwoPhaseCommit_CommitIsAbortedIfPreparationFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockTwoPhaseCommitParticipant(ctrl)
	p2 := NewMockTwoPhaseCommitParticipant(ctrl)

	injectedError := fmt.Errorf("injected error")
	gomock.InOrder(
		p1.EXPECT().Check(TwoPhaseCommit(0)).Return(nil),
		p2.EXPECT().Check(TwoPhaseCommit(0)).Return(nil),
		p1.EXPECT().Prepare(TwoPhaseCommit(1)).Return(nil),
		p2.EXPECT().Prepare(TwoPhaseCommit(1)).Return(injectedError),
		p1.EXPECT().Rollback(TwoPhaseCommit(1)).Return(nil),
	)

	coordinator, err := NewTwoPhaseCommitCoordinator(t.TempDir(), p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if want, got := TwoPhaseCommit(0), coordinator.LastCommit(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	_, err = coordinator.RunCommit()
	if !errors.Is(err, injectedError) {
		t.Errorf("unexpected error: %v", err)
	}

	if want, got := TwoPhaseCommit(0), coordinator.LastCommit(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}
}

func TestTwoPhaseCommit_CommitNumberIsPersisted(t *testing.T) {
	dir := t.TempDir()

	for commit := TwoPhaseCommit(0); commit < 10; commit++ {
		coordinator, err := NewTwoPhaseCommitCoordinator(dir)
		if err != nil {
			t.Fatalf("failed to create coordinator: %v", err)
		}
		if want, got := commit, coordinator.LastCommit(); want != got {
			t.Errorf("unexpected last commit: want %d, got %d", want, got)
		}
		newCommit, err := coordinator.RunCommit()
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

	coordinator, err := NewTwoPhaseCommitCoordinator(dir)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := coordinator.RunCommit(); err != nil {
			t.Fatalf("failed to run commit: %v", err)
		}
	}

	ctrl := gomock.NewController(t)
	p1 := NewMockTwoPhaseCommitParticipant(ctrl)
	p2 := NewMockTwoPhaseCommitParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().Check(TwoPhaseCommit(3)).Return(nil),
		p2.EXPECT().Check(TwoPhaseCommit(3)).Return(nil),
	)

	_, err = NewTwoPhaseCommitCoordinator(dir, p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
}

type myData interface {
	Get(int) int
	Set(int) int

	Check(TwoPhaseCommit) error    // < make sure the state has the given commit it could revert to if needed
	Prepare(TwoPhaseCommit) error  // < prepare a commit
	Commit(TwoPhaseCommit) error   // < fix a full state that can be restored
	Rollback(TwoPhaseCommit) error // < undo a prepared commit

	Flush() error // < write current state to disk; Problem: this is messing up the last commit
	Close() error // < close the data structure

	// Idea: have 3 sets of files
	//  - the committed configuration (the configuration to be restored; if there was never a commit, this is the empty state)
	//  - the flushed configuration (stored during shutdown, reset during an init)
	//  - the prepared configuration (during a two-phase commit)
}

var _ TwoPhaseCommitParticipant = myData(nil)
var _ common.FlushAndCloser = myData(nil)
