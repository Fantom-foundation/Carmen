package utils

import (
	"errors"
	"fmt"
	"testing"

	"go.uber.org/mock/gomock"
)

func TestTwoPhaseCommit_CanHandleSuccessfulCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockTwoPhaseCommitParticipant(ctrl)
	p2 := NewMockTwoPhaseCommitParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().Init(TwoPhaseCommit(0)).Return(nil),
		p2.EXPECT().Init(TwoPhaseCommit(0)).Return(nil),
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
		p1.EXPECT().Init(TwoPhaseCommit(0)).Return(nil),
		p2.EXPECT().Init(TwoPhaseCommit(0)).Return(nil),
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

func TestTwoPhaseCommit_ParticipantsAreInitializedWithCommitNumber(t *testing.T) {
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
		p1.EXPECT().Init(TwoPhaseCommit(3)).Return(nil),
		p2.EXPECT().Init(TwoPhaseCommit(3)).Return(nil),
	)

	_, err = NewTwoPhaseCommitCoordinator(dir, p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
}
