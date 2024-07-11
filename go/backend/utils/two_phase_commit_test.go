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

	control := NewTwoPhaseCommitCoordinator(p1, p2)

	gomock.InOrder(
		p1.EXPECT().Prepare(TwoPhaseCommit(1)).Return(nil),
		p2.EXPECT().Prepare(TwoPhaseCommit(1)).Return(nil),
		p1.EXPECT().Commit(TwoPhaseCommit(1)).Return(nil),
		p2.EXPECT().Commit(TwoPhaseCommit(1)).Return(nil),
	)

	if want, got := TwoPhaseCommit(0), control.LastCommit(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	commit, err := control.RunCommit()
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

	control := NewTwoPhaseCommitCoordinator(p1, p2)

	injectedError := fmt.Errorf("injected error")
	gomock.InOrder(
		p1.EXPECT().Prepare(TwoPhaseCommit(1)).Return(nil),
		p2.EXPECT().Prepare(TwoPhaseCommit(1)).Return(injectedError),
		p1.EXPECT().Rollback(TwoPhaseCommit(1)).Return(nil),
	)

	if want, got := TwoPhaseCommit(0), control.LastCommit(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	_, err := control.RunCommit()
	if !errors.Is(err, injectedError) {
		t.Errorf("unexpected error: %v", err)
	}

	if want, got := TwoPhaseCommit(0), control.LastCommit(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}
}
