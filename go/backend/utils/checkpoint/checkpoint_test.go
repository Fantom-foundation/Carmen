// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/mock/gomock"
)

func TestCheckpointCoordinator_CanHandleSuccessfulCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		p2.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		p1.EXPECT().Prepare(Checkpoint(1)),
		p2.EXPECT().Prepare(Checkpoint(1)),
		p1.EXPECT().Commit(Checkpoint(1)),
		p2.EXPECT().Commit(Checkpoint(1)),
	)

	coordinator, err := NewCoordinator(t.TempDir(), p1, p2)
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

func TestCheckpointCoordinator_CommitIsAbortedIfPreparationFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)

	injectedError := fmt.Errorf("injected error")
	gomock.InOrder(
		p1.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		p2.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		p1.EXPECT().Prepare(Checkpoint(1)),
		p2.EXPECT().Prepare(Checkpoint(1)).Return(injectedError),
		p1.EXPECT().Abort(Checkpoint(1)),
	)

	coordinator, err := NewCoordinator(t.TempDir(), p1, p2)
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

func TestCheckpointCoordinator_ErrorsDuringAbortAreCollected(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)

	injectedCommitError := fmt.Errorf("injected error")
	injectedAbortError := fmt.Errorf("injected error")
	gomock.InOrder(
		p1.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		p2.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		p1.EXPECT().Prepare(Checkpoint(1)),
		p2.EXPECT().Prepare(Checkpoint(1)).Return(injectedCommitError),
		p1.EXPECT().Abort(Checkpoint(1)).Return(injectedAbortError),
	)

	coordinator, err := NewCoordinator(t.TempDir(), p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if want, got := Checkpoint(0), coordinator.GetCurrentCheckpoint(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}

	_, err = coordinator.CreateCheckpoint()
	if !errors.Is(err, injectedCommitError) {
		t.Errorf("missing injected commit error %v, got: %v", injectedCommitError, err)
	}

	if !errors.Is(err, injectedAbortError) {
		t.Errorf("missing injected abort error %v, got: %v", injectedAbortError, err)
	}

	if want, got := Checkpoint(0), coordinator.GetCurrentCheckpoint(); want != got {
		t.Errorf("unexpected last commit: want %d, got %d", want, got)
	}
}

func TestCheckpointCoordinator_CommitNumberIsPersisted(t *testing.T) {
	dir := t.TempDir()

	for commit := Checkpoint(0); commit < 10; commit++ {
		coordinator, err := NewCoordinator(dir)
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

func TestCheckpointCoordinator_ParticipantsAreCheckedForLastCommitNumber(t *testing.T) {
	dir := t.TempDir()

	coordinator, err := NewCoordinator(dir)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := coordinator.CreateCheckpoint(); err != nil {
			t.Fatalf("failed to run commit: %v", err)
		}
	}

	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().GuaranteeCheckpoint(Checkpoint(3)),
		p2.EXPECT().GuaranteeCheckpoint(Checkpoint(3)),
	)

	_, err = NewCoordinator(dir, p1, p2)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
}

func TestCheckpointCoordinator_CreationFailsIfTheProvidedDirectoryLacksWritePermissions(t *testing.T) {
	dir := t.TempDir()

	if _, err := NewCoordinator(dir); err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("failed to get directory info: %v", err)
	}
	defer os.Chmod(dir, info.Mode())

	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatalf("failed to change permissions: %v", err)
	}
	if _, err := NewCoordinator(dir); err == nil {
		t.Errorf("expected coordinator creation to fail since no files can be created in given directory, but it did not")
	}
}

func TestCheckpointCoordinator_CreationFailsIfTheProvidedPathIsNotADirectory(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "file")
	if err := os.WriteFile(path, []byte{}, 0600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if _, err := NewCoordinator(path); err == nil {
		t.Errorf("expected coordinator creation to fail since the provided path is not a directory, but it did not")
	}
}

func TestCheckpointCoordinator_MalformedCommittedCheckPointIsDetected(t *testing.T) {
	dir := t.TempDir()

	// The file is not empty, but it does not contain a valid checkpoint number.
	path := filepath.Join(dir, "committed")
	if err := os.WriteFile(path, []byte{1, 2, 3, 4}, 0600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if _, err := NewCoordinator(dir); err != nil {
		t.Errorf("unexpected error when loading valid commit: %v", err)
	}

	if err := os.WriteFile(path, []byte{1, 2, 3}, 0600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if _, err := NewCoordinator(dir); err == nil {
		t.Errorf("invalid commit number should have been detected, but it was not")
	}
}

func TestCheckpointCoordinator_InconsistentParticipantsAreDetected(t *testing.T) {
	dir := t.TempDir()

	checkpoint := Checkpoint(42)
	if err := createCheckpointFile(filepath.Join(dir, "committed"), checkpoint); err != nil {
		t.Fatalf("failed to write commit: %v", err)
	}

	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().GuaranteeCheckpoint(checkpoint),
		p2.EXPECT().GuaranteeCheckpoint(checkpoint).Return(errors.New("inconsistent state")),
	)

	if _, err := NewCoordinator(dir, p1, p2); err == nil {
		t.Errorf("inconsistent state should have been detected, but it was not")
	}
}

func TestCheckpointCoordinator_FailedCommitLeadsToAbort(t *testing.T) {
	dir := t.TempDir()

	ctrl := gomock.NewController(t)
	participant := NewMockParticipant(ctrl)

	gomock.InOrder(
		participant.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		participant.EXPECT().Prepare(Checkpoint(1)),
		participant.EXPECT().Abort(Checkpoint(1)),
	)

	coordinator, err := NewCoordinator(dir, participant)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	// by placing a prepare file with limit access rights into the directory, the prepare will fail
	if err := os.WriteFile(filepath.Join(dir, "prepare"), []byte{}, 0400); err != nil {
		t.Fatalf("failed to create prepare file: %v", err)
	}

	_, err = coordinator.CreateCheckpoint()
	if err == nil {
		t.Fatalf("expected error, but got none")
	}
}

func TestCheckpointCoordinator_FailedCommitOfParticipantLeadsToAnError(t *testing.T) {
	dir := t.TempDir()

	ctrl := gomock.NewController(t)
	participant := NewMockParticipant(ctrl)

	injectedError := fmt.Errorf("injected error")
	gomock.InOrder(
		participant.EXPECT().GuaranteeCheckpoint(Checkpoint(0)),
		participant.EXPECT().Prepare(Checkpoint(1)),
		participant.EXPECT().Commit(Checkpoint(1)).Return(injectedError),
	)

	coordinator, err := NewCoordinator(dir, participant)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if _, err = coordinator.CreateCheckpoint(); !errors.Is(err, injectedError) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestore_RestoreSignalsAllParticipantsToRestoreLastCheckpoint(t *testing.T) {
	dir := t.TempDir()

	checkpoint := Checkpoint(42)
	if err := createCheckpointFile(filepath.Join(dir, "committed"), checkpoint); err != nil {
		t.Fatalf("failed to write commit file: %v", err)
	}

	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)

	gomock.InOrder(
		p1.EXPECT().Restore(checkpoint),
		p2.EXPECT().Restore(checkpoint),
	)

	if err := Restore(dir, p1, p2); err != nil {
		t.Fatalf("failed to restore: %v", err)
	}
}

func TestRestore_RestoreIssuesAreCollectedAndReported(t *testing.T) {
	dir := t.TempDir()

	ctrl := gomock.NewController(t)
	p1 := NewMockParticipant(ctrl)
	p2 := NewMockParticipant(ctrl)
	p3 := NewMockParticipant(ctrl)
	p4 := NewMockParticipant(ctrl)

	checkpoint := Checkpoint(1)
	if err := createCheckpointFile(filepath.Join(dir, "committed"), checkpoint); err != nil {
		t.Fatalf("failed to write commit file: %v", err)
	}

	issue1 := fmt.Errorf("issue 1")
	issue2 := fmt.Errorf("issue 2")
	gomock.InOrder(
		p1.EXPECT().Restore(checkpoint),
		p2.EXPECT().Restore(checkpoint).Return(issue1),
		p3.EXPECT().Restore(checkpoint).Return(issue2),
		p4.EXPECT().Restore(checkpoint),
	)

	err := Restore(dir, p1, p2, p3, p4)
	if !errors.Is(err, issue1) {
		t.Errorf("missing issue 1: %v", err)
	}
	if !errors.Is(err, issue2) {
		t.Errorf("missing issue 2: %v", err)
	}
}

func TestRestore_RestoreFailsIfThereIsNoCheckpointData(t *testing.T) {
	dir := t.TempDir()

	ctrl := gomock.NewController(t)
	p := NewMockParticipant(ctrl)

	if err := Restore(dir, p); err == nil {
		t.Errorf("restoration should have failed, but it did not: %v", err)
	}
}
