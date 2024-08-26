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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/exp/slices"
)

//go:generate mockgen -source checkpoint.go -destination checkpoint_mocks.go -package checkpoint

// A checkpoint is a monotonically increasing number that is used to identify
// a backup state of a data structure that can be created in coordination with
// multiple participants. The checkpoint is used to ensure that all participants
// have a consistent view of the overall state they are part of.
type Checkpoint uint32

// Coordinator is able to coordinate the creation and restoration of
// checkpoints with multiple participants. The coordinator is responsible for
// ensuring that all participants are atomically transitioned to a new checkpoint.
type Coordinator interface {
	// GetCurrentCheckpoint returns the last checkpoint that was created. If no
	// checkpoint was created yet, the return value is 0.
	GetCurrentCheckpoint() Checkpoint

	// CreateCheckpoint creates a new checkpoint and transitions all participants
	// to the new checkpoint. If any participant fails, all participants are reverted
	// to retain their current checkpoint.
	CreateCheckpoint() (Checkpoint, error)
}

// Participant engages in a coordinated creation and restoration of
// checkpoints.
type Participant interface {
	// GuaranteeCheckpoint requires the participant to check whether a restoration
	// to the given checkpoint is possible. If the participant is not able to restore to
	// the given checkpoint, an error is returned. If the participant finds the given
	// checkpoint as a prepared checkpoint that has not yet been committed, the call
	// should perform the commit. If there is a prepared checkpoint beyond the given
	// checkpoint, it can be discarded.
	GuaranteeCheckpoint(Checkpoint) error

	// Prepare is called to signal the participant to prepare for a new checkpoint.
	// The new checkpoint shall be created, but not yet committed to be the new
	// target for future restore calls.
	Prepare(Checkpoint) error

	// Commit is called to signal the participant to commit the previously prepared
	// checkpoint. Older checkpoints can be discarded. Failing to commit after
	// preparing a checkpoint is considered an unrecoverable issue and the data
	// structure managed by this participant is considered irreversibly corrupted.
	Commit(Checkpoint) error

	// Abort is called to signal the participant to undo the preparation step
	// of a commit started by a Prepare call. The participant should discard the
	// newly created checkpoint and retain the previous checkpoint as the target
	// for future restore calls.
	Abort(Checkpoint) error
}

// Restorer is able to restore a previously created checkpoint for
// a participating data structure.
type Restorer interface {
	// Restore reverts the participant to the given checkpoint. If the participant
	// is not able to restore to the given checkpoint, an error is returned.
	Restore(Checkpoint) error
}

// Restore restores the given participants to the last checkpoint that was created
// by a coordinator which was using the given directory for tracking checkpoints.
func Restore(directory string, participants ...Restorer) error {
	lastCheckpoint, err := readCheckpointFile(filepath.Join(directory, "committed"))
	if err != nil {
		return fmt.Errorf("failed to read checkpoint to be restored: %w", err)
	}

	errs := []error{}
	for _, p := range participants {
		if err := p.Restore(lastCheckpoint); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// coordinator implements the CheckpointCoordinator interface using a two-phase
// commit protocol and an atomic file-system operation to ensure recoverability if the
// process crashes during checkpoint creation.
type coordinator struct {
	path           string
	participants   []Participant
	lastCheckpoint Checkpoint
}

// NewCoordinator creates a new checkpoint coordinator using the given directory
// for retaining coordination information and managing the creation and restoration of checkpoints
// for the given participants. During its creation, it is checked whether all participants
// are in sync regarding their check points. If not, the creation fails.
func NewCoordinator(directory string, participants ...Participant) (*coordinator, error) {
	// create the directory to be used for coordination if it does not exist yet
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", directory, err)
	}

	// Check that directory provides required permissions.
	testFile := filepath.Join(directory, "test")
	err := errors.Join(
		createCheckpointFile(testFile, 42),
		os.Remove(testFile),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to access directory %s: %w", directory, err)
	}

	lastCheckpoint, err := readCheckpointFile(filepath.Join(directory, "committed"))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read last checkpoint: %w", err)
		}
		lastCheckpoint = 0
	}

	errs := []error{}
	for _, p := range participants {
		if err := p.GuaranteeCheckpoint(lastCheckpoint); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &coordinator{
		path:           directory,
		participants:   slices.Clone(participants),
		lastCheckpoint: lastCheckpoint,
	}, nil
}

func (c *coordinator) CreateCheckpoint() (Checkpoint, error) {
	commit := c.lastCheckpoint + 1

	prepared := make([]Participant, 0, len(c.participants))
	abort := func() error {
		errs := []error{}
		for _, p := range prepared {
			if err := p.Abort(commit); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}

	// Signal all participants to prepare. If any participant fails, all
	// previous participants are rolled back to retain their current
	// checkpoint.
	for _, p := range c.participants {
		if err := p.Prepare(commit); err != nil {
			return 0, errors.Join(err, abort())
		}
		prepared = append(prepared, p)
	}

	// Prepare and commit the checkpoint file atomically.
	prepareFile := filepath.Join(c.path, "prepare")
	commitFile := filepath.Join(c.path, "committed")
	err := errors.Join(
		createCheckpointFile(prepareFile, commit),
		// Renaming files in the same directory is (in most cases) atomic on POSIX systems.
		os.Rename(prepareFile, commitFile),
	)
	if err != nil {
		return 0, errors.Join(err, abort())
	}

	// Signal all participants to commit. At this point, all participants
	// have to eventually transition to the committed state. If they fail
	// healing has to handle the recovery.
	errs := []error{}
	for _, p := range c.participants {
		if err := p.Commit(commit); err != nil {
			errs = append(errs, err)
		}
	}

	c.lastCheckpoint = commit
	return commit, errors.Join(errs...)
}

func (c *coordinator) GetCurrentCheckpoint() Checkpoint {
	return c.lastCheckpoint
}

func createCheckpointFile(path string, checkpoint Checkpoint) error {
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], uint32(checkpoint))
	return os.WriteFile(path, data[:], 0600)
}

func readCheckpointFile(path string) (Checkpoint, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	content := make([]byte, 4)
	if _, err := io.ReadFull(file, content); err != nil {
		return 0, errors.Join(err, file.Close())
	}
	return Checkpoint(binary.BigEndian.Uint32(content)), file.Close()
}
