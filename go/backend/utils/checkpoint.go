package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/exp/slices"
)

//go:generate mockgen -source checkpoint.go -destination checkpoint_mocks.go -package utils

// A checkpoint is a monotonically increasing number that is used to identify
// a backup state of a data structure that can be created in coordination with
// multiple participants. The checkpoint is used to ensure that all participants
// have a consistent view of the overall state they are part of.
type Checkpoint uint32

// CheckpointCoordinator is able to coordinate the creation and restoration of
// checkpoints with multiple participants. The coordinator is responsible for
// ensuring that all participants are atomically transitioned to a new checkpoint.
type CheckpointCoordinator interface {
	// GetCurrentCheckpoint returns the last checkpoint that was created. If no
	// checkpoint was created yet, the return value is 0.
	GetCurrentCheckpoint() Checkpoint

	// CreateCheckpoint creates a new checkpoint and transitions all participants
	// to the new checkpoint. If any participant fails, all participants are reverted
	// to retain their current checkpoint.
	CreateCheckpoint() (Checkpoint, error)

	// Restore transitions all participants to the last checkpoint that was created.
	Restore() error
}

// CheckpointParticipant is a participant in a checkpoint that can participate in
// the coordinated creation and restoration of checkpoints.
type CheckpointParticipant interface {
	// GuaranteeCheckpoint requires the participant to check whether a restoration
	// to the given checkpoint is possible. If the participant is not able to restore to
	// the given checkpoint, an error is returned. If the participant finds the given
	// checkpoint as a prepared checkpoint that has not yet been committed, it should
	// be committed. If there is a prepared checkpoint beyond the given checkpoint,
	// it shall be discarded.
	GuaranteeCheckpoint(Checkpoint) error

	// Prepare is called to signal the participant to prepare for a new checkpoint.
	// The new checkpoint shall be created, but not yet committed to be the new
	// primary target for future restore calls.
	Prepare(Checkpoint) error

	// Commit is called to signal the participant to commit the previously prepared
	// checkpoint. Older checkpoints can be discarded. Failing to commit after
	// preparing a checkpoint is considered an unrecoverable issue and the data
	// structure managed by this participant is considered irreversibly corrupted.
	Commit(Checkpoint) error

	// Abort is called to signal the participant to undo the preparation step
	// of a commit started by a Prepare call. The participant should discard the
	// newly created checkpoint and retain the previous checkpoint as the primary
	// target for future restore calls.
	Abort(Checkpoint) error

	// Restore requests this participant to restore the state overed by the given
	// checkpoint -- which may be the last previously committed checkpoint or a
	// prepared checkpoint that has not been aborted or committed yet.
	Restore(Checkpoint) error
}

// checkpointCoordinator implements the CheckpointCoordinator interface using a two-phase
// commit protocol and an atomic file-system operation to ensure recoverability if the
// process crashes during checkpoint creation.
type checkpointCoordinator struct {
	path           string
	participants   []CheckpointParticipant
	lastCheckpoint Checkpoint
}

// NewCheckpointCoordinator creates a new checkpoint coordinator using the given directory
// for retaining coordination information and managing the creation and restoration of checkpoints
// for the given participants. During its creation, it is checked whether all participants
// are in sync regarding their check points. If not, the creation fails.
func NewCheckpointCoordinator(directory string, participants ...CheckpointParticipant) (*checkpointCoordinator, error) {
	// create the directory to be used for coordination if it does not exist yet
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", directory, err)
	}

	// TODO: make sure path is a write-able directory

	file, err := os.Open(filepath.Join(directory, "committed"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var lastCheckpoint Checkpoint
	if err == nil {
		content := make([]byte, 4)
		if _, err := io.ReadFull(file, content); err != nil {
			return nil, err // < TODO: decorate errors
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
		lastCheckpoint = Checkpoint(binary.BigEndian.Uint32(content))
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

	return &checkpointCoordinator{
		path:           directory,
		participants:   slices.Clone(participants),
		lastCheckpoint: lastCheckpoint,
	}, nil
}

func (c *checkpointCoordinator) CreateCheckpoint() (Checkpoint, error) {
	errs := []error{}
	commit := c.lastCheckpoint + 1

	// Signal all participants to prepare. If any participant fails, all
	// previous participants are rolled back to retain their current
	// checkpoint.
	for i, p := range c.participants {
		if err := p.Prepare(commit); err != nil {
			errs = append(errs, err)
			for j := i - 1; j >= 0; j-- {
				if err := c.participants[j].Abort(commit); err != nil {
					errs = append(errs, err)
				}
			}
			return 0, errors.Join(errs...)
		}
	}

	// Prepare atomic commit.
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], uint32(commit))
	prepareFile := filepath.Join(c.path, "prepare")
	if err := os.WriteFile(prepareFile, data[:], 0644); err != nil {
		errs = append(errs, err)
		for _, p := range c.participants {
			if err := p.Abort(commit); err != nil {
				errs = append(errs, err)
			}
		}
		return 0, errors.Join(errs...)
	}

	// Perform atomic commit by renaming the prepare file to the commit file.
	commitFile := filepath.Join(c.path, "committed")
	if err := os.Rename(prepareFile, commitFile); err != nil {
		errs = append(errs, err)
		for _, p := range c.participants {
			if err := p.Abort(commit); err != nil {
				errs = append(errs, err)
			}
		}
		return 0, errors.Join(errs...)
	}

	// Signal all participants to commit. At this point, all participants
	// have to eventually transition to the committed state. If they fail
	// healing has to handle the recovery.
	for _, p := range c.participants {
		if err := p.Commit(commit); err != nil {
			errs = append(errs, err)
		}
	}

	c.lastCheckpoint = commit
	return commit, errors.Join(errs...)
}

func (c *checkpointCoordinator) GetCurrentCheckpoint() Checkpoint {
	return c.lastCheckpoint
}

func (c *checkpointCoordinator) Restore() error {
	var errs []error
	for _, p := range c.participants {
		if err := p.Restore(c.lastCheckpoint); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
