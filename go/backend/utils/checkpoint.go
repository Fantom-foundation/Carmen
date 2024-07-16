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

type Checkpoint uint32

type CheckpointCoordinator interface {
	CreateCheckpoint() (Checkpoint, error)
	GetLastCheckpoint() Checkpoint
	Restore() error
}

type CheckpointParticipant interface {
	IsAvailable(Checkpoint) error
	Prepare(Checkpoint) error
	Commit(Checkpoint) error
	Rollback(Checkpoint) error
	Restore(Checkpoint) error
}

type checkpointCoordinator struct {
	path           string
	participants   []CheckpointParticipant
	lastCheckpoint Checkpoint
}

var _ CheckpointCoordinator = &checkpointCoordinator{}

func NewCheckpointCoordinator(path string, participants ...CheckpointParticipant) (*checkpointCoordinator, error) {

	// TODO: make sure path is a write-able directory
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", path, err)
	}

	file, err := os.Open(filepath.Join(path, "committed"))
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
		if err := p.IsAvailable(lastCheckpoint); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &checkpointCoordinator{
		path:           path,
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
				if err := c.participants[j].Rollback(commit); err != nil {
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
			if err := p.Rollback(commit); err != nil {
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
			if err := p.Rollback(commit); err != nil {
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

func (c *checkpointCoordinator) GetLastCheckpoint() Checkpoint {
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
