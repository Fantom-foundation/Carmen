package utils

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/exp/slices"
)

//go:generate mockgen -source two_phase_commit.go -destination two_phase_commit_mocks.go -package utils

type TwoPhaseCommit uint32

type TwoPhaseCommitCoordinator interface {
	RunCommit() (TwoPhaseCommit, error)
	LastCommit() TwoPhaseCommit
}

type TwoPhaseCommitParticipant interface {
	Check(TwoPhaseCommit) error
	Prepare(TwoPhaseCommit) error
	Commit(TwoPhaseCommit) error
	Rollback(TwoPhaseCommit) error
}

type twoPhaseCommitCoordinator struct {
	path         string
	participants []TwoPhaseCommitParticipant
	lastCommit   TwoPhaseCommit
}

var _ TwoPhaseCommitCoordinator = &twoPhaseCommitCoordinator{}

func NewTwoPhaseCommitCoordinator(path string, participants ...TwoPhaseCommitParticipant) (*twoPhaseCommitCoordinator, error) {

	// TODO: make sure path is a write-able directory

	file, err := os.Open(filepath.Join(path, "committed"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var lastCommit TwoPhaseCommit
	if err == nil {
		content := make([]byte, 4)
		if _, err := io.ReadFull(file, content); err != nil {
			return nil, err // < TODO: decorate errors
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
		lastCommit = TwoPhaseCommit(binary.BigEndian.Uint32(content))
	}

	errs := []error{}
	for _, p := range participants {
		if err := p.Check(lastCommit); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &twoPhaseCommitCoordinator{
		path:         path,
		participants: slices.Clone(participants),
		lastCommit:   lastCommit,
	}, nil
}

func (c *twoPhaseCommitCoordinator) RunCommit() (TwoPhaseCommit, error) {
	errs := []error{}
	commit := c.lastCommit + 1

	// Signal all participants to prepare. If any participant fails, all
	// previous participants are rolled back to retain at their current
	// state.
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

	c.lastCommit = commit
	return commit, errors.Join(errs...)
}

func (c *twoPhaseCommitCoordinator) LastCommit() TwoPhaseCommit {
	return c.lastCommit
}
