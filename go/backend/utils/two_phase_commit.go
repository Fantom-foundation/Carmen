package utils

import (
	"errors"

	"golang.org/x/exp/slices"
)

//go:generate mockgen -source two_phase_commit.go -destination two_phase_commit_mocks.go -package utils

type TwoPhaseCommit int

type TwoPhaseCommitCoordinator interface {
	RunCommit() (TwoPhaseCommit, error)
	LastCommit() TwoPhaseCommit
}

type TwoPhaseCommitParticipant interface {
	Prepare(TwoPhaseCommit) error
	Commit(TwoPhaseCommit) error
	Rollback(TwoPhaseCommit) error
}

type twoPhaseCommitCoordinator struct {
	participants []TwoPhaseCommitParticipant
	lastCommit   TwoPhaseCommit
}

var _ TwoPhaseCommitCoordinator = &twoPhaseCommitCoordinator{}

func NewTwoPhaseCommitCoordinator(participants ...TwoPhaseCommitParticipant) *twoPhaseCommitCoordinator {
	return &twoPhaseCommitCoordinator{
		participants: slices.Clone(participants),
	}
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

	// TODO: atomic swap of valid commit

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
