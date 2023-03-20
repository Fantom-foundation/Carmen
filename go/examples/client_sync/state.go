package demo

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// DemoState is the per-client state maintained.
// TODO: replace with real state once snapshotting is supported
type DemoState struct {
	addrs *memory.Index[common.Address, uint64]
	keys  *memory.Index[common.Key, uint64]
}

func newDemoState() *DemoState {
	return &DemoState{
		addrs: memory.NewIndex[common.Address, uint64](common.AddressSerializer{}),
		keys:  memory.NewIndex[common.Key, uint64](common.KeySerializer{}),
	}
}

func (s *DemoState) AddAddress(addr common.Address) {
	s.addrs.GetOrAdd(addr)
}

func (s *DemoState) AddKey(key common.Key) {
	s.keys.GetOrAdd(key)
}

func (s *DemoState) GetProof() (backend.Proof, error) {
	addressProof, err := s.addrs.GetProof()
	if err != nil {
		return nil, err
	}
	keysProof, err := s.keys.GetProof()
	if err != nil {
		return nil, err
	}
	return backend.GetComposedProof([]backend.Proof{addressProof, keysProof}), nil
}

func (s *DemoState) CreateSnapshot() (backend.Snapshot, error) {
	addressSnapshot, err := s.addrs.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	keysSnapshot, err := s.keys.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	return backend.NewComposedSnapshot([]backend.Snapshot{addressSnapshot, keysSnapshot}), nil
}

func (s *DemoState) Restore(data backend.SnapshotData) error {
	subData, _, err := backend.SplitCompositeData(data)
	if err != nil {
		return err
	}
	if len(subData) != 2 {
		return fmt.Errorf("invalid snapshot data format")
	}
	if err = s.addrs.Restore(subData[0]); err != nil {
		return err
	}
	return s.keys.Restore(subData[1])
}

func (s *DemoState) GetSnapshotVerifier(data []byte) (backend.SnapshotVerifier, error) {
	subData, partCounts, err := backend.SplitCompositeMetaData(data)
	if err != nil {
		return nil, err
	}
	verifierA, err := s.addrs.GetSnapshotVerifier(subData[0])
	if err != nil {
		return nil, err
	}
	verifierB, err := s.keys.GetSnapshotVerifier(subData[1])
	if err != nil {
		return nil, err
	}
	return backend.NewComposedSnapshotVerifier([]backend.SnapshotVerifier{verifierA, verifierB}, partCounts), nil
}
