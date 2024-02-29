package carmen

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/Fantom-foundation/Carmen/go/state"
)

func openDatabase(
	directory string,
	configuration Configuration,
	properties Properties,
) (Database, error) {
	liveCache, err := properties.GetInteger(LiveDBCache, 0)
	if err != nil {
		return nil, err
	}
	archiveCache, err := properties.GetInteger(ArchiveCache, 0)
	if err != nil {
		return nil, err
	}
	params := state.Parameters{
		Directory:    directory,
		Variant:      state.Variant(configuration.Variant),
		Schema:       state.Schema(configuration.Schema),
		Archive:      state.ArchiveType(configuration.Archive),
		LiveCache:    int64(liveCache),
		ArchiveCache: int64(archiveCache),
	}
	db, err := state.NewState(params)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return &database{
		db:    db,
		state: state.CreateStateDBUsing(db),
	}, nil
}

type database struct {
	db    state.State
	state state.StateDB

	headStateInUse atomic.Bool
}

func (db *database) BeginBlock(block uint64) (HeadBlockContext, error) {
	if !db.headStateInUse.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("concurrent block context already open")
	}
	// TODO:
	// - check that the next block is valid
	// - check that the data base is not closed
	return &headBlockContext{
		db:    db,
		block: block,
		state: db.state,
	}, nil
}

func (db *database) AddBlock(block uint64, run func(HeadBlockContext) error) error {
	ctxt, err := db.BeginBlock(block)
	if err != nil {
		return fmt.Errorf("failed to start block %d: %w", block, err)
	}
	if err := run(ctxt); err != nil {
		ctxt.Abort()
		return fmt.Errorf("error while processing block %d: %w", block, err)
	}
	return ctxt.Commit()
}

func (db *database) QueryBlock(block uint64, run func(HistoricBlockContext) error) error {
	ctxt, err := db.GetHistoricContext(block)
	if err != nil {
		return fmt.Errorf("failed to start block %d: %w", block, err)
	}
	defer ctxt.Close()
	return run(ctxt)
}

func (db *database) GetHeadStateHash() (Hash, error) {
	hash, err := db.db.GetHash()
	return Hash(hash), err
}

func (db *database) GetBlockHeight() (int64, error) {
	height, empty, err := db.state.GetArchiveBlockHeight()
	if err != nil {
		return 0, err
	}
	if empty {
		return -1, err
	}
	return int64(height), err
}

func (db *database) GetHistoricStateHash(block uint64) (Hash, error) {
	state, err := db.db.GetArchiveState(block)
	if err != nil {
		return Hash{}, err
	}
	hash, err := state.GetHash()
	return Hash(hash), err
}

func (db *database) GetHistoricContext(block uint64) (HistoricBlockContext, error) {
	s, err := db.db.GetArchiveState(block)
	if err != nil {
		return nil, err
	}
	return &archiveBlockContext{state.CreateNonCommittableStateDBUsing(s)}, err
}

func (db *database) StartBulkLoad(block uint64) (BulkLoad, error) {
	// TODO: check and test the following
	// - there is no concurrent transaction or bulk-load operation
	// - the target block is valid
	return &bulkLoad{db.state.StartBulkLoad(block)}, nil
}

func (db *database) Flush() error {
	if db.headStateInUse.Load() {
		return fmt.Errorf("can not flush while there is an open block context")
	}
	return db.state.Flush()
}

func (db *database) Close() error {
	return errors.Join(
		db.Flush(),
		db.state.Close(),
	)
}
