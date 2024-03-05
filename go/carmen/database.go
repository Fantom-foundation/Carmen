package carmen

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"sync"
)

const errDbClosed = common.ConstError("database is already closed")
const errBlockContextRunning = common.ConstError("block context is running")
const errTransactionRunning = common.ConstError("transaction is running")

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
	statedb := state.CreateStateDBUsing(db)
	return openStateDb(db, statedb)
}

func openStateDb(db state.State, statedb state.StateDB) (Database, error) {
	lastBlock, empty, err := statedb.GetArchiveBlockHeight()
	if err != nil && !errors.Is(err, state.NoArchiveError) {
		return nil, errors.Join(
			fmt.Errorf("cannot get archive: %w", err),
			statedb.Close(),
			db.Close(),
		)
	}

	lastBlockSig := int64(lastBlock)
	if empty {
		lastBlockSig = -1
	}
	return &database{
		db:        db,
		state:     statedb,
		lastBlock: lastBlockSig,
	}, nil
}

type database struct {
	db    state.State
	state state.StateDB

	lock           sync.Mutex
	headStateInUse bool
	numQueries     int // number of active history queries
	lastBlock      int64
}

func (db *database) BeginBlock(block uint64) (HeadBlockContext, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return nil, errDbClosed
	}

	if db.headStateInUse {
		return nil, errBlockContextRunning
	}

	if db.lastBlock >= int64(block) {
		return nil, fmt.Errorf("block is not greater than last block: lastBlock: %d >= block: %d", db.lastBlock, block)
	}

	db.headStateInUse = true
	return &headBlockContext{
		commonContext: commonContext{
			db: db,
		},
		block: int64(block),
		state: db.state,
	}, nil
}

func (db *database) AddBlock(block uint64, run func(HeadBlockContext) error) error {
	ctxt, err := db.BeginBlock(block)
	if err != nil {
		return fmt.Errorf("failed to start block %d: %w", block, err)
	}
	if err := run(ctxt); err != nil {
		return errors.Join(
			fmt.Errorf("error while processing block %d: %w", block, err),
			ctxt.Abort())
	}
	return ctxt.Commit()
}

func (db *database) QueryBlock(block uint64, run func(HistoricBlockContext) error) error {
	ctxt, err := db.GetHistoricContext(block)
	if err != nil {
		return fmt.Errorf("failed to start block %d: %w", block, err)
	}

	return errors.Join(
		run(ctxt),
		ctxt.Close(),
	)
}

func (db *database) GetHeadStateHash() (Hash, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return Hash{}, errDbClosed
	}

	hash, err := db.db.GetHash()
	return Hash(hash), err
}

func (db *database) GetArchiveBlockHeight() (int64, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return 0, errDbClosed
	}

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
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return Hash{}, errDbClosed
	}

	state, err := db.db.GetArchiveState(block)

	if err != nil {
		return Hash{}, err
	}
	hash, err := state.GetHash()
	return Hash(hash), err
}

func (db *database) GetHistoricContext(block uint64) (HistoricBlockContext, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return nil, errDbClosed
	}

	if db.lastBlock < int64(block) {
		return nil, fmt.Errorf("block does not exist: lastBlock: %d block: %d", db.lastBlock, block)
	}

	s, err := db.db.GetArchiveState(block)
	if err != nil {
		return nil, err
	}

	db.numQueries++

	return &archiveBlockContext{
		commonContext: commonContext{
			db: db,
		},

		state: state.CreateNonCommittableStateDBUsing(s)}, err
}

func (db *database) StartBulkLoad(block uint64) (BulkLoad, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return nil, errDbClosed
	}

	if db.headStateInUse {
		return nil, errBlockContextRunning
	}

	if db.lastBlock >= int64(block) {
		return nil, fmt.Errorf("block is not greater than last block: lastBlock: %d >= block: %d", db.lastBlock, block)
	}

	db.headStateInUse = true

	return &bulkLoad{
		nested: db.state.StartBulkLoad(block),
		db:     db,
		block:  int64(block),
	}, nil
}

func (db *database) Flush() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	return db.flush()
}

func (db *database) flush() error {
	if db.db == nil {
		return errDbClosed
	}

	return db.state.Flush()
}

func (db *database) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.headStateInUse || db.numQueries > 0 {
		return errBlockContextRunning
	}

	if err := db.flush(); err != nil {
		return err
	}

	if err := db.state.Close(); err != nil {
		return err
	}

	db.db = nil

	return nil
}

func (db *database) moveBlockNumber(block int64) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lastBlock = block
}

func (db *database) releaseHeadState() {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.headStateInUse = false
}

func (db *database) releaseArchiveQuery() {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.numQueries--
}

func (db *database) moveBlockAndReleaseHead(block int64) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.headStateInUse = false
	db.lastBlock = block
}
