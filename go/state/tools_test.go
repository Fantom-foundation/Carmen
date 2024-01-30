package state_test

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"math/big"
	"testing"
)

func TestExportImportLive(t *testing.T) {
	sourceParams := state.Parameters{
		Variant:   "go-file",
		Schema:    5,
		Archive:   state.NoArchive,
		Directory: t.TempDir(),
	}
	sourceHash := createExampleState(t, sourceParams)

	var b bytes.Buffer
	if err := state.ExportLiveDb(sourceParams, &b); err != nil {
		t.Fatal(err)
	}

	targetParams := sourceParams
	targetParams.Directory = t.TempDir()
	if err := state.ImportLiveDb(targetParams, &b); err != nil {
		t.Fatal(err)
	}

	if err := state.VerifyLiveDb(targetParams, nil); err != nil {
		t.Fatal(err)
	}

	targetState, err := state.NewState(targetParams)
	if err != nil {
		t.Fatal(err)
	}
	targetHash, err := targetState.GetHash()
	if err != nil {
		t.Fatal(err)
	}
	if targetHash != sourceHash {
		t.Error("hash of the exported/imported state does not match")
	}
}

func TestExportImportArchive(t *testing.T) {
	sourceParams := state.Parameters{
		Variant:   "go-file",
		Schema:    5,
		Archive:   state.S5Archive,
		Directory: t.TempDir(),
	}
	sourceHash := createExampleState(t, sourceParams)

	var b bytes.Buffer
	if err := state.ExportLiveDb(sourceParams, &b); err != nil {
		t.Fatal(err)
	}

	targetParams := sourceParams
	targetParams.Directory = t.TempDir()
	if err := state.InitializeArchive(targetParams, &b, 1); err != nil {
		t.Fatal(err)
	}

	if err := state.VerifyArchive(targetParams, nil); err != nil {
		t.Fatal(err)
	}

	targetLiveState, err := state.NewState(targetParams)
	if err != nil {
		t.Fatal(err)
	}
	targetState, err := targetLiveState.GetArchiveState(1)
	if err != nil {
		t.Fatal(err)
	}
	targetHash, err := targetState.GetHash()
	if err != nil {
		t.Fatal(err)
	}
	if targetHash != sourceHash {
		t.Error("hash of the exported/imported state does not match")
	}
}

func createExampleState(t *testing.T, params state.Parameters) common.Hash {
	address1 = common.Address{0x01}
	sourceState, err := state.NewState(params)
	if err != nil {
		t.Fatal(err)
	}
	sourceStateDb := state.CreateStateDBUsing(sourceState)
	sourceStateDb.BeginBlock()
	sourceStateDb.AddBalance(address1, big.NewInt(123))
	sourceStateDb.EndBlock(1)
	hash := sourceStateDb.GetHash()
	if err := sourceStateDb.Close(); err != nil {
		t.Fatal(err)
	}
	return hash
}
