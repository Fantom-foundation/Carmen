//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public License v3.
//

package gostate

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"testing"
)

func TestScheme5_Archive_And_Live_Must_Be_InSync(t *testing.T) {
	dir := t.TempDir()

	archiveConfig := namedStateConfig{
		config: state.Configuration{
			Variant: VariantGoMemory,
			Schema:  5,
			Archive: state.S5Archive,
		},
		factory: newGoMemoryState,
	}

	addBlock := func(block uint64, db state.State) {
		update := common.Update{
			CreatedAccounts: []common.Address{{byte(block)}},
			Balances:        []common.BalanceUpdate{{common.Address{byte(block)}, common.Balance{byte(100)}}},
		}
		if err := db.Apply(block, update); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	// open and create some blocks including archive
	db, err := archiveConfig.createState(dir)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	const blocks = 10
	for i := 0; i < blocks; i++ {
		addBlock(uint64(i), db)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("cannot close database: %v", err)
	}

	// open as non-archive
	noArchiveConfig := namedStateConfig{
		config: state.Configuration{
			Variant: archiveConfig.config.Variant,
			Schema:  archiveConfig.config.Schema,
			Archive: state.NoArchive,
		},
		factory: archiveConfig.factory,
	}

	// continue adding without the archive
	db, err = noArchiveConfig.createState(dir)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	for i := 0; i < blocks; i++ {
		addBlock(uint64(i+blocks), db)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("cannot close database: %v", err)
	}

	// opening archive should fail as archive and non-archive is not in-sync
	if _, err := archiveConfig.createState(dir); err == nil {
		t.Errorf("opening database should fail")
	}
}

func TestCarmen_Empty_Archive_And_Live_Must_Be_InSync(t *testing.T) {

	dir := t.TempDir()

	archiveConfig := namedStateConfig{
		config: state.Configuration{
			Variant: VariantGoMemory,
			Schema:  5,
			Archive: state.S5Archive,
		},
		factory: newGoMemoryState,
	}

	noArchiveConfig := namedStateConfig{
		config: state.Configuration{
			Variant: archiveConfig.config.Variant,
			Schema:  archiveConfig.config.Schema,
			Archive: state.NoArchive,
		},
		factory: archiveConfig.factory,
	}

	// open and start some blocks as non archive
	db, err := noArchiveConfig.createState(dir)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	const blocks = 10
	for i := 0; i < blocks; i++ {
		block := uint64(i)
		update := common.Update{
			CreatedAccounts: []common.Address{{byte(block)}},
			Balances:        []common.BalanceUpdate{{common.Address{byte(block)}, common.Balance{byte(100)}}},
		}
		if err := db.Apply(block, update); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("cannot close database: %v", err)
	}

	// opening archive should fail as archive and non-archive is not in-sync
	if _, err := archiveConfig.createState(dir); err == nil {
		t.Errorf("opening database should fail")
	}
}
