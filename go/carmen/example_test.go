// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/carmen"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"golang.org/x/exp/maps"
)

func ExampleDatabase_AddBlock() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithoutArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Add a new block
	if err := db.AddBlock(5, func(context carmen.HeadBlockContext) error {
		if err := context.RunTransaction(func(context carmen.TransactionContext) error {
			context.CreateAccount(carmen.Address{1})
			context.AddBalance(carmen.Address{1}, carmen.NewAmount(100))
			fmt.Printf("Transaction executed")
			return nil
		}); err != nil {
			log.Fatalf("cannot create transaction: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot add block: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	// Output: Transaction executed
}

func ExampleDatabase_BeginBlock() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithoutArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Begin a new block
	bctx, err := db.BeginBlock(5)
	if err != nil {
		log.Fatalf("cannot begin block: %v", err)
	}

	// Begin a new transaction withing the block
	tctx, err := bctx.BeginTransaction()
	if err != nil {
		log.Fatalf("cannot begin transaction: %v", err)
	}

	tctx.CreateAccount(carmen.Address{1})
	tctx.AddBalance(carmen.Address{1}, carmen.NewAmount(100))

	if err := tctx.Commit(); err != nil {
		log.Fatalf("cannot commit transaction: %v", err)
	}

	if err := bctx.Commit(); err != nil {
		log.Fatalf("cannot commit block: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}
}

func ExampleDatabase_QueryHeadState() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithoutArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Query state information for the current head block
	if err := db.QueryHeadState(func(context carmen.QueryContext) {
		balance := context.GetBalance(carmen.Address{1, 2, 3})
		fmt.Printf("Account balance: %v", balance)
	}); err != nil {
		log.Fatalf("query operation failed: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	// Output: Account balance: 0
}

func ExampleDatabase_QueryHistoricState() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Add a new block
	if err := db.AddBlock(5, func(context carmen.HeadBlockContext) error {
		if err := context.RunTransaction(func(context carmen.TransactionContext) error {
			context.CreateAccount(carmen.Address{1, 2, 3})
			context.AddBalance(carmen.Address{1, 2, 3}, carmen.NewAmount(100))
			return nil
		}); err != nil {
			log.Fatalf("cannot create transaction: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot add block: %v", err)
	}

	// block wait until the archive is in sync
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	// Query state information for the current head block
	if err := db.QueryHistoricState(5, func(context carmen.QueryContext) {
		balance := context.GetBalance(carmen.Address{1, 2, 3})
		if got, want := balance, carmen.NewAmount(100); got != want {
			log.Fatalf("balance does not match: %d != %d", got, want)
		}
		fmt.Printf("Balance of %v is %v\n", carmen.Address{1, 2, 3}, balance)
	}); err != nil {
		log.Fatalf("query operation failed: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	// Output: Balance of [1 2 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] is 100
}

func ExampleDatabase_QueryBlock() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Add a new block
	if err := db.AddBlock(5, func(context carmen.HeadBlockContext) error {
		if err := context.RunTransaction(func(context carmen.TransactionContext) error {
			context.CreateAccount(carmen.Address{1})
			context.AddBalance(carmen.Address{1}, carmen.NewAmount(100))
			return nil
		}); err != nil {
			log.Fatalf("cannot create transaction: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot add block: %v", err)
	}

	// block wait until the archive is in sync
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	// query history block
	if err := db.QueryBlock(5, func(ctxt carmen.HistoricBlockContext) error {
		return ctxt.RunTransaction(func(ctxt carmen.TransactionContext) error {
			balance := ctxt.GetBalance(carmen.Address{1})
			if got, want := balance, carmen.NewAmount(100); got != want {
				log.Fatalf("balance does not match: %d != %d", got, want)
			}
			fmt.Printf("Balance of %v is %v\n", carmen.Address{1}, balance)
			return nil
		})
	}); err != nil {
		log.Fatalf("cannot query block: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}

	// Output: Balance of [1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] is 100
}

func ExampleDatabase_GetHistoricContext() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Add a new block
	if err := db.AddBlock(5, func(context carmen.HeadBlockContext) error {
		if err := context.RunTransaction(func(context carmen.TransactionContext) error {
			context.CreateAccount(carmen.Address{1})
			context.AddBalance(carmen.Address{1}, carmen.NewAmount(100))
			return nil
		}); err != nil {
			log.Fatalf("cannot create transaction: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot add block: %v", err)
	}

	// block wait until the archive is in sync
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	// query history block
	hctx, err := db.GetHistoricContext(5)
	if err != nil {
		log.Fatalf("cannot begin history query: %v", hctx)
	}

	tctx, err := hctx.BeginTransaction()
	if err != nil {
		log.Fatalf("cannot begin transaction: %v", err)
	}

	balance := tctx.GetBalance(carmen.Address{1})
	if got, want := balance, carmen.NewAmount(100); got != want {
		log.Fatalf("balance does not match: %d != %d", got, want)
	}
	fmt.Printf("Balance of %v is %v\n", carmen.Address{1}, balance)

	if err := tctx.Abort(); err != nil {
		log.Fatalf("cannot abort transaction: %v", err)
	}

	if err := hctx.Close(); err != nil {
		log.Fatalf("cannot close block: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}

	// Output: Balance of [1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] is 100
}

func ExampleHistoricBlockContext_GetProof() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// ------- Prepare the database -------

	const N = 10
	// Add N blocks with one address and storage slot each
	for i := 0; i < N; i++ {
		if err := db.AddBlock(uint64(i), func(context carmen.HeadBlockContext) error {
			if err := context.RunTransaction(func(context carmen.TransactionContext) error {
				context.CreateAccount(carmen.Address{byte(i)})
				context.AddBalance(carmen.Address{byte(i)}, carmen.NewAmount(uint64(i)))
				context.SetState(carmen.Address{byte(i)}, carmen.Key{byte(i)}, carmen.Value{byte(i)})
				return nil
			}); err != nil {
				log.Fatalf("cannot create transaction: %v", err)
			}
			return nil
		}); err != nil {
			log.Fatalf("cannot add block: %v", err)
		}
	}

	// block wait until the archive is in sync
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	// ------- Query witness proofs for each block -------

	completeProof := make(map[string]struct{}, 1024)
	// proof each address and key from each block, and merge all in one proof
	for i := 0; i < N; i++ {
		if err := db.QueryBlock(uint64(i), func(ctxt carmen.HistoricBlockContext) error {
			proof, err := ctxt.GetProof(carmen.Address{byte(i)}, carmen.Key{byte(i)})
			if err != nil {
				log.Fatalf("cannot create witness proof: %v", err)
			}

			// proof can be extracted and merged with other proofs
			for _, e := range proof.GetElements() {
				completeProof[e] = struct{}{}
			}

			return nil
		}); err != nil {
			log.Fatalf("cannot query block: %v", err)
		}
	}

	rootHashes := make([]carmen.Hash, N)
	for i := 0; i < N; i++ {
		if err := db.QueryHistoricState(uint64(i), func(ctxt carmen.QueryContext) {
			rootHashes[i] = ctxt.GetStateHash()
		}); err != nil {
			log.Fatalf("cannot query block: %v", err)
		}
	}

	// ------- Close the database - no more needed -------
	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}

	// ------- WitnessProof can be deserialized  -------

	recoveredProof := carmen.CreateWitnessProofFromNodes(maps.Keys(completeProof)...)

	// ------- Properties can be proven offline  -------
	for i := 0; i < N; i++ {
		{
			// query account balance
			balance, complete, err := recoveredProof.GetBalance(rootHashes[i], carmen.Address{byte(i)})
			if err != nil {
				log.Fatalf("cannot get balance: %v", err)
			}
			if !complete {
				log.Fatalf("proof is incomplete")
			}
			fmt.Printf("Balance of address 0x%x at block: %d is 0x%x\n", carmen.Address{byte(i)}, i, balance)
		}
		{
			// query storage slot
			value, complete, err := recoveredProof.GetState(rootHashes[i], carmen.Address{byte(i)}, carmen.Key{byte(i)})
			if err != nil {
				log.Fatalf("cannot get state: %v", err)
			}
			if !complete {
				log.Fatalf("proof is incomplete")
			}
			fmt.Printf("Storage slot value of key 0x%x at block: %d and address: 0x%x is 0x%x\n", carmen.Key{byte(i)}, i, carmen.Address{byte(i)}, value)
		}
	}

	// Output: Balance of address 0x0000000000000000000000000000000000000000 at block: 0 is 0x30
	//Storage slot value of key 0x0000000000000000000000000000000000000000000000000000000000000000 at block: 0 and address: 0x0000000000000000000000000000000000000000 is 0x0000000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0100000000000000000000000000000000000000 at block: 1 is 0x31
	//Storage slot value of key 0x0100000000000000000000000000000000000000000000000000000000000000 at block: 1 and address: 0x0100000000000000000000000000000000000000 is 0x0100000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0200000000000000000000000000000000000000 at block: 2 is 0x32
	//Storage slot value of key 0x0200000000000000000000000000000000000000000000000000000000000000 at block: 2 and address: 0x0200000000000000000000000000000000000000 is 0x0200000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0300000000000000000000000000000000000000 at block: 3 is 0x33
	//Storage slot value of key 0x0300000000000000000000000000000000000000000000000000000000000000 at block: 3 and address: 0x0300000000000000000000000000000000000000 is 0x0300000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0400000000000000000000000000000000000000 at block: 4 is 0x34
	//Storage slot value of key 0x0400000000000000000000000000000000000000000000000000000000000000 at block: 4 and address: 0x0400000000000000000000000000000000000000 is 0x0400000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0500000000000000000000000000000000000000 at block: 5 is 0x35
	//Storage slot value of key 0x0500000000000000000000000000000000000000000000000000000000000000 at block: 5 and address: 0x0500000000000000000000000000000000000000 is 0x0500000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0600000000000000000000000000000000000000 at block: 6 is 0x36
	//Storage slot value of key 0x0600000000000000000000000000000000000000000000000000000000000000 at block: 6 and address: 0x0600000000000000000000000000000000000000 is 0x0600000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0700000000000000000000000000000000000000 at block: 7 is 0x37
	//Storage slot value of key 0x0700000000000000000000000000000000000000000000000000000000000000 at block: 7 and address: 0x0700000000000000000000000000000000000000 is 0x0700000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0800000000000000000000000000000000000000 at block: 8 is 0x38
	//Storage slot value of key 0x0800000000000000000000000000000000000000000000000000000000000000 at block: 8 and address: 0x0800000000000000000000000000000000000000 is 0x0800000000000000000000000000000000000000000000000000000000000000
	//Balance of address 0x0900000000000000000000000000000000000000 at block: 9 is 0x39
	//Storage slot value of key 0x0900000000000000000000000000000000000000000000000000000000000000 at block: 9 and address: 0x0900000000000000000000000000000000000000 is 0x0900000000000000000000000000000000000000000000000000000000000000
}

func ExampleWitnessProof_GetStorageElements() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// ------- Prepare the database -------

	// Add only one block with one address and storage
	if err := db.AddBlock(0, func(context carmen.HeadBlockContext) error {
		if err := context.RunTransaction(func(context carmen.TransactionContext) error {
			context.CreateAccount(carmen.Address{1})
			context.AddBalance(carmen.Address{1}, carmen.NewAmount(1))
			context.SetState(carmen.Address{1}, carmen.Key{1}, carmen.Value{1})
			return nil
		}); err != nil {
			log.Fatalf("cannot create transaction: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot add block: %v", err)
	}

	// block wait until the archive is in sync
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	// ------- Query witness proofs from a block -------

	var elements []string
	// proof each address and key from each block, and merge all in one proof
	if err := db.QueryBlock(0, func(ctxt carmen.HistoricBlockContext) error {
		proof, err := ctxt.GetProof(carmen.Address{1}, carmen.Key{1})
		if err != nil {
			log.Fatalf("cannot create witness proof: %v", err)
		}

		// store proof
		elements = proof.GetElements()
		return nil
	}); err != nil {
		log.Fatalf("cannot query block: %v", err)
	}

	var rootHash carmen.Hash
	if err := db.QueryHistoricState(0, func(ctxt carmen.QueryContext) {
		rootHash = ctxt.GetStateHash()
	}); err != nil {
		log.Fatalf("cannot query block: %v", err)
	}

	// ------- Close the database - no more needed -------
	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}

	// ------- Recover Proof and Split into Account and Storage Proof  -------

	proof := carmen.CreateWitnessProofFromNodes(elements...)
	accountProof, accountComplete := proof.Extract(rootHash, carmen.Address{1})
	storageElements, storageRoot, storageComplete := proof.GetStorageElements(rootHash, carmen.Address{1}, carmen.Key{1})

	fmt.Printf("Account proof is complete: %v and has %d elements\n", accountComplete, len(accountProof.GetElements()))
	fmt.Printf("Storage proof is complete: %v and has %d elements and root: %v\n", storageComplete, len(storageElements), storageRoot)

	// Output: Account proof is complete: true and has 1 elements
	// Storage proof is complete: true and has 1 elements and root: [46 124 116 253 41 173 162 163 11 134 9 5 150 41 208 193 156 91 227 104 90 168 232 46 92 214 249 73 33 194 217 13]
}

func ExampleDatabase_GetMemoryFootprint() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// block wait until the archive is in sync
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	fp := db.GetMemoryFootprint()

	fmt.Printf("Database currently uses %v B", fp.Total())
	fmt.Printf("Memory breakdown:\n%s", fp)

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}
}

func ExampleHistoricBlockContext_Export() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.AddBlock(uint64(1), func(context carmen.HeadBlockContext) error {
		if err := context.RunTransaction(func(context carmen.TransactionContext) error {
			context.CreateAccount(carmen.Address{byte(1)})
			context.AddBalance(carmen.Address{byte(1)}, carmen.NewAmount(uint64(1)))
			context.SetState(carmen.Address{byte(1)}, carmen.Key{byte(1)}, carmen.Value{byte(1)})
			return nil
		}); err != nil {
			log.Fatalf("cannot create transaction: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot add block: %v", err)
	}

	// flush to sync archive up to latest block
	if err := db.Flush(); err != nil {
		log.Fatalf("cannot flush: %v", err)
	}

	var rootHash carmen.Hash
	b := bytes.NewBuffer(nil)
	if err = db.QueryBlock(uint64(1), func(ctxt carmen.HistoricBlockContext) error {
		rootHash, err = ctxt.Export(context.Background(), b)
		if err != nil {
			log.Fatalf("cannot export: %v", err)
		}
		return err
	}); err != nil {
		log.Fatalf("failed to export block: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}

	importedDbPath, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temp dir: %v", err)
	}
	liveDbLocation := filepath.Join(importedDbPath, "live")

	err = io.ImportLiveDb(liveDbLocation, b)
	if err != nil {
		log.Fatalf("cannot import live db")
	}

	// Make sure database is valid
	importedDb, err := carmen.OpenDatabase(importedDbPath, carmen.GetCarmenGoS5WithoutArchiveConfiguration(), nil)
	if err != nil {
		log.Fatalf("cannot open imported database: %v", err)
	}

	if err := importedDb.QueryHistoricState(uint64(1), func(ctxt carmen.QueryContext) {
		if got, want := ctxt.GetStateHash(), rootHash; got != want {
			log.Fatalf("unexpected root hash of imported db\ngot: %s, want: %s", got, want)
		}
	}); err != nil {
		log.Fatalf("cannot query block: %v", err)
	}

	if err := importedDb.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}
	if err := os.RemoveAll(importedDbPath); err != nil {
		log.Fatalf("cannot remove dir: %v", err)
	}
}
