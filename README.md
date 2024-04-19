# Introduction

Carmen is a fast and space conservative database for blockchains. It outperforms other projects in the
transaction speed while consuming only a fraction of space of what is standard in other projects.

Carmen maintains the world state known from Ethereum space, modelling accounts and storages. It is a
versioning database that allows for modification of a current state and querying of historical states.

The implementation enables various schemes and two implementation languages. At the moment, the production
quality schema is state hash root compatible with Ethereum’s World State, organised in the Merkle Patricia Trie.

A few experimental schemas are provided that utilise fast flat storages with various organisation
of key value pairs. A version implemented in Golang and C++ is provided for a selection of schemas.

***

# How to Build
1. Clone the repository
   ```
   git clone https://github.com/Fantom-foundation/Carmen
   ```
2. Run tests
   * [Go tests](https://github.com/Fantom-foundation/Carmen/tree/main/go#development)
   * [C++ tests](https://github.com/Fantom-foundation/Carmen/blob/main/cpp/README.md#build-and-test)
***

# How to Integrate
1. Get the latest `Go` version of `Carmen`
```
go get -u github.com/Fantom-foundation/Carmen/go
```
2. Import the public interface
```
import "github.com/Fantom-foundation/Carmen/go/carmen"
```


# How to Use
Carmen is configured for the scheme to use, and if historical data are retained or not. The Merkle-Patricia-Trie 
compatible scheme is called S5, and it can be either Archive or non-Archive (LiveDB).

A state of the blockchain may be updated always only at the head of the chain, and it is stored in the LiveDB. 
The state becomes verbatim as part of a block once appended to the blockchain via the new block. If the Archive mode 
is enabled, the state of the last block is appended to the archive and it can be retrieved later. If the Archive 
is not configured, only the head state is available. LiveDB is enabled all the time, while the Archive is tentative.

As a blockchain is a sequence of blocks where each contains a set of transactions, the API is oriented 
to creation and retrieval of transactions within blocks. Carmen provides an API for accessing both Live 
and Archive databases to append new blocks to the blockchain and to query history for previous blocks.

## LiveDB

The current state can be updated or queried via a new block. Two forms of APIs are provided:

<details>
    <summary>Functional style API</summary>

```
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
			context.AddBalance(carmen.Address{1}, big.NewInt(100))
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
```
</details>

<details>
    <summary>Imperative style API</summary>

```
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
	tctx.AddBalance(carmen.Address{1}, big.NewInt(100))

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
```
</details>


## Archive

Historic state can be queried via any previously stored blocks. Two forms of APIs are provided: \

<details>
    <summary>Functional style API</summary>

```
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
			context.AddBalance(carmen.Address{1}, big.NewInt(100))
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
			if got, want := balance, big.NewInt(100); got.Cmp(want) != 0 {
				log.Fatalf("balance does not match: %d != %d", got, want)
			}
			fmt.Printf("Balance of %v is %d\n", carmen.Address{1}, balance)
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
```
</details>

<details>
    <summary>Imperative style API</summary>

```
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
			context.AddBalance(carmen.Address{1}, big.NewInt(100))
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
	if got, want := balance, big.NewInt(100); got.Cmp(want) != 0 {
		log.Fatalf("balance does not match: %d != %d", got, want)
	}
	fmt.Printf("Balance of %v is %d\n", carmen.Address{1}, balance)

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

```
</details>


## Query API
Both LiveDB and Archive can be queried using a **query API** that avoids hassle packing every request to a block 
and transaction in previous APIs. It is done via a functional style callback that provides a query:

<details>
    <summary>Querying a LiveDB</summary>

```
func ExampleDatabase_QueryHeadState() {
	dir, err := os.MkdirTemp("", "carmen_db_*")
	if err != nil {
		log.Fatalf("cannot create temporary directory: %v", err)
	}
	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithArchiveConfiguration(), nil)
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
```
</details>

<details>
    <summary>Querying an Archive</summary>

```
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
			context.AddBalance(carmen.Address{1, 2, 3}, big.NewInt(100))
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
		if got, want := balance, big.NewInt(100); got.Cmp(want) != 0 {
			log.Fatalf("balance does not match: %d != %d", got, want)
		}
		fmt.Printf("Balance of %v is %d\n", carmen.Address{1, 2, 3}, balance)
	}); err != nil {
		log.Fatalf("query operation failed: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Fatalf("cannot close db: %v", err)
	}

	// Output: Balance of [1 2 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] is 100
}
```
</details>

# Performance 

Carmen is the backbone of [Fantom Sonic](https://fantom.foundation/sonicPage) blockchain. 
It is the key component that enables high performance with a disk space conservative manner. 

Carmen was extensively tested as part of the Sonic client (i.e. a blockchain node). 
Three main scenarios were tested:
1. *Realistic traffic:* a mix of transactions that resemble realistic traffic, such as token transfers, token mints, and complex multi-step swaps.
2. *Token swaps:* transactions processing only ERC-20 swaps, demonstrating what the next generation of DEXs can achieve.
3. *Token transfers:* transactions processing only to process only ERC-20 transfers, demonstrating usage for next-generation wallets, payment providers.

The client could process thousands of transactions per second as depicted in the table 
and further detailed in [a blog post](https://blog.fantom.foundation/3-incredible-performances-from-fantom-sonic-closed-testnet/)

| Configuration | Speed Tx/s |
-------------------------------
| Realistic       | 2.000 |
| Token swaps     | 4.000 |
| Token transfers | 10.000 |

Processing this traffic, Carmen needed consumed disk space: 
* *~60GB* to store 100M transactions 
* additional *~160GB* to store Archive 
  

# License 

The license text is available in [LICENSE](LICENSE)

***
