package s4

/*

Package s4 ...

Todos:
 - document this package

 - implement Live MPT structure
    - set values
    - get values
    - hashing
    - consistency check
    - dumping for debugging
	- implement full-state verification
	- implement single-value verification
	- implement partial states

 - implement Archive MPT structure
    - set values
    - get values
    - hashing
    - consistency check
    - dumping for debugging
	- implement full-history verification
	- implement single-value verification

 - improvements
	- have Stocks return fresh pointers on read
	- ~~replace paths in leaf nodes (account and value) with address and key~~
	- support the collection of StateTrie statistics (number of nodes of types, nesting depth, account sizes, ...)
	- implement common node cache in StateTrie
    - implement worker pool and parallelize hashing operations
	- implement bulk insertion operations
	- make structures thread-safe
	- use writers and readers in encoders, instead of buffers
	- shard data among multiple files for concurrent file reads
	- lazy-initialize files in Stock data structure
	- release state trie asynchroniously in the background

*/

/*
Issues: - blk ~15.9M

2023-07-10 21:45:31.601 |  3:48:13 | 1 | validator | lfvm | carmen | go-file | s4 | 2023/07/10 21:45:31 INFO     runvm/RunVM[0m: Elapsed time: 3h 47m 56s, at block 15921431 (~ 5745 Tx/s, ~ 769288845 Gas/s)
2023-07-10 21:45:39.705 |  3:48:22 | 1 | validator | lfvm | carmen | go-file | s4 | 2023/07/10 21:45:39 Diff for Exist([[0x9D601Eaf282e0078f2Ebe9b4FFA68d484F8Fc3AC]] )
2023-07-10 21:45:39.705 |  3:48:22 | 1 | validator | lfvm | carmen | go-file | s4 | 	Primary: true 
2023-07-10 21:45:39.705 |  3:48:22 | 1 | validator | lfvm | carmen | go-file | s4 | 	Shadow: false
2023-07-10 21:45:39.768 |  3:48:22 | 1 | validator | lfvm | carmen | go-file | s4 | record-replay: CloseSubstateDB
2023-07-10 21:45:40.246 |  3:48:22 | 1 | validator | lfvm | carmen | go-file | s4 | state-db error after block 15923693, transaction 1: Exist([0x9D601Eaf282e0078f2Ebe9b4FFA68d484F8Fc3AC] ) diverged from shadow DB.
*/