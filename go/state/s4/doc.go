package s4

/*

Package s4 ...

Todos:
 - document this package

 - implement Live MPT structure
    - ~~set values~~
    - ~~get values~~
    - hashing
    - ~consistency check~~
    - ~dumping for debugging~~
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
	- ~~implement common node cache in StateTrie~~
    - implement worker pool and parallelize hashing operations
	- implement bulk insertion operations
	- make structures thread-safe
	- use writers and readers in encoders, instead of buffers
	- shard data among multiple files for concurrent file reads
	- ~~lazy-initialize files in Stock data structure~~
	- ~~use write buffer in file-based Stock~~
	- try passing arguments in node manipulations by value instead of by-pointer
	- release state trie asynchroniously in the background

*/
