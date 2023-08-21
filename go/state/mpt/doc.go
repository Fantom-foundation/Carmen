package mpt

/*

package mpt ...

Todos:
 - document this package

 - implement Live MPT structure
    - ~~set values~~
    - ~~get values~~
    - ~~hashing~~
    - ~~consistency check~~
    - ~~dumping for debugging~~
	- implement full-state verification
	- implement single-value verification
	- implement partial states

 - implement Archive MPT structure
    - ~~apply updates~~
    - ~~get values~~
    - ~~hashing~~
	- ~~implement unit tests for frozen nodes~~
	- make thread safe
    - consistency check
    - ~~dumping for debugging~~
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
	- ~~try passing arguments in node manipulations by value instead of by-pointer~~
	- release state trie asynchroniously in the background
	- implement a custom node cache with faster lookups
	- compute hashes in parallel

*/

/*

Current issue:

unexpected hash for block 652606
wanted 0xcd0e77d84ab4a53c081e296b88483dd45675bb414b9ad2e9a30e9c7174df76d3
   got 0xf0d95fcba54dac3c889fa104b8914d9a49db17e5feeb3da8c0eaa935df099113


Have:
1cd3 - 267222e216a9febf1db123715d9a48a0424b05b3c66dc384953e9ec554eb6228
1cd32 - 3937f00b4c39d1f3120ad9d9557bfc62d2ed2d3e2e08108c080cd6c7d5de4b02
1cd32d2b - e512951de8d7532804d7add78a5625e6570b46c5ad9f39d42342e5ad665818d9
1cd32d2b7 - 9df823fbe8929925be376981e614e1e191fd6be3770a5d366dd2d16af3bdeaf2
1cd32d2bc - e7f1b1dc5bd6a8aa153134ddae4d2bf64a80ad1205355f385c5879a622a73612


Should:
1cd3 - cf60a7ee1cde1f30066afad28cc4df3d3f6f143956152d05278112a64ce7e8a6
1cd32 - c21afd56e4d1f365c9c36ec6c1e823ee1de99e1e7df9c6968958147118728dac
1cd32d2b - 0f284164ed2106b827a49f8298c2fedc8b726c1fff3b574fba83fda47aa1fe8e
1cd32d2b7 - 0000000000000000000000000000000000000000000000000000000000000000
1cd32d2bc - e7f1b1dc5bd6a8aa153134ddae4d2bf64a80ad1205355f385c5879a622a73612


*/
