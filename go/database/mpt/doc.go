// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

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
	- release state trie asynchronously in the background
	- implement a custom node cache with faster lookups
	- compute hashes in parallel
	- perform lookups iterative instead of recursive
	- lock only pairs of nodes instead of full path for reads
	- split locks for Node content updates and hash updates to support hashing while reading
	- limit re-hashing need by copying known hashes from cloned nodes in archive

*/
