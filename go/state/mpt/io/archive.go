package io

import "github.com/Fantom-foundation/Carmen/go/common"

// This file provides a pair of import and export functions capable of
// serializing the content of an Archive into a single, payload-only data
// blob with build-in consistency check which can be utilized for safely
// transferring state information between systems.
//
// Format:
//
//  file   ::= <magic-number> <version> [<code>]* <state> [<update>]*
//  code   ::= 'C' <2-byte big-endian code length> <code>
//  state  ::= [<hash>]+ [<entry>]*
//  hash   ::= 'H' <1-byte hash type identifier> <state-hash>
//  entry  ::= 'A' <address> <balance> <nonce> <code-hash>
//           | 'S' <key> <value>
//  update ::= 'U' <4-byte big-endian block> [<hash>]+ [<change>]+
//  change ::= 'A' <address>           // starts a new account scope
//           | 'R'                     // reset the current account
//           | 'B' <balance>           // update the current account's balance
//           | 'N' <nonce>             // update the current account's nonce
//           | 'C' <code-hash>         // update the current account's code
//           | 'V' <key> <value>       // update the value of a storage slot
//           | 'D' <key>               // delete a storage slot
//
// All properties belong to the account preceding it. The produced data stream
// may be further compressed (e.g. using Gzip) to reduce its size.

var archiveMagicNumber []byte = []byte("Fantom-Archive-State")

const archiveFormatVersion = byte(1)

type Diff map[common.Address]AccountDiff

type AccountDiff struct {
	Reset   bool
	Balance *common.Balance
	Nonce   *common.Nonce
	Code    *common.Hash
	Storage map[common.Key]common.Value
}
