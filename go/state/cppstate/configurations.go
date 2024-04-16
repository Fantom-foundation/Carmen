//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package cppstate

import "github.com/Fantom-foundation/Carmen/go/state"

const (
	VariantCppMemory  state.Variant = "cpp-memory"
	VariantCppFile    state.Variant = "cpp-file"
	VariantCppLevelDb state.Variant = "cpp-ldb"
)

func init() {
	supportedArchives := []state.ArchiveType{
		state.NoArchive,
		state.LevelDbArchive,
		state.SqliteArchive,
	}

	// Register all configuration options supported by the C++ implementation.
	for schema := state.Schema(1); schema <= state.Schema(3); schema++ {
		for _, archive := range supportedArchives {
			state.RegisterStateFactory(state.Configuration{
				Variant: VariantCppMemory,
				Schema:  schema,
				Archive: archive,
			}, newInMemoryState)
			state.RegisterStateFactory(state.Configuration{
				Variant: VariantCppFile,
				Schema:  schema,
				Archive: archive,
			}, newFileBasedState)
			state.RegisterStateFactory(state.Configuration{
				Variant: VariantCppLevelDb,
				Schema:  schema,
				Archive: archive,
			}, newLevelDbBasedState)
		}
	}
}
