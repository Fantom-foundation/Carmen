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

package carmen

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/state"
	"github.com/Fantom-foundation/Carmen/go/state/gostate"
	"golang.org/x/exp/maps"
)

// Configuration is a unique identifier of a Carmen DB setup option.
// Each setup is defined by three components:
//   - Variant: the technology used for implementing the DB. This may
//     reference different languages (Go, C++, ...) or storage solutions
//     (in-memory, files, levelDB, SQL, ...)
//   - Schema: defining the data format used for storing the data in the
//     respective implementation. The format may have an impact on performance
//     characteristics of the implementation as well as the availability and
//     format of state hashes and witness proofs
//   - Archive: defines what type of archive is utilized to store historic
//     data. In general, tracking historic data is optional in Carmen, and
//     can be disabled by this option. Alternative archive solutions may be
//     SQL, LevelDB, or file based
//
// Not every combination of variants, schemas, and archive values is
// supported. Furthermore, the Carmen infrastructure is open for future
// extensions, thus the set of supported implementations may change.
// This may happen over time, as new implementations are added to the project
// or by user code adding new options.
// All available Configurations can be obtained using the GetAllConfigurations()
// function defined in this file. See its description to get more information
// on how the set of supported Configurations can be extended.
type Configuration struct {
	Variant Variant
	Schema  Schema
	Archive Archive
}

func (c Configuration) String() string {
	return fmt.Sprintf("%v-S%v-%v", c.Variant, c.Schema, c.Archive)
}

// Variant describes the base technology underlying an implementation.
// Examples are go-file for a Go/File-based implementation or cpp-memory
// for a C++ based in-memory implementation. Values should not be defined
// by client code using Carmen. Instead, constants shall be provided by
// specific implementations.
type Variant string

// Schema are used to differentiate between data representation
// formats used by the implementations. Different implementations may support
// the same set of schemas, but not all variants are required to support all
// schemas. If two variants support the same schema, their data representation
// is identical.
type Schema int

// Archive is a type for identifying different archive implementations.
// Different variants may support different types of archive implementations.
type Archive string

// GetCarmenGoS5WithoutArchiveConfiguration returns the configuration for the
// officially supported Carmen DB implementation using a file based DB
// implemented in Go using an Ethereum compatible Merkle Patricia Trie format
// without Archive features. This configuration is intended to be used in cases
// where only the head-state of a block chain is required. No history is
// recorded. This significantly reduces disk usage.
func GetCarmenGoS5WithoutArchiveConfiguration() Configuration {
	return Configuration{
		Variant: Variant(gostate.VariantGoFile),
		Schema:  Schema(state.Schema(5)),
		Archive: Archive(state.NoArchive),
	}
}

// GetCarmenGoS5WithArchiveConfiguration returns the configuration for the
// officially supported Carmen DB implementation using a file based DB
// implemented in Go using an Ethereum compatible Merkle Patricia Trie format
// with Archive features. This configuration is intended to be used in cases
// where access to historic data is required.
func GetCarmenGoS5WithArchiveConfiguration() Configuration {
	return Configuration{
		Variant: Variant(gostate.VariantGoFile),
		Schema:  Schema(state.Schema(5)),
		Archive: Archive(state.S5Archive),
	}
}

// GetAllConfigurations returns a slice of all Database configurations
// supported in the current build target.
// By default, only the configurations provided by this package are listed.
// However, by importing the carmen/experimental package or by explicitly
// registering custom implementations using RegisterConfiguration() below,
// additional options can be made available.
func GetAllConfigurations() []Configuration {
	return maps.Keys(registeredConfigurations)
}

var registeredConfigurations = map[Configuration]struct{}{}

// RegisterConfiguration enables custom configurations to be registered. This
// function facilitates extensibility of
// configurations supported by this package. This method does not have to be called, when using
// one of the supported Carmen DBs.
// Before calling this function, make sure that corresponding factory
// functions have been registered in the 'state' package. Otherwise, a panic
// will indicate that the newly registered configuration can not be supported.
func RegisterConfiguration(config Configuration) {
	cfg := state.Configuration{
		Variant: state.Variant(config.Variant),
		Schema:  state.Schema(config.Schema),
		Archive: state.ArchiveType(config.Archive),
	}
	if _, found := state.GetAllRegisteredStateFactories()[cfg]; !found {
		panic(fmt.Errorf("unable to register configuration %v: no factory found", config))
	}
	registeredConfigurations[config] = struct{}{}
}

func init() {
	// Registers the two officially supported Carmen DB configurations.
	RegisterConfiguration(GetCarmenGoS5WithArchiveConfiguration())
	RegisterConfiguration(GetCarmenGoS5WithoutArchiveConfiguration())
}
