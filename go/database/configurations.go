package database

import (
	"fmt"
	"maps"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// ----------------------------------------------------------------------------
//                        for Carmen users
// ----------------------------------------------------------------------------

// Parameters struct defining configuration parameters for state instances.
type Parameters struct {
	Variant      Variant
	Schema       Schema
	Archive      ArchiveType
	Directory    string
	LiveCache    int64 // bytes, approximate, supported only by S5 now
	ArchiveCache int64 // bytes, approximate, supported only by S5 now
}

// UnsupportedConfiguration is the error returned if unsupported configuration
// parameters have been specified. The text may contain further details regarding the
// unsupported feature.
const UnsupportedConfiguration = common.ConstError("unsupported configuration")

// NewDatabase is the public interface for creating Carmen DB instances. If for the
// given parameters a database can be constructed, the resulting DB is returned. If
// construction fails, an error is reported. If the requested configuration is not
// supported, the error is an UnsupportedConfiguration error.
func NewDatabase(params Parameters) (Database, error) {
	config := Configuration{
		Variant: params.Variant,
		Schema:  Schema(params.Schema),
		Archive: params.Archive,
	}
	// Enforce default values.
	if config.Variant == "" {
		config.Variant = "go-file"
	}
	if config.Schema == 0 {
		config.Schema = 5
	}
	if config.Archive == "" {
		config.Archive = NoArchive
	}
	factory, found := databaseFactoryRegistry[config]
	if !found {
		return nil, fmt.Errorf("%w: no registered implementation for %v", UnsupportedConfiguration, config)
	}
	return factory(params)
}

// ----------------------------------------------------------------------------
//                      for Carmen implementations
// ----------------------------------------------------------------------------

type Configuration struct {
	Variant Variant
	Schema  Schema
	Archive ArchiveType
}

func (c *Configuration) String() string {
	return fmt.Sprintf("%s_s%d_%v", c.Variant, c.Schema, c.Archive)
}

type Variant string

type Schema uint8

type ArchiveType string

const (
	NoArchive      ArchiveType = "none"
	LevelDbArchive ArchiveType = "ldb"
	SqliteArchive  ArchiveType = "sql"
	S4Archive      ArchiveType = "s4"
	S5Archive      ArchiveType = "s5"
)

type DatabaseFactory func(params Parameters) (Database, error)

var databaseFactoryRegistry = map[Configuration]DatabaseFactory{}

func RegisterDatabaseFactory(config Configuration, factory DatabaseFactory) {
	if _, found := databaseFactoryRegistry[config]; found {
		panic(fmt.Sprintf("attempted to register multiple factories for %v", config))
	}
	databaseFactoryRegistry[config] = factory
}

func GetAllRegisteredDatabaseFactories() map[Configuration]DatabaseFactory {
	return maps.Clone(databaseFactoryRegistry)
}
