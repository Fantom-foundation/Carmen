package cppstate

import "github.com/Fantom-foundation/Carmen/go/database"

const (
	VariantCppMemory  database.Variant = "cpp-memory"
	VariantCppFile    database.Variant = "cpp-file"
	VariantCppLevelDb database.Variant = "cpp-ldb"
)

func init() {
	supportedArchives := []database.ArchiveType{
		database.NoArchive,
		database.LevelDbArchive,
		database.SqliteArchive,
	}

	// Register all configuration options supported by the C++ implementation.
	for schema := database.Schema(1); schema <= database.Schema(3); schema++ {
		for _, archive := range supportedArchives {
			database.RegisterDatabaseFactory(database.Configuration{
				Variant: VariantCppMemory,
				Schema:  schema,
				Archive: archive,
			}, newInMemoryState)
			database.RegisterDatabaseFactory(database.Configuration{
				Variant: VariantCppFile,
				Schema:  schema,
				Archive: archive,
			}, newFileBasedState)
			database.RegisterDatabaseFactory(database.Configuration{
				Variant: VariantCppLevelDb,
				Schema:  schema,
				Archive: archive,
			}, newLevelDbBasedState)
		}
	}
}
