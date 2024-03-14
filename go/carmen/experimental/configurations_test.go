package experimental_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/carmen"
	"github.com/Fantom-foundation/Carmen/go/carmen/experimental"
)

func TestConfigurations_ContainGoAndCppImplementations(t *testing.T) {
	goSeen := false
	cppSeen := false
	for _, config := range experimental.GetDatabaseConfigurations() {
		if strings.HasPrefix(string(config.Variant), "go") {
			goSeen = true
		}
		if strings.HasPrefix(string(config.Variant), "cpp") {
			cppSeen = true
		}
	}
	if !goSeen {
		t.Errorf("missing Go based implementations")
	}
	if !cppSeen {
		t.Errorf("missing C++ based implementations")
	}
}

func TestConfigurations_ConfigurationsAreRegisteredGlobally(t *testing.T) {
	registeredConfigs := carmen.GetAllConfigurations()
	for _, config := range experimental.GetDatabaseConfigurations() {
		if !slices.Contains(registeredConfigs, config) {
			t.Errorf("missing registration of configuration %v", config)
		}
	}
}

func TestConfiguration_RegisteredConfigurationsCanBeUsed(t *testing.T) {
	for _, config := range carmen.GetAllConfigurations() {
		config := config
		t.Run(config.String(), func(t *testing.T) {
			t.Parallel()
			db, err := carmen.OpenDatabase(t.TempDir(), config, nil)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("failed to close database: %v", err)
			}
		})
	}
}
