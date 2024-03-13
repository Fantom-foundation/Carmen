package carmen

import (
	"slices"
	"testing"
)

func TestConfiguration_OnlyContainsOfficialImplementations(t *testing.T) {
	configs := GetAllConfigurations()

	want := []Configuration{
		GetCarmenGoS5WithArchiveConfiguration(),
		GetCarmenGoS5WithoutArchiveConfiguration(),
	}

	if want, got := len(want), len(configs); want != got {
		t.Fatalf("unexpected number of official configurations, wanted %d, got %d", want, got)
	}

	for _, config := range want {
		if !slices.Contains(configs, config) {
			t.Errorf("missing registration of configuration %v", config)
		}
	}
}

func TestConfiguration_RegisteredConfigurationsCanBeUsed(t *testing.T) {
	for _, config := range GetAllConfigurations() {
		config := config
		t.Run(config.String(), func(t *testing.T) {
			t.Parallel()
			db, err := OpenDatabase(t.TempDir(), config, nil)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("failed to close database: %v", err)
			}
		})
	}
}

func TestConfiguration_RegisteringUnsupportedConfigurationFails(t *testing.T) {
	config := Configuration{
		Variant: Variant("something-that-is-not-supported"),
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected a panic, got nothing")
		}
	}()
	RegisterConfiguration(config)
}
