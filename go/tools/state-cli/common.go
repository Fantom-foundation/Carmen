package main

import (
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/Fantom-foundation/Carmen/go/state"
)

// open opens a State in a directoy using the 'go-file' schema 3 format.
func open(dir string) (state.State, error) {
	return state.NewGoFileState(state.Parameters{
		Directory: dir,
		Schema:    3,
		Archive:   state.NoArchive,
	})
}

func StartCPUProfile(profileName string) error {
	f, err := os.Create(profileName)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %s", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		return fmt.Errorf("could not start CPU profile: %s", err)
	}
	return nil
}

func StopCPUProfile() {
	pprof.StopCPUProfile()
}
