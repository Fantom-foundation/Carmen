package main

import (
	"fmt"
	"os"

	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

type mptInfo struct {
	config mpt.MptConfig
	mode   mpt.StorageMode
}

func checkMptDirectoryAndGetInfo(dir string) (mptInfo, error) {
	// check that the provided repository is a directory
	if stat, err := os.Stat(dir); err != nil {
		return mptInfo{}, fmt.Errorf("no such directory: %v", dir)
	} else if !stat.IsDir() {
		return mptInfo{}, fmt.Errorf("%v is not a directory", dir)
	}

	// try to obtain information of the contained MPT
	return getMptInfo(dir)
}

func getMptInfo(dir string) (mptInfo, error) {
	var res mptInfo
	meta, present, err := mpt.ReadForestMetadata(dir + "/forest.json")
	if err != nil {
		return res, err
	}

	if !present {
		return res, fmt.Errorf("invalid directory content: missing forest.json")
	}

	// Try to resolve the configuration.
	config, found := mpt.GetConfigByName(meta.Configuration)
	if !found {
		return res, fmt.Errorf("unknown MPT configuration: %v", meta.Configuration)
	}

	mode := mpt.Live
	if meta.Archive {
		mode = mpt.Archive
	}

	return mptInfo{
		config: config,
		mode:   mode,
	}, nil
}
