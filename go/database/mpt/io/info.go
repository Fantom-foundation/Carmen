package io

import (
	"fmt"
	"os"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

type MptInfo struct {
	Config mpt.MptConfig
	Mode   mpt.StorageMode
}

func CheckMptDirectoryAndGetInfo(dir string) (MptInfo, error) {
	// check that the provided repository is a directory
	if stat, err := os.Stat(dir); err != nil {
		return MptInfo{}, fmt.Errorf("no such directory: %v", dir)
	} else if !stat.IsDir() {
		return MptInfo{}, fmt.Errorf("%v is not a directory", dir)
	}

	// try to obtain information of the contained MPT
	return getMptInfo(dir)
}

func getMptInfo(dir string) (MptInfo, error) {
	var res MptInfo
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

	mode := mpt.Immutable
	if meta.Mutable {
		mode = mpt.Mutable
	}

	return MptInfo{
		Config: config,
		Mode:   mode,
	}, nil
}
