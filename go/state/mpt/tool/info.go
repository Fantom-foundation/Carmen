package main

import (
	"fmt"
	"os"

	"github.com/Fantom-foundation/Carmen/go/state/mpt"
	"github.com/urfave/cli/v2"
)

var Info = cli.Command{
	Action:    info,
	Name:      "info",
	Usage:     "lists information about a Carmen MTP state repository",
	ArgsUsage: "<director>",
}

func info(context *cli.Context) error {
	// parse the directory argument
	if context.Args().Len() != 1 {
		return fmt.Errorf("missing directory storing state")
	}
	dir := context.Args().Get(0)

	// check that the provided repository is a directory
	if stat, err := os.Stat(dir); err != nil {
		return fmt.Errorf("no such directory: %v", dir)
	} else if !stat.IsDir() {
		return fmt.Errorf("%v is not a directory", dir)
	}

	// try to obtain information of the contained MPT
	mptInfo, err := getMptInfo(dir)
	if err != nil {
		return err
	}

	fmt.Printf("Directory contains an MPT State with the following properties:\n")
	fmt.Printf("\tMPT Configuration: %v\n", mptInfo.config.Name)
	fmt.Printf("\tMode:              %v\n", mptInfo.mode)

	// attempt to open the MPT
	if mptInfo.mode == mpt.Live {
		trie, err := mpt.OpenFileLiveTrie(dir, mptInfo.config)
		if err != nil {
			fmt.Printf("\tFailed to open:    %v\n", err)
			return nil
		} else {
			fmt.Printf("\tCan be opened:     Yes\n")
		}

		if err := trie.Close(); err != nil {
			return fmt.Errorf("error closing forest: %v", err)
		}
	} else {
		archive, err := mpt.OpenArchiveTrie(dir, mptInfo.config)
		if err != nil {
			fmt.Printf("\tFailed to open:    %v\n", err)
			return nil
		} else {
			fmt.Printf("\tCan be opened:     Yes\n")
		}

		height, empty, err := archive.GetBlockHeight()
		if err != nil {
			fmt.Printf("\tBlock height:      %v\n", err)
		} else if empty {
			fmt.Printf("\tBlock height:      empty\n")
		} else {
			fmt.Printf("\tBlock height:      %d\n", height)
		}

		if err := archive.Close(); err != nil {
			return fmt.Errorf("error closing forest: %v", err)
		}
	}

	return nil
}

type mptInfo struct {
	config mpt.MptConfig
	mode   mpt.StorageMode
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
