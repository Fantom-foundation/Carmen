package main

import (
	"fmt"

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

	// try to obtain information of the contained MPT
	mptInfo, err := checkMptDirectoryAndGetInfo(dir)
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
