package main

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/io"

	"github.com/Fantom-foundation/Carmen/go/state/mpt"
	"github.com/urfave/cli/v2"
)

var Info = cli.Command{
	Action: info,
	Name:   "info",
	Usage:  "lists information about a Carmen MTP state repository",
	Flags: []cli.Flag{
		&statsFlag,
	},
	ArgsUsage: "<director>",
}

var (
	statsFlag = cli.BoolFlag{
		Name:  "stats",
		Usage: "Compute and print node statistics",
	}
)

func info(context *cli.Context) error {
	// parse the directory argument
	if context.Args().Len() != 1 {
		return fmt.Errorf("missing directory storing state")
	}
	dir := context.Args().Get(0)

	withStats := context.Bool(statsFlag.Name)

	// try to obtain information of the contained MPT
	mptInfo, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	fmt.Printf("Directory contains an MPT State with the following properties:\n")
	fmt.Printf("\tMPT Configuration: %v\n", mptInfo.Config.Name)
	fmt.Printf("\tMode:              %v\n", mptInfo.Mode)

	// attempt to open the MPT
	if mptInfo.Mode == mpt.Mutable {
		trie, err := mpt.OpenFileLiveTrie(dir, mptInfo.Config, mpt.DefaultMptStateCapacity)
		if err != nil {
			fmt.Printf("\tFailed to open:    %v\n", err)
			return nil
		} else {
			fmt.Printf("\tCan be opened:     Yes\n")
		}

		if withStats {
			fmt.Printf("\nCollecting Node Statistics ...\n")
			stats, err := mpt.GetTrieNodeStatistics(trie)
			if err != nil {
				return err
			}
			fmt.Print("\n--- Node Statistics ---\n")
			fmt.Println(stats.String())
		}

		if err := trie.Close(); err != nil {
			return fmt.Errorf("error closing forest: %v", err)
		}
	} else {
		archive, err := mpt.OpenArchiveTrie(dir, mptInfo.Config, mpt.DefaultMptStateCapacity)
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

		if withStats {
			fmt.Printf("\nCollecting Node Statistics ...\n")
			stats, err := mpt.GetForestNodeStatistics(dir, mptInfo.Config)
			if err != nil {
				return err
			}
			fmt.Print("\n--- Node Statistics ---\n")
			fmt.Println(stats.String())
		}

	}

	return nil
}
