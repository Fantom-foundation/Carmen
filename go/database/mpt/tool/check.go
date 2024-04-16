//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package main

import (
	"fmt"
	"strings"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var Check = cli.Command{
	Action:    check,
	Name:      "check",
	Usage:     "performs extensive invariants checks",
	ArgsUsage: "<director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

func check(context *cli.Context) error {
	// parse the directory argument
	if context.Args().Len() != 1 {
		return fmt.Errorf("missing directory storing state")
	}

	// Start profiling ...
	cpuProfileFileName := context.String(cpuProfileFlag.Name)
	if strings.TrimSpace(cpuProfileFileName) != "" {
		if err := startCpuProfiler(cpuProfileFileName); err != nil {
			return err
		}
		defer stopCpuProfiler()
	}

	dir := context.Args().Get(0)

	// try to obtain information of the contained MPT
	info, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	if info.Mode == mpt.Immutable {
		fmt.Printf("Checking archive in %s ...\n", dir)
		err = checkArchive(dir, info)
	} else {
		fmt.Printf("Checking live DB in %s ...\n", dir)
		err = checkLiveDB(dir, info)
	}
	if err == nil {
		fmt.Printf("All checks passed!\n")
	}
	return err
}

func checkLiveDB(dir string, info io.MptInfo) error {
	live, err := mpt.OpenFileLiveTrie(dir, info.Config, mpt.DefaultMptStateCapacity)
	if err != nil {
		return err
	}
	defer live.Close()
	return live.Check()
}

func checkArchive(dir string, info io.MptInfo) error {
	archive, err := mpt.OpenArchiveTrie(dir, info.Config, mpt.DefaultMptStateCapacity)
	if err != nil {
		return err
	}
	defer archive.Close()
	return archive.Check()
}
