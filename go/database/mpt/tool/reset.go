// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var Reset = cli.Command{
	Action:    reset,
	Name:      "reset",
	Usage:     "resets the given archive to a selected block",
	ArgsUsage: "<director> <block>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

func reset(context *cli.Context) error {
	// parse the directory argument
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing directory and/or block height parameter")
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
	blockArg := context.Args().Get(1)
	block, err := strconv.Atoi(blockArg)
	if err != nil {
		return fmt.Errorf("invalid block height %s", blockArg)
	}

	// try to obtain information of the contained MPT
	info, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	if info.Mode != mpt.Immutable {
		return fmt.Errorf("reset is only supported for archives")
	}

	fmt.Printf("Resetting archive in %s to block %d ...\n", dir, block)
	err = mpt.RestoreBlockHeight(dir, info.Config, uint64(block))
	if err == nil {
		fmt.Printf("Archive successfully reset to block %d\n", block)
	}
	return err
}
