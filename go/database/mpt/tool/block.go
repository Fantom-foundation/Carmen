//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
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

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var Block = cli.Command{
	Action:    block,
	Name:      "block",
	Usage:     "retrieves information about a given block",
	ArgsUsage: "<archive-director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
		&targetBlockFlag,
	},
}

var targetBlockFlag = cli.Uint64Flag{
	Name:  "block",
	Usage: "the block for which information should be obtained",
}

func block(context *cli.Context) error {
	// parse the directory argument
	if context.Args().Len() != 1 {
		return fmt.Errorf("missing directory storing archive")
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
	block := context.Uint64(targetBlockFlag.Name)

	// try to obtain information of the selected block
	info, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}
	archive, err := mpt.OpenArchiveTrie(dir, info.Config, 1024)
	if err != nil {
		return fmt.Errorf("failed to open archive in %s: %w", dir, err)
	}

	fmt.Printf("Block: %d\n", block)
	hash, err := archive.GetHash(block)
	if err != nil {
		return fmt.Errorf("failed to get hash for block %d: %w", block, err)
	}
	fmt.Printf("Hash: %x\n", hash)

	diff, err := archive.GetDiffForBlock(block)
	if err != nil {
		return fmt.Errorf("failed to get diff for block %d: %w", block, err)
	}
	fmt.Printf("%s\n", diff)
	update, err := diffToUpdate(diff)
	if err != nil {
		return fmt.Errorf("failed to convert diff to update: %w", err)
	}
	fmt.Printf("%s\n", &update)

	if err := archive.Close(); err != nil {
		return fmt.Errorf("failed to close archive: %w", err)
	}
	return nil
}

func diffToUpdate(diff mpt.Diff) (common.Update, error) {
	res := common.Update{}
	for account, diff := range diff {
		if diff.Reset {
			res.AppendDeleteAccount(account)
		}
		if diff.Balance != nil {
			res.AppendBalanceUpdate(account, *diff.Balance)
		}
		if diff.Nonce != nil {
			res.AppendNonceUpdate(account, *diff.Nonce)
		}
		if diff.Code != nil {
			res.AppendCodeUpdate(account, (*diff.Code)[:])
		}
		for key, value := range diff.Storage {
			res.AppendSlotUpdate(account, key, value)
		}
	}
	err := res.Normalize()
	return res, err
}
