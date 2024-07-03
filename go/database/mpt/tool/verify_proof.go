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
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
	"log"
	"math"
	"strings"
)

var VerifyProof = cli.Command{
	Action:    verifyProof,
	Name:      "verify-proof",
	Usage:     "verifies the consistency of witness proofs",
	ArgsUsage: "<director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
		&blockTo,
		&blockFrom,
		&blockNum,
	},
}

var (
	blockFrom = cli.IntFlag{
		Name:  "block-from",
		Usage: "the starting block number to verify",
		Value: 0,
	}
	blockTo = cli.IntFlag{
		Name:  "block-to",
		Usage: "the ending block number to verify",
		Value: 0,
	}
	blockNum = cli.IntFlag{
		Name:  "block-num",
		Usage: "block number to verify",
		Value: 0,
	}
)

func verifyProof(context *cli.Context) error {
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
	info, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	ctx := interrupt.CancelOnInterrupt(context.Context)
	if info.Config.Name == mpt.S5ArchiveConfig.Name {
		from := context.Int(blockFrom.Name)
		to := context.Int(blockTo.Name)
		toStr := fmt.Sprintf("%d", context.Int(blockTo.Name))
		if !context.IsSet(blockTo.Name) {
			to = math.MaxInt
			toStr = "max"
		}
		if context.IsSet(blockNum.Name) {
			from = context.Int(blockNum.Name)
			to = from
			log.Printf("archive single block verification configured: %d", context.Int(blockNum.Name))
		} else {
			log.Printf("archive block range verification configured: [%d;%v]", context.Int(blockFrom.Name), toStr)

		}
		return mpt.VerifyProofArchiveTrie(ctx, dir, info.Config, from, to, &verificationObserver{})
	}

	if info.Config.Name == mpt.S5LiveConfig.Name {
		if context.IsSet(blockNum.Name) || context.IsSet(blockFrom.Name) || context.IsSet(blockTo.Name) {
			log.Printf("WARNING: 'block-num', 'block-from' and 'block-to' flags are not supported for live trie verification, ignoring them.")
		}
		log.Printf("live trie verification configured")
		return mpt.VerifyProofLiveTrie(ctx, dir, info.Config, &verificationObserver{})
	}

	return fmt.Errorf("can only support witness proof of S5 instances, found %v in directory", info.Config.Name)

}
