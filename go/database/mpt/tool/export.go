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
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var ExportCmd = cli.Command{
	Action:    addPerformanceDiagnoses(doExport),
	Name:      "export",
	Usage:     "exports a LiveDB or Archive instance into a file",
	ArgsUsage: "<db director> <target-file>",
	Flags: []cli.Flag{
		&targetBlockFlag,
	},
}

func doExport(context *cli.Context) error {
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing state directory and/or target file parameter")
	}
	dir := context.Args().Get(0)
	trg := context.Args().Get(1)

	// check the type of target database
	mptInfo, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	start := time.Now()
	logFromStart(start, "export started")

	file, err := os.Create(trg)
	if err != nil {
		return err
	}
	bufferedWriter := bufio.NewWriter(file)
	out := gzip.NewWriter(bufferedWriter)

	ctx := interrupt.CancelOnInterrupt(context.Context)

	var exportErr error

	if mptInfo.Mode == mpt.Immutable {
		if context.IsSet(targetBlockFlag.Name) {
			// Passed Archive and chosen block to export
			blkNumber := context.Uint64(targetBlockFlag.Name)
			exportErr = io.ExportBlockFromArchive(ctx, dir, out, blkNumber)
		} else {
			// Passed Archive without chosen block
			exportErr = io.ExportArchive(ctx, dir, out)
		}
	} else {
		// Passed LiveDB
		exportErr = io.Export(ctx, dir, out)
	}

	if err = errors.Join(
		exportErr,
		out.Close(),
		bufferedWriter.Flush(),
		file.Close(),
	); err != nil {
		return err
	}
	logFromStart(start, "export done")
	return nil
}

func logFromStart(start time.Time, msg string) {
	now := time.Now()
	t := uint64(now.Sub(start).Seconds())
	log.Printf("[t=%4d:%02d] - %s.\n", t/60, t%60, msg)
}
