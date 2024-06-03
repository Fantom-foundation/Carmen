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
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var ExportCmd = cli.Command{
	Action:    doExport,
	Name:      "export",
	Usage:     "exports a LiveDB or Archive instance into a file",
	ArgsUsage: "<db director> <target-file>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

func doExport(cliCtx *cli.Context) error {
	if cliCtx.Args().Len() != 2 {
		return fmt.Errorf("missing state directory and/or target file parameter")
	}
	dir := cliCtx.Args().Get(0)
	trg := cliCtx.Args().Get(1)

	// Start profiling ...
	cpuProfileFileName := cliCtx.String(cpuProfileFlag.Name)
	if strings.TrimSpace(cpuProfileFileName) != "" {
		if err := startCpuProfiler(cpuProfileFileName); err != nil {
			return err
		}
		defer stopCpuProfiler()
	}

	// check the type of target database
	mptInfo, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	export := io.Export
	if mptInfo.Mode == mpt.Immutable {
		export = io.ExportArchive
	}

	start := time.Now()
	logFromStart(start, "export started")

	ctx, cancel := context.WithCancel(cliCtx.Context)
	catchInterrupt(ctx, cancel, start)

	file, err := os.Create(trg)
	if err != nil {
		return err
	}
	bufferedWriter := bufio.NewWriter(file)
	out := gzip.NewWriter(bufferedWriter)
	defer func() {
		if io.IsContextDone(ctx) {
			logFromStart(start, "export canceled")
			return
		}
		logFromStart(start, "export done")
		cancel()
	}()
	return errors.Join(
		export(ctx, dir, out),
		out.Close(),
		bufferedWriter.Flush(),
		file.Close(),
	)
}

func catchInterrupt(ctx context.Context, cancel context.CancelFunc, start time.Time) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		defer signal.Stop(c)
		select {
		case <-c:
			logFromStart(start, "Closing, please wait until proper shutdown to prevent database corruption")
			cancel()
		case <-ctx.Done():
		}
	}()
}

func logFromStart(start time.Time, msg string) {
	now := time.Now()
	t := uint64(now.Sub(start).Seconds())
	log.Printf("[t=%4d:%02d] - %s.\n", t/60, t%60, msg)
}
