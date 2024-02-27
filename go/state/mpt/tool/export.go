package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Fantom-foundation/Carmen/go/state/mpt"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/io"

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

func doExport(context *cli.Context) error {
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing state directory and/or target file parameter")
	}
	dir := context.Args().Get(0)
	trg := context.Args().Get(1)

	// Start profiling ...
	cpuProfileFileName := context.String(cpuProfileFlag.Name)
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
	file, err := os.Create(trg)
	if err != nil {
		return err
	}
	bufferedWriter := bufio.NewWriter(file)
	out := gzip.NewWriter(bufferedWriter)
	defer func() {
		logFromStart(start, "export done")
	}()
	return errors.Join(
		export(dir, out),
		out.Close(),
		bufferedWriter.Flush(),
		file.Close(),
	)
}

func logFromStart(start time.Time, msg string) {
	now := time.Now()
	t := uint64(now.Sub(start).Seconds())
	log.Printf("[t=%4d:%02d] - %s.\n", t/60, t%60, msg)
}
