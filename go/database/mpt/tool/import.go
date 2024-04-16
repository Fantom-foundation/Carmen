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
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	mptIo "github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var ImportLiveDbCmd = cli.Command{
	Action:    doLiveDbImport,
	Name:      "import-live-db",
	Usage:     "imports a LiveDB instance from a file",
	ArgsUsage: "<source-file> <target director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

var ImportArchiveCmd = cli.Command{
	Action:    doArchiveImport,
	Name:      "import-archive",
	Usage:     "imports an Archive instance from a file",
	ArgsUsage: "<source-file> <target director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

var ImportLiveAndArchiveCmd = cli.Command{
	Action:    doLiveAndArchiveImport,
	Name:      "import",
	Usage:     "imports both LiveDB and Archive instance from a file",
	ArgsUsage: "<source-file> <target director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

func doLiveDbImport(context *cli.Context) error {
	return doImport(context, mptIo.ImportLiveDb)
}

func doArchiveImport(context *cli.Context) error {
	return doImport(context, mptIo.ImportArchive)
}

func doLiveAndArchiveImport(context *cli.Context) error {
	return doImport(context, mptIo.ImportLiveAndArchive)
}

func doImport(context *cli.Context, runImport func(directory string, in io.Reader) error) error {
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing source file and/or target directory parameter")
	}
	src := context.Args().Get(0)
	dir := context.Args().Get(1)

	// Start profiling ...
	cpuProfileFileName := context.String(cpuProfileFlag.Name)
	if strings.TrimSpace(cpuProfileFileName) != "" {
		if err := startCpuProfiler(cpuProfileFileName); err != nil {
			return err
		}
		defer stopCpuProfiler()
	}

	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("error creating output directory: %v", err)
	}

	start := time.Now()
	logFromStart(start, "import started")
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	var in io.Reader = bufio.NewReader(file)
	if in, err = gzip.NewReader(in); err != nil {
		return err
	}
	defer func() {
		logFromStart(start, "import done")
	}()
	return errors.Join(
		runImport(dir, in),
		file.Close(),
	)
}
