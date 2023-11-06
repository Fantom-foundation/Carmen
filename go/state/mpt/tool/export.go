package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/io"
	"os"

	"github.com/urfave/cli/v2"
)

var ExportCmd = cli.Command{
	Action:    doExport,
	Name:      "export",
	Usage:     "exports a LiveDB instance into a file",
	ArgsUsage: "<live-db director> <target-file>",
}

func doExport(context *cli.Context) error {
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing state directory and/or target file parameter")
	}
	dir := context.Args().Get(0)
	trg := context.Args().Get(1)

	file, err := os.Create(trg)
	if err != nil {
		return err
	}
	bufferedWriter := bufio.NewWriter(file)
	out := gzip.NewWriter(bufferedWriter)
	return errors.Join(
		io.Export(dir, out),
		out.Close(),
		bufferedWriter.Flush(),
		file.Close(),
	)
}
