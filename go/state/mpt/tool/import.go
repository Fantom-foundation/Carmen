package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"
)

var ImportCmd = cli.Command{
	Action:    doImport,
	Name:      "import",
	Usage:     "imports a LiveDB instance from a file",
	ArgsUsage: "<source-file> <live-db target director>",
}

func doImport(context *cli.Context) error {
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing source file and/or target directory parameter")
	}
	src := context.Args().Get(0)
	dir := context.Args().Get(1)

	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("error creating output directory: %v", err)
	}

	file, err := os.Open(src)
	if err != nil {
		return err
	}
	var in io.Reader = bufio.NewReader(file)
	if in, err = gzip.NewReader(in); err != nil {
		return err
	}
	return errors.Join(
		Import(dir, in),
		file.Close(),
	)
}
