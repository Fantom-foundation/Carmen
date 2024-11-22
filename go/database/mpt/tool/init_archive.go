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
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"io"
	"os"

	mptIo "github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var InitArchive = cli.Command{
	Action:    addPerformanceDiagnoses(doArchiveInit),
	Name:      "init-archive",
	Usage:     "initializes an Archive instance from a file",
	ArgsUsage: "<source-file> <archive target director>",
	Flags: []cli.Flag{
		&blockHeightFlag,
	},
}

var (
	blockHeightFlag = cli.Uint64Flag{
		Name:  "block-height",
		Usage: "the block height the input file is describing",
	}
)

func doArchiveInit(context *cli.Context) error {
	if context.Args().Len() != 2 {
		return fmt.Errorf("missing source file and/or target directory parameter")
	}
	src := context.Args().Get(0)
	dir := context.Args().Get(1)

	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("error creating output directory: %v", err)
	}

	height := context.Uint64(blockHeightFlag.Name)

	logger := mptIo.NewLog()
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	var in io.Reader = bufio.NewReader(file)
	if in, err = gzip.NewReader(in); err != nil {
		return err
	}
	return errors.Join(
		mptIo.InitializeArchive(logger, dir, in, height, mpt.NodeCacheConfig{}),
		file.Close(),
	)
}
