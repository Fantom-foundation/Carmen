package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io"
	"github.com/urfave/cli/v2"
)

var Verify = cli.Command{
	Action:    verify,
	Name:      "verify",
	Usage:     "verifies the consistency of an MPT",
	ArgsUsage: "<director>",
	Flags: []cli.Flag{
		&cpuProfileFlag,
	},
}

func verify(context *cli.Context) error {
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

	// try to obtain information of the contained MPT
	info, err := io.CheckMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	// run forest verification
	observer := &verificationObserver{}

	if info.Mode == mpt.Immutable {
		return mpt.VerifyArchive(dir, info.Config, observer)
	}
	return mpt.VerifyFileLiveTrie(dir, info.Config, observer)
}

type verificationObserver struct {
	start time.Time
}

func (o *verificationObserver) StartVerification() {
	o.start = time.Now()
	o.printHeader()
	fmt.Println("Starting verification ...")
}

func (o *verificationObserver) Progress(msg string) {
	o.printHeader()
	fmt.Println(msg)
}

func (o *verificationObserver) EndVerification(res error) {
	if res == nil {
		o.printHeader()
		fmt.Println("Verification successful!")
	}
}

func (o *verificationObserver) printHeader() {
	now := time.Now()
	t := uint64(now.Sub(o.start).Seconds())
	fmt.Printf("%s [t=%4d:%02d] - ", now.Format("15:04:05"), t/60, t%60)
}
