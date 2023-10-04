package main

import (
	"fmt"
	"time"

	"github.com/Fantom-foundation/Carmen/go/state/mpt"
	"github.com/urfave/cli/v2"
)

var Verify = cli.Command{
	Action:    verify,
	Name:      "verify",
	Usage:     "verifies the consistency of an MPT",
	ArgsUsage: "<director>",
}

func verify(context *cli.Context) error {
	// parse the directory argument
	if context.Args().Len() != 1 {
		return fmt.Errorf("missing directory storing state")
	}
	dir := context.Args().Get(0)

	// try to obtain information of the contained MPT
	info, err := checkMptDirectoryAndGetInfo(dir)
	if err != nil {
		return err
	}

	// run forest verification
	observer := &verificationObserver{}

	if info.mode == mpt.Archive {
		return mpt.VerifyArchive(dir, info.config, observer)
	}
	return mpt.VerifyFileLiveTrie(dir, info.config, observer)
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
