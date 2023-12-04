package state

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/io"
	sio "io"
	"os"
	"path/filepath"
)

var ErrVerificationNotSupported = errors.New("verification not supported for used parameters")
var ErrImportNotSupported = errors.New("import not supported for used parameters")

func VerifyLiveDb(params Parameters, observer mpt.VerificationObserver) error {
	if params.Variant == GoFile && params.Schema == 5 {
		info, err := io.CheckMptDirectoryAndGetInfo(params.Directory)
		if err != nil {
			return fmt.Errorf("failed to check live state dir; %v", err)
		}
		if err := mpt.VerifyFileLiveTrie(params.Directory, info.Config, observer); err != nil {
			return fmt.Errorf("live state verification failed; %v", err)
		}
	}
	return ErrVerificationNotSupported
}

func VerifyArchive(params Parameters, observer mpt.VerificationObserver) error {
	if params.Archive == NoArchive {
		return nil // archive not used
	}
	if params.Archive == S5Archive {
		archiveDir := params.Directory + string(filepath.Separator) + "archive"
		info, err := io.CheckMptDirectoryAndGetInfo(archiveDir)
		if err != nil {
			return fmt.Errorf("failed to check archive dir; %v", err)
		}
		if err := mpt.VerifyArchive(archiveDir, info.Config, observer); err != nil {
			return fmt.Errorf("archive verification failed; %v", err)
		}
	}
	return ErrVerificationNotSupported
}

func ImportLiveDb(params Parameters, reader sio.Reader) error {
	if params.Variant == GoFile && params.Schema == 5 {
		if err := os.MkdirAll(params.Directory, 0700); err != nil {
			return fmt.Errorf("failed to create data dir; %v", err)
		}
		if err := io.ImportLiveDb(params.Directory, reader); err != nil {
			return fmt.Errorf("failed to import live state; %v", err)
		}
	}
	return ErrImportNotSupported
}

func ImportArchive(params Parameters, reader sio.Reader, blockNum uint64) error {
	if params.Archive == NoArchive {
		return nil // archive not used - skip import
	}
	if params.Archive == S5Archive {
		archiveDir := params.Directory + string(filepath.Separator) + "archive"
		if err := os.MkdirAll(archiveDir, 0700); err != nil {
			return fmt.Errorf("failed to create archive dir; %v", err)
		}
		if err := io.InitializeArchive(archiveDir, reader, blockNum); err != nil {
			return fmt.Errorf("failed to initialize archive; %v", err)
		}
	}
	return ErrImportNotSupported
}
