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
var ErrExportNotSupported = errors.New("export not supported for used parameters")

func VerifyLiveDb(params Parameters, observer mpt.VerificationObserver) error {
	if params.Variant == "go-file" && params.Schema == 5 {
		liveDir := params.Directory + string(filepath.Separator) + "live"
		info, err := io.CheckMptDirectoryAndGetInfo(liveDir)
		if err != nil {
			return fmt.Errorf("failed to check live state dir; %v", err)
		}
		if err := mpt.VerifyFileLiveTrie(liveDir, info.Config, observer); err != nil {
			return fmt.Errorf("live state verification failed; %v", err)
		}
		return nil
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
		return nil
	}
	return ErrVerificationNotSupported
}

func ExportLiveDb(params Parameters, out sio.Writer) error {
	if params.Variant == "go-file" && params.Schema == 5 {
		liveDir := params.Directory + string(filepath.Separator) + "live"
		if err := io.Export(liveDir, out); err != nil {
			return fmt.Errorf("failed to export live state; %v", err)
		}
		return nil
	}
	return ErrExportNotSupported
}

func ImportLiveDb(params Parameters, reader sio.Reader) error {
	if params.Variant == "go-file" && params.Schema == 5 {
		liveDir := params.Directory + string(filepath.Separator) + "live"
		if err := os.MkdirAll(liveDir, 0700); err != nil {
			return fmt.Errorf("failed to create data dir; %v", err)
		}
		if err := io.ImportLiveDb(liveDir, reader); err != nil {
			return fmt.Errorf("failed to import live state; %v", err)
		}
		return nil
	}
	return ErrImportNotSupported
}

func InitializeArchive(params Parameters, reader sio.Reader, blockNum uint64) error {
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
		return nil
	}
	return ErrImportNotSupported
}
