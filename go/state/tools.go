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

// VerificationObserver is a listener interface for tracking the progress of the verification
// of a forest. It can, for instance, be implemented by a user interface to keep the user updated
// on current activities.
type VerificationObserver interface {
	StartVerification()
	Progress(msg string)
	EndVerification(res error)
}

// VerifyLiveDb validates the live state. If the test passes, the live state data
// stored in the respective directory can be considered valid.
func VerifyLiveDb(params Parameters, observer VerificationObserver) error {
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

// VerifyArchive validates the archive states database. If the test passes,
// the archive states data stored in the respective directory can be considered valid.
func VerifyArchive(params Parameters, observer VerificationObserver) error {
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

// ExportLiveDb opens a LiveDB instance retained in the given directory and writes
// its content to the given output writer. The result contains all the
// information required by the ImportLiveDb function below to reconstruct the full
// state of the LiveDB.
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

// ImportLiveDb creates a fresh StateDB in the given directory and fills it
// with the content read from the given reader.
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

// InitializeArchive creates a fresh Archive in the given directory containing
// the state read from the input stream at the given block. All states before
// the given block are empty.
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
