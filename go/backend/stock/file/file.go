// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package file

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type fileStock[I stock.Index, V any] struct {
	directory       string
	encoder         stock.ValueEncoder[V]
	values          utils.SeekableFile
	freelist        *fileBasedStack[I]
	numValueSlots   I
	numValuesInFile int64
	bufferPool      sync.Pool

	// Checkpoint related fields.
	lastCheckpoint               checkpoint.Checkpoint
	numValuesCoveredByCheckpoint I
	freeListSizeOfCheckpoint     int
}

// OpenStock opens a stock retained in the given directory. To that end, meta
// data is loaded and verified. A non-existing directory will be implicitly
// created and an empty directory is a valid target to be initialized as an
// empty stock.
func OpenStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (stock.Stock[I, V], error) {
	return openStock[I, V](encoder, directory)
}

func openStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (*fileStock[I, V], error) {
	return openVerifyStock[I, V](encoder, directory, verifyStockInternal[I, V])
}

// openVerifyStock opens the stock the same as its public counterpart. This method allows for injecting a custom method to verify the stock.
func openVerifyStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string, verify func(encoder stock.ValueEncoder[V], directory string) (metadata, error)) (*fileStock[I, V], error) {
	// Create the directory if needed.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}

	// Verify the content of the stock and get its metadata.
	meta, err := verify(encoder, directory)
	if err != nil {
		return nil, err
	}

	_, valuefile, freelistfile := getFileNames(directory)
	values, err := utils.OpenBufferedFile(valuefile)
	if err != nil {
		return nil, err
	}

	freelist, err := openFileBasedStack[I](freelistfile)
	if err != nil {
		return nil, err
	}

	var committed checkpointMetaData
	commitMetaDataFile := getCommittedCheckpointFile(directory)
	if exists(commitMetaDataFile) {
		committed, err = utils.ReadJsonFile[checkpointMetaData](commitMetaDataFile)
		if err != nil {
			return nil, err
		}
	}

	// Create new files
	valueSize := encoder.GetEncodedSize()
	return &fileStock[I, V]{
		encoder:         encoder,
		directory:       directory,
		values:          values,
		freelist:        freelist,
		numValueSlots:   I(meta.ValueListLength),
		numValuesInFile: meta.NumValuesInFile,
		bufferPool: sync.Pool{New: func() any {
			return &buffer[V]{
				raw: make([]byte, valueSize),
			}
		}},
		lastCheckpoint:               committed.Checkpoint,
		numValuesCoveredByCheckpoint: I(committed.Metadata.ValueListLength),
		freeListSizeOfCheckpoint:     int(committed.Metadata.FreeListLength),
	}, nil
}

// buffer combines a raw data and value buffer required in pairs for Get and Set
// operations. Instances are cached in sync.Pools to avoid allocations for every
// single use.
type buffer[V any] struct {
	raw   []byte
	value V
}

// VerifyStock verifies the consistency of the meta-information maintained for the stock in
// the given directory. This includes
//   - checking the presence and size of all required files
//   - checking the correct metadata for a stock using the given index and encoder
//   - checking the value range of elements in the free-list
//
// For compatibility with the OpenStock function above, an empty directory is considered a
// valid stock as well.
func VerifyStock[I stock.Index, V any](directory string, encoder stock.ValueEncoder[V]) error {
	if !isDirectory(directory) {
		return fmt.Errorf("directory %v does not exist", directory)
	}
	_, err := verifyStockInternal[I, V](encoder, directory)
	return err
}

func verifyStockInternal[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (metadata, error) {
	var meta metadata

	// If none of the needed files exist, the stock is empty and thus consistent.
	metafile, valuefile, freelistfile := getFileNames(directory)
	if !exists(metafile) && !exists(valuefile) && !exists(freelistfile) {
		return meta, nil
	}

	// Missing files are a problem.
	for _, file := range []string{metafile, valuefile, freelistfile} {
		if !exists(file) {
			return meta, fmt.Errorf("required `%v` not found", file)
		}
	}

	return verifyStockFilesInternal[I](encoder, metafile, valuefile, freelistfile)
}

func verifyStockFilesInternal[I stock.Index, V any](encoder stock.ValueEncoder[V], metafile, valuefile, freelistfile string) (metadata, error) {
	// Attempt to parse the meta-data.
	meta, err := utils.ReadJsonFile[metadata](metafile)
	if err != nil {
		return meta, err
	}

	// Check meta-data format information.
	if meta.Version != dataFormatVersion {
		return meta, fmt.Errorf("invalid file format version, got %d, wanted %d", meta.Version, dataFormatVersion)
	}
	indexSize := int(unsafe.Sizeof(I(0)))
	if meta.IndexTypeSize != indexSize {
		return meta, fmt.Errorf("invalid index type encoding, expected %d byte, found %d", indexSize, meta.IndexTypeSize)
	}
	valueSize := encoder.GetEncodedSize()
	if meta.ValueTypeSize != valueSize {
		return meta, fmt.Errorf("invalid value type encoding, expected %d byte, found %d", valueSize, meta.ValueTypeSize)
	}

	// Check size of the value file.
	{
		stats, err := os.Stat(valuefile)
		if err != nil {
			return meta, err
		}
		expectedSize := meta.NumValuesInFile * int64(valueSize)
		if got, want := stats.Size(), expectedSize; got < want {
			return meta, fmt.Errorf("insufficient value file size, got %d, wanted %d", got, want)
		}
	}

	// check stack file
	freelist, err := os.OpenFile(freelistfile, os.O_RDWR, 0644)
	if err != nil {
		return meta, err
	}
	defer freelist.Close()
	if err := verifyStackInternal[I](meta, freelist); err != nil {
		return meta, err
	}

	return meta, nil
}

func verifyStackInternal[I stock.Index](meta metadata, freelistfile utils.OsFile) error {
	// Check size of the free-list file.
	{
		indexSize := int(unsafe.Sizeof(I(0)))
		stats, err := freelistfile.Stat()
		if err != nil {
			return err
		}
		if got, want := stats.Size(), int64(meta.FreeListLength*indexSize); got != want {
			return fmt.Errorf("invalid free-list file size, got %d, wanted %d", got, want)
		}
	}

	// Check the content of the free-list file.
	{
		stack, err := initFileBasedStack[I](freelistfile)
		if err != nil {
			return err
		}
		defer stack.Close()
		list, err := stack.GetAll()
		if err != nil {
			return err
		}
		for _, entry := range list {
			if entry < 0 || entry >= I(meta.ValueListLength) {
				return fmt.Errorf("invalid value in free list: %d not in range [%d,%d)", entry, 0, meta.ValueListLength)
			}
		}
	}

	return nil
}

const (
	fileNameMetadata            = "meta.json"
	fileNameValues              = "values.dat"
	fileNameFreeList            = "freelist.dat"
	fileNameCommittedCheckpoint = "committed.json"
	fileNamePendingCheckpoint   = "pending.json"
)

func getFileNames(directory string) (metafile string, valuefile string, freelistfile string) {
	metafile = filepath.Join(directory, fileNameMetadata)
	valuefile = filepath.Join(directory, fileNameValues)
	freelistfile = filepath.Join(directory, fileNameFreeList)
	return
}

func getCommittedCheckpointFile(directory string) string {
	return filepath.Join(directory, fileNameCommittedCheckpoint)
}

func getPendingCheckpointFile(directory string) string {
	return filepath.Join(directory, fileNamePendingCheckpoint)
}

func (s *fileStock[I, V]) New() (I, error) {
	index := s.numValueSlots

	// Reuse free index positions or grow list of values.
	// However, committed parts of the free-list must not
	// be reused any more.
	if s.freelist.Size() > s.freeListSizeOfCheckpoint {
		free, err := s.freelist.Pop()
		if err != nil {
			return 0, err
		}
		index = free
	} else {
		s.numValueSlots++
	}

	return I(index), nil
}

func (s *fileStock[I, V]) Get(index I) (V, error) {
	var res V
	if index >= I(s.numValueSlots) || index < 0 {
		return res, nil
	}
	if index >= I(s.numValuesInFile) {
		return res, nil
	}
	// Load value from the file.
	valueSize := s.encoder.GetEncodedSize()
	offset := int64(valueSize) * int64(index)
	buffer := s.bufferPool.Get().(*buffer[V])
	defer s.bufferPool.Put(buffer)
	_, err := s.values.ReadAt(buffer.raw, offset)
	if err != nil {
		return res, err
	}

	if err := s.encoder.Load(buffer.raw, &buffer.value); err != nil {
		return res, err
	}
	return buffer.value, nil
}

func (s *fileStock[I, V]) Set(index I, value V) error {
	if index >= s.numValueSlots || index < 0 {
		return fmt.Errorf("index out of range, got %d, range [0,%d)", index, s.numValueSlots)
	}
	if index < s.numValuesCoveredByCheckpoint {
		return fmt.Errorf("index %d is already committed and cannot be updated any more", index)
	}

	// Encode the value to be written.
	valueSize := s.encoder.GetEncodedSize()
	buffer := s.bufferPool.Get().(*buffer[V])
	defer s.bufferPool.Put(buffer)
	buffer.value = value
	if err := s.encoder.Store(buffer.raw, &buffer.value); err != nil {
		return err
	}

	// If the new data is beyond the end of the current file and empty, we can skip
	// the write operation.
	if index >= I(s.numValuesInFile) && allZero(buffer.raw) {
		return nil
	}

	// Write a serialized form of the value to disk.
	offset := int64(valueSize) * int64(index)
	if _, err := s.values.WriteAt(buffer.raw, offset); err != nil {
		return err
	}
	if int64(index) >= s.numValuesInFile {
		s.numValuesInFile = int64(index) + 1
	}
	return nil
}

func (s *fileStock[I, V]) Delete(index I) error {
	if index >= s.numValueSlots || index < 0 {
		return nil
	}
	if index < s.numValuesCoveredByCheckpoint {
		return fmt.Errorf("index %d is already committed and cannot be updated any more", index)
	}
	return s.freelist.Push(index)
}

func (s *fileStock[I, V]) GetIds() (stock.IndexSet[I], error) {
	free, err := s.freelist.GetAll()
	if err != nil {
		return nil, err
	}
	res := stock.MakeComplementSet[I](0, s.numValueSlots)
	for _, i := range free {
		res.Remove(i)
	}
	return res, nil
}

func (s *fileStock[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	res := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	res.AddChild("freelist", s.freelist.GetMemoryFootprint())
	return res
}

func (s *fileStock[I, V]) Flush() error {
	meta, _, _ := getFileNames(s.directory)
	return errors.Join(
		utils.WriteJsonFile(meta, s.getMetadata()),
		s.values.Flush(),
		s.freelist.Flush(),
	)
}

func (s *fileStock[I, V]) getMetadata() metadata {
	var index I
	indexSize := int(unsafe.Sizeof(index))
	return metadata{
		Version:         dataFormatVersion,
		IndexTypeSize:   indexSize,
		ValueTypeSize:   s.encoder.GetEncodedSize(),
		ValueListLength: int(s.numValueSlots),
		FreeListLength:  s.freelist.Size(),
		NumValuesInFile: s.numValuesInFile,
	}
}

func (s *fileStock[I, V]) Close() error {
	// Flush is executed before the closing of the file since
	// in Go the evaluation order of arguments is fixed.
	// see: https://go.dev/ref/spec#Order_of_evaluation
	return errors.Join(
		s.Flush(),
		s.values.Close(),
		s.freelist.Close(),
	)
}

func (s *fileStock[I, V]) GuaranteeCheckpoint(checkpoint checkpoint.Checkpoint) error {
	// If the stock is at the requested commit, everything is fine.
	if s.lastCheckpoint == checkpoint {
		return nil
	}

	// If the stock is behind the requested commit, it can be brought up to date.
	pendingFile := getPendingCheckpointFile(s.directory)
	if exists(pendingFile) {
		meta, err := utils.ReadJsonFile[checkpointMetaData](pendingFile)
		if err != nil {
			return err
		}
		if meta.Checkpoint == checkpoint {
			return s.Commit(checkpoint)
		}
	}

	// Otherwise, the requested checkpoint can not be guaranteed.
	return fmt.Errorf("unable to guarantee availability of checkpoint %d", checkpoint)
}

func (s *fileStock[I, V]) Prepare(checkpoint checkpoint.Checkpoint) error {
	if want, got := s.lastCheckpoint+1, checkpoint; want != got {
		return fmt.Errorf("invalid next checkpoint, expected %d, got %d", want, got)
	}

	if err := s.Flush(); err != nil {
		return err
	}

	// As part of the preparation, existing slots are sealed. Those slots
	// should not be updated any more after the flush.
	s.numValuesCoveredByCheckpoint = s.numValueSlots
	s.freeListSizeOfCheckpoint = s.freelist.Size()

	pendingFile := getPendingCheckpointFile(s.directory)
	return utils.WriteJsonFile(pendingFile, checkpointMetaData{
		Checkpoint: checkpoint,
		Metadata:   s.getMetadata(),
	})
}

func (s *fileStock[I, V]) Commit(checkpoint checkpoint.Checkpoint) error {
	if want, got := s.lastCheckpoint+1, checkpoint; want != got {
		return fmt.Errorf("invalid next checkpoint, expected %d, got %d", want, got)
	}
	pendingFile := getPendingCheckpointFile(s.directory)
	meta, err := utils.ReadJsonFile[checkpointMetaData](pendingFile)
	if err != nil {
		return err
	}
	if meta.Checkpoint != checkpoint {
		return fmt.Errorf("inconsistent data in checkpoint metadata, expected checkpoint %d, got %d", checkpoint, meta.Checkpoint)
	}

	committedFile := getCommittedCheckpointFile(s.directory)
	if err := os.Rename(pendingFile, committedFile); err != nil {
		return err
	}

	s.lastCheckpoint = checkpoint
	s.numValuesCoveredByCheckpoint = I(meta.Metadata.ValueListLength)
	s.freeListSizeOfCheckpoint = int(meta.Metadata.FreeListLength)
	return nil
}

func (s *fileStock[I, V]) Abort(checkpoint checkpoint.Checkpoint) error {
	if want, got := s.lastCheckpoint+1, checkpoint; want != got {
		return fmt.Errorf("invalid next checkpoint, expected %d, got %d", want, got)
	}

	// Get last committed checkpoint.
	meta, err := utils.ReadJsonFile[checkpointMetaData](getCommittedCheckpointFile(s.directory))
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		meta = checkpointMetaData{}
	}

	// Delete pending checkpoint file.
	pendingFile := getPendingCheckpointFile(s.directory)
	if err := os.Remove(pendingFile); err != nil {
		return err
	}

	// Reset checkpoint boundaries to previous checkpoint.
	s.numValuesCoveredByCheckpoint = I(meta.Metadata.ValueListLength)
	s.freeListSizeOfCheckpoint = int(meta.Metadata.FreeListLength)
	return nil
}

func GetRestorer(directory string) *fileStockRestorer {
	return &fileStockRestorer{
		directory: directory,
	}
}

type fileStockRestorer struct {
	directory string
}

func (s *fileStockRestorer) Restore(checkpoint checkpoint.Checkpoint) error {
	// If the stock is at the requested commit, everything is fine.
	committedFile := getCommittedCheckpointFile(s.directory)
	if checkpoint != 0 && !exists(committedFile) {
		return fmt.Errorf("failed to restore checkpoint %d, missing committed file %v", checkpoint, committedFile)
	}

	checkpointData, err := utils.ReadJsonFile[checkpointMetaData](committedFile)
	if err != nil {
		return err
	}
	if checkpointData.Checkpoint != checkpoint {
		return fmt.Errorf("inconsistent data in checkpoint metadata, expected checkpoint %d, got %d", checkpoint, checkpointData.Checkpoint)
	}

	// fix individual files
	meta, values, freelist := getFileNames(s.directory)

	// the meta data file can be simply restored from the checkpoint
	if err := utils.WriteJsonFile(meta, checkpointData.Metadata); err != nil {
		return err
	}

	// truncate the values file to the expected size (rounded up to the next 4k boundary)
	length := int64(checkpointData.Metadata.ValueListLength) * int64(checkpointData.Metadata.ValueTypeSize)
	if length%4096 != 0 {
		length = (length/4096 + 1) * 4096
	}
	if err := os.Truncate(values, length); err != nil {
		return err
	}

	// truncate freelist
	length = int64(checkpointData.Metadata.FreeListLength) * int64(checkpointData.Metadata.IndexTypeSize)
	if err := os.Truncate(freelist, length); err != nil {
		return err
	}

	pendingFile := getPendingCheckpointFile(s.directory)
	if exists(pendingFile) {
		if err := os.Remove(pendingFile); err != nil {
			return err
		}
	}

	return nil
}

const dataFormatVersion = 0

// metadata is the helper type to read and write metadata from/to the disk.
type metadata struct {
	Version         int
	IndexTypeSize   int
	ValueTypeSize   int
	ValueListLength int
	FreeListLength  int
	NumValuesInFile int64
}

func allZero(data []byte) bool {
	for _, cur := range data {
		if cur != 0 {
			return false
		}
	}
	return true
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

type checkpointMetaData struct {
	Checkpoint checkpoint.Checkpoint
	Metadata   metadata
}
