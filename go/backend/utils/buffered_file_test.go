// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package utils

import (
	"bytes"
	"fmt"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/slices"
	"os"
	"testing"
)

func TestBufferedFile_Open_NonExisting(t *testing.T) {
	path := "/test.dat"
	_, err := OpenBufferedFile(path)
	if err == nil {
		t.Errorf("file should not be opened")
	}
}

func TestBufferedFile_Open_TooSmallFile(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("cannot create test file: %s", err)
	}

	_, err = file.WriteString("Hello, World!")
	if err != nil {
		t.Fatalf("cannot create test content: %s", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
	}

	_, err = OpenBufferedFile(path)
	if err == nil {
		t.Errorf("file should be too small")
	}
}

func TestBufferedFile_FileStatsFailing(t *testing.T) {
	ctrl := gomock.NewController(t)
	f := NewMockOsFile(ctrl)

	var info os.FileInfo
	err := fmt.Errorf("cannot get file stat")
	f.EXPECT().Stat().Return(info, err)
	f.EXPECT().Close()

	if _, err := openBufferedFile(f); err == nil {
		t.Errorf("openning file should produce error")
	}
}

func TestBufferedFile_Open_ReadFailing(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(bufferSize))

	f := NewMockOsFile(ctrl)
	err := fmt.Errorf("cannot read file")
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).Return(0, err)
	f.EXPECT().Close()

	if _, err := openBufferedFile(f); err == nil {
		t.Errorf("openning file should produce error")
	}
}

func TestBufferedFile_ReadBeyondSize(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).AnyTimes().Return(bufferSize, nil)

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("openning file should not produce error, %s", err)
	}

	dst := make([]byte, 1)
	dst[0] = 0xAA
	if _, err := bf.ReadAt(dst, 2*bufferSize); err != nil {
		t.Errorf("reading should not fail: %s", err)
	}
	if dst[0] != 0x0 {
		t.Errorf("reading beyond the file size should return an empty data")
	}
}

func TestBufferedFile_ReadPartlyBeyondSize(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(2 * bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).Times(2).Return(bufferSize, nil)

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("openning file should not produce error, %s", err)
	}

	for i := 0; i < bufferSize; i++ {
		bf.buffer[i] = byte(i)
	}

	got := make([]byte, 3*bufferSize)
	if _, err := bf.ReadAt(got, 0.5*bufferSize); err != nil {
		t.Errorf("reading should not fail: %s", err)
	}

	want := make([]byte, 3*bufferSize)
	copy(want, bf.buffer[0:0.5*bufferSize])

	if !slices.Equal(want, got) {
		t.Errorf("read data does not match: %x != %x", got, want)
	}
}

func TestBufferedFile_ReadPartlyBeyondSizeFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(2 * bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	var count int
	f.EXPECT().Read(gomock.Any()).Times(2).DoAndReturn(func([]byte) (int, error) {
		if count == 0 {
			count++
			return bufferSize, nil
		} else {
			// second call will fail to fail reading in the second split
			err := fmt.Errorf("second call is failing")
			return 0, err
		}
	})

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("openning file should not produce error, %s", err)
	}

	got := make([]byte, 3*bufferSize)
	if _, err := bf.ReadAt(got, 0.5*bufferSize); err == nil {
		t.Errorf("reading should fail")
	}

}

func TestBufferedFile_ReadSplit(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(2 * bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).Times(2).Return(bufferSize, nil)

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("openning file should not produce error, %s", err)
	}

	for i := 0; i < bufferSize; i++ {
		bf.buffer[i] = byte(i)
	}

	got := make([]byte, bufferSize)
	if _, err := bf.ReadAt(got, 0.5*bufferSize); err != nil {
		t.Errorf("reading should not fail: %s", err)
	}

	want := make([]byte, bufferSize)
	copy(want, bf.buffer[0:0.5*bufferSize])

	if !slices.Equal(want, got) {
		t.Errorf("read data does not match: %x != %x", got, want)
	}
}

func TestBufferedFile_Write_SeekFailing(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).AnyTimes().Return(bufferSize, nil)
	err := fmt.Errorf("cannot seek file")
	f.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), err)

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("cannot open file: %s", err)
	}

	if _, err := bf.WriteAt([]byte{0xA}, 2*bufferSize); err == nil {
		t.Errorf("writing should file")
	}
}

func TestBufferedFile_Write_SeekWrongPosition(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).AnyTimes().Return(bufferSize, nil)
	f.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(999), nil) // wrong position

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("cannot open file: %s", err)
	}

	if _, err := bf.WriteAt([]byte{0xA}, 2*bufferSize); err == nil {
		t.Errorf("writing should file")
	}
}

func TestBufferedFile_Write_Failing(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).AnyTimes().Return(bufferSize, nil)
	f.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), nil)
	err := fmt.Errorf("cannot write")
	f.EXPECT().Write(gomock.Any()).Return(0, err)

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("cannot open file: %s", err)
	}

	if _, err := bf.WriteAt([]byte{0xA}, 2*bufferSize); err == nil {
		t.Errorf("writing should file")
	}
}

func TestBufferedFile_Write_FailingDueToRead(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(10 * bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).AnyTimes().Return(bufferSize, nil)
	// second time it will seek to an unexpected position, causing the failure
	f.EXPECT().Seek(gomock.Any(), gomock.Any()).Times(2).Return(int64(0), nil)
	f.EXPECT().Write(gomock.Any()).Return(bufferSize, nil)

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("cannot open file: %s", err)
	}

	if _, err := bf.WriteAt([]byte{0xA}, 2*bufferSize); err == nil {
		t.Errorf("writing should file")
	}
}

func TestBufferedFile_Write_FailingNumOfWrites(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(bufferSize))

	f := NewMockOsFile(ctrl)
	f.EXPECT().Stat().Return(info, nil)
	f.EXPECT().Read(gomock.Any()).AnyTimes().Return(bufferSize, nil)
	f.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), nil)
	f.EXPECT().Write(gomock.Any()).Return(0, nil) // written zero elements

	bf, err := openBufferedFile(f)
	if err != nil {
		t.Fatalf("cannot open file: %s", err)
	}

	if _, err := bf.WriteAt([]byte{0xA}, 2*bufferSize); err == nil {
		t.Errorf("writing should file")
	}
}

func TestBufferedFile_OpenClose(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Errorf("failed to close buffered file: %v", err)
	}
}

func TestBufferedFile_ReadNegativePosition(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	if _, err := file.ReadAt([]byte{}, -1); err == nil {
		t.Errorf("reading should fail")
	}
}

func TestBufferedFile_WriteNegativePosition(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xAA}, -1); err == nil {
		t.Errorf("writing should fail")
	}
}

func TestBufferedFile_WrittenDataCanBeRead(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			path := t.TempDir() + "/test.dat"
			file, err := OpenBufferedFile(path)
			if err != nil {
				t.Fatalf("failed to open buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				if _, err := file.WriteAt([]byte{byte(i)}, int64(i)); err != nil {
					t.Fatalf("failed to write at position %d: %v", i, err)
				}
			}

			for i := 0; i < n; i++ {
				dst := []byte{0}
				if _, err := file.ReadAt(dst, int64(i)); err != nil {
					t.Fatalf("failed to read at position %d: %v", i, err)
				}
				if dst[0] != byte(i) {
					t.Errorf("invalid data read at postion %d, wanted %d, got %d", i, byte(i), dst[0])
				}
			}

			if err := file.Close(); err != nil {
				t.Errorf("failed to close buffered file: %v", err)
			}
		})
	}
}

func TestBufferedFile_DataIsPersistent(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			path := t.TempDir() + "/test.dat"
			file, err := OpenBufferedFile(path)
			if err != nil {
				t.Fatalf("failed to open buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				if _, err := file.WriteAt([]byte{byte(i + 1)}, int64(i)); err != nil {
					t.Fatalf("failed to write at position %d: %v", i, err)
				}
			}

			if err := file.Close(); err != nil {
				t.Fatalf("failed to close file: %v", err)
			}

			// Reopen the file.
			file, err = OpenBufferedFile(path)
			if err != nil {
				t.Fatalf("failed to reopen buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				dst := []byte{0}
				if _, err := file.ReadAt(dst, int64(i)); err != nil {
					t.Fatalf("failed to read at position %d: %v", i, err)
				}
				if dst[0] != byte(i+1) {
					t.Errorf("invalid data read at postion %d, wanted %d, got %d", i, byte(i+1), dst[0])
				}
			}

			if err := file.Close(); err != nil {
				t.Errorf("failed to close buffered file: %v", err)
			}
		})
	}
}

func TestBufferedFile_ReadAndWriteCanHandleUnalignedData(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	// By writting data of length 3 we are sometimes writing data crossing
	// the internal aligned buffer-page boundary.
	for i := 0; i < 1000; i++ {
		if _, err := file.WriteAt([]byte{byte(i), byte(i + 1), byte(i + 2)}, int64(i)*3); err != nil {
			t.Fatalf("failed to write at position %d: %v", i, err)
		}
	}

	for i := 0; i < 1000; i++ {
		dst := []byte{0, 0, 0}
		if _, err := file.ReadAt(dst, int64(i)*3); err != nil {
			t.Fatalf("failed to read at position %d: %v", i, err)
		}
		want := []byte{byte(i), byte(i + 1), byte(i + 2)}
		if !bytes.Equal(dst, want) {
			t.Errorf("invalid data read at postion %d, wanted %d, got %d", i, want, dst)
		}
	}

	if err := file.Close(); err != nil {
		t.Errorf("failed to close buffered file: %v", err)
	}
}

func TestBufferedFile_WriteAndReadAddBufferBoundary(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	src := []byte{1, 2, 3, 4, 5}
	file.WriteAt(src, 5*bufferSize-2)

	dst := []byte{0, 0, 0, 0, 0}
	file.ReadAt(dst, 5*bufferSize-2)

	if !bytes.Equal(src, dst) {
		t.Errorf("failed to read data written across buffer boundary, wanted %v, got %v", src, dst)
	}
}
