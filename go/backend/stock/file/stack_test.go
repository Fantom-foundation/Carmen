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
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestStack_OpenClose(t *testing.T) {
	stack, err := openFileBasedStack[int](t.TempDir() + "/stack.dat")
	if err != nil {
		t.Fatalf("failed to open empty stack: %v", err)
	}
	if err := stack.Close(); err != nil {
		t.Fatalf("failed to close stack: %v", err)
	}
}

func TestStack_PushAndPop(t *testing.T) {
	stack, err := openFileBasedStack[int](t.TempDir() + "/stack.dat")
	if err != nil {
		t.Fatalf("failed to open empty stack: %v", err)
	}
	defer stack.Close()

	if err := stack.Push(12); err != nil {
		t.Fatalf("failed to push element: %v", err)
	}

	if err := stack.Push(14); err != nil {
		t.Fatalf("failed to push element: %v", err)
	}

	if got, err := stack.Pop(); err != nil || got != 14 {
		t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
	}

	if got, err := stack.Pop(); err != nil || got != 12 {
		t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
	}
}

func TestStack_LargePushAndPop(t *testing.T) {
	stack, err := openFileBasedStack[int](t.TempDir() + "/stack.dat")
	if err != nil {
		t.Fatalf("failed to open empty stack: %v", err)
	}
	defer stack.Close()

	for i := 0; i < 10*stackBufferSize; i++ {
		if got, want := stack.Size(), i; got != want {
			t.Fatalf("invalid size, wanted %d, got %d", want, got)
		}
		if err := stack.Push(i); err != nil {
			t.Fatalf("failed to push element: %v", err)
		}
	}

	for i := 10*stackBufferSize - 1; i >= 0; i-- {
		if got, err := stack.Pop(); err != nil || got != i {
			t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
		}
		if got, want := stack.Size(), i; got != want {
			t.Fatalf("invalid size, wanted %d, got %d", want, got)
		}
	}
}

func TestStack_CloseAndReopen(t *testing.T) {
	file := t.TempDir() + "/stack.dat"
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to open empty stack: %v", err)
		}

		if err := stack.Push(12); err != nil {
			t.Fatalf("failed to push element: %v", err)
		}

		if err := stack.Push(14); err != nil {
			t.Fatalf("failed to push element: %v", err)
		}

		if err := stack.Close(); err != nil {
			t.Fatalf("failed to close stack: %v", err)
		}
	}

	// Reopen stack and check whether content is preserved.
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to re-open stack: %v", err)
		}
		defer stack.Close()

		if got, want := stack.Size(), 2; got != want {
			t.Fatalf("invalid stack size after reopening, wanted %d, got %d", want, got)
		}

		if got, err := stack.Pop(); err != nil || got != 14 {
			t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
		}

		if got, err := stack.Pop(); err != nil || got != 12 {
			t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
		}
	}
}

func TestStack_CloseAndReopenLarge(t *testing.T) {
	N := 10*stackBufferSize + 123
	file := t.TempDir() + "/stack.dat"
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to open empty stack: %v", err)
		}

		for i := 0; i < N; i++ {
			if got, want := stack.Size(), i; got != want {
				t.Fatalf("invalid size, wanted %d, got %d", want, got)
			}
			if err := stack.Push(i); err != nil {
				t.Fatalf("failed to push element: %v", err)
			}
		}

		if err := stack.Close(); err != nil {
			t.Fatalf("failed to close stack: %v", err)
		}
	}

	// Reopen stack and check whether content is preserved.
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to re-open stack: %v", err)
		}
		defer stack.Close()

		if got, want := stack.Size(), N; got != want {
			t.Fatalf("invalid stack size after reopening, wanted %d, got %d", want, got)
		}

		for i := N - 1; i >= 0; i-- {
			if got, err := stack.Pop(); err != nil || got != i {
				t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
			}
			if got, want := stack.Size(), i; got != want {
				t.Fatalf("invalid size, wanted %d, got %d", want, got)
			}
		}
	}
}

func TestStack_Open_FailCreate(t *testing.T) {
	file := "/root/file.x"
	if _, err := openFileBasedStack[int](file); err == nil {
		t.Errorf("cration of stack should fail")
	}
}

func TestStack_Open_FailSeek(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("expected error")) // causes init error

	if _, err := initFileBasedStack[int](file); err == nil {
		t.Errorf("cration of stack should fail")
	}
}

func TestStack_Open_FailRead(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), nil)
	file.EXPECT().Read(gomock.Any()).Return(0, fmt.Errorf("expected error")) // causes init error

	if _, err := initFileBasedStack[int](file); err == nil {
		t.Errorf("cration of stack should fail")
	}
}

func TestStack_Push_FailSeek(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	var counter int
	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(offset int64, whence int) (int64, error) {
		var err error
		if counter >= 1 {
			err = fmt.Errorf("expected error")
		}
		counter++
		return 0, err
	})
	file.EXPECT().Read(gomock.Any()).Return(8, nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	// fil-in the buffer to cause calling flush to the file
	for i := 0; i < cap(stack.buffer); i++ {
		err = errors.Join(err, stack.Push(10))
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Errorf("pushing value to stack should fail")
	}
}

func TestStack_Push_FailWrite(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	file.EXPECT().Write(gomock.Any()).Return(0, fmt.Errorf("expected error")) // causes init error
	file.EXPECT().Read(gomock.Any()).AnyTimes().Return(8, nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	// fil-in the buffer to cause calling flush to the file
	for i := 0; i < cap(stack.buffer); i++ {
		err = errors.Join(err, stack.Push(10))
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Errorf("pushing value to stack should fail")
	}
}

func TestStack_Pop_FailSeek(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	var counter int
	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(offset int64, whence int) (int64, error) {
		var err error
		if counter >= 1 {
			err = fmt.Errorf("expected error")
		}
		counter++
		return 0, err
	})
	file.EXPECT().Read(gomock.Any()).Return(8, nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	stack.buffer = stack.buffer[0:0] // trick empty buffer to force reading from file
	if _, err := stack.Pop(); err == nil {
		t.Errorf("popping value to stack should fail")
	}
}

func TestStack_Pop_FailRead(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	var counter int
	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	file.EXPECT().Read(gomock.Any()).AnyTimes().DoAndReturn(func([]byte) (int, error) {
		var err error
		if counter >= 9 { // 8time one byte +1
			err = fmt.Errorf("expected error")
		}
		counter++
		return 1, err
	})

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	stack.buffer = stack.buffer[0:0] // trick empty buffer to force reading from file
	if _, err := stack.Pop(); err == nil {
		t.Errorf("popping value to stack should fail")
	}
}

func TestStack_GetAll_FailSeek(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	var counter int
	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(offset int64, whence int) (int64, error) {
		var err error
		if counter >= 2 { // 1x Init, 1x, Flush, then failure
			err = fmt.Errorf("expected error")
		}
		counter++
		return 0, err
	})
	file.EXPECT().Read(gomock.Any()).Return(8, nil)
	file.EXPECT().Write(gomock.Any()).AnyTimes().Return(0, nil)
	file.EXPECT().Truncate(gomock.Any()).AnyTimes().Return(nil)
	file.EXPECT().Sync().AnyTimes().Return(nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	if _, err := stack.GetAll(); err == nil {
		t.Errorf("popping value to stack should fail")
	}
}

func TestStack_GetAll_FailRead(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	var counter int
	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	file.EXPECT().Read(gomock.Any()).AnyTimes().DoAndReturn(func([]byte) (int, error) {
		var err error
		if counter >= 9 {
			err = fmt.Errorf("expected error")
		}
		counter++
		return 1, err
	})
	file.EXPECT().Write(gomock.Any()).AnyTimes().Return(0, nil)
	file.EXPECT().Truncate(gomock.Any()).AnyTimes().Return(nil)
	file.EXPECT().Sync().AnyTimes().Return(nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	if _, err := stack.GetAll(); err == nil {
		t.Errorf("popping value to stack should fail")
	}
}

func TestStack_Flush_FailTruncate(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	file.EXPECT().Read(gomock.Any()).Return(8, nil)
	file.EXPECT().Truncate(gomock.Any()).Return(fmt.Errorf("expected error"))
	file.EXPECT().Sync().AnyTimes().Return(nil)
	file.EXPECT().Write(gomock.Any()).AnyTimes().Return(0, nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	if err := stack.Flush(); err == nil {
		t.Errorf("popping value to stack should fail")
	}
}

func TestStack_Flush_FailSync(t *testing.T) {
	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Stat().Return(info, nil)
	file.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	file.EXPECT().Read(gomock.Any()).Return(8, nil)
	file.EXPECT().Truncate(gomock.Any()).Return(nil)
	file.EXPECT().Sync().Return(fmt.Errorf("expected error"))
	file.EXPECT().Write(gomock.Any()).AnyTimes().Return(0, nil)

	stack, err := initFileBasedStack[int](file)
	if err != nil {
		t.Errorf("cannot init stack: %s", err)
	}

	if err := stack.Flush(); err == nil {
		t.Errorf("popping value to stack should fail")
	}
}
