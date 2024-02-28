package common

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
)

func ExecuteTestInSubProcess(t *testing.T, testName string, args ...string) {
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("failed to resolve path to test binary: %v", err)
	}

	commandArgs := []string{"-test.run", testName}
	commandArgs = append(commandArgs, args...)
	cmd := exec.Command(path, commandArgs...)
	errBuf := new(bytes.Buffer)
	cmd.Stderr = errBuf
	stdBuf := new(bytes.Buffer)
	cmd.Stdout = stdBuf

	if err := cmd.Run(); err != nil {
		t.Errorf("Subprocess finished with error: %v\n stdout:\n%s stderr:\n%s", err, stdBuf.String(), errBuf.String())
	}
}
