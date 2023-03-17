package demo

import (
	"testing"
)

func TestDemoClient_IsAClient(t *testing.T) {
	var _ Client = &DemoClient{}
}
