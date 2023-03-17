package demo

import "testing"

func TestDemoNetwork_IsANetwork(t *testing.T) {
	var _ Network = &DemoNetwork{}
}
