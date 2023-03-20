package demo

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type blockGenerator struct {
	nextBlock uint64
}

func (g *blockGenerator) GetNextBlockUpdateMessage() Message {
	blockNumber := g.nextBlock
	g.nextBlock++
	a := byte(g.nextBlock)
	b := byte(g.nextBlock >> 8)
	c := byte(g.nextBlock >> 16)
	d := byte(g.nextBlock >> 24)
	return BlockUpdateBroadcast{
		block:        blockNumber,
		newAddresses: []common.Address{{a, b, c, d}},
		newKeys:      []common.Key{{a, b, c, d}, {d, c, b, a}},
	}
}

func TestStateSynchronization(t *testing.T) {
	const blocksPerIncrement = 1000

	blockGen := blockGenerator{}

	// We start by creating a network.
	var net Network = &DemoNetwork{}

	// Let's have our first client join the network.
	client1 := DemoClient{}
	if err := client1.Join(net); err != nil {
		t.Errorf("client 1 failed to join: %v", err)
	}

	// Run a few blocks and create a new snapshot.
	for i := 0; i < blocksPerIncrement; i++ {
		net.Broadcast(blockGen.GetNextBlockUpdateMessage())
	}
	net.Broadcast(EndOfEpochBroadcast{})

	// Now have another client join the network.
	client2 := DemoClient{}
	if err := client2.Join(net); err != nil {
		t.Errorf("client 2 failed to join: %v", err)
	}

	// Now, the state of client 1 and 2 should be in sync.
	if !client1.GetStateProof().Equal(client2.GetStateProof()) {
		t.Errorf("clients not in sync")
	}

	// Run a few more blocks and create a new snapshot.
	for i := 0; i < blocksPerIncrement; i++ {
		net.Broadcast(blockGen.GetNextBlockUpdateMessage())
	}
	net.Broadcast(EndOfEpochBroadcast{})

	client3 := DemoClient{}
	if err := client3.Join(net); err != nil {
		t.Errorf("client 3 failed to join: %v", err)
	}

	// Now, the state of client 1, 2 and 3 should be in sync.
	if !client1.GetStateProof().Equal(client2.GetStateProof()) {
		t.Errorf("clients not in sync")
	}
	if !client1.GetStateProof().Equal(client3.GetStateProof()) {
		t.Errorf("clients not in sync")
	}
}
