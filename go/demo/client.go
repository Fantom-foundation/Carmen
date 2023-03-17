package demo

import (
	"bytes"
	"fmt"
	"log"

	"github.com/Fantom-foundation/Carmen/go/backend"
)

// Client is a (very) simplified implementation of a client
// capable of joining, interacting, and leafing a network.
type Client interface {
	// Call processes a point-to-point request and produces a response.
	Call(Message) Message
	// Observe processes a broadcast message.
	Observe(Message)
}

type DemoClient struct {
	// Block chain state.
	blockHeight uint64           // the current block height
	state       *DemoState       // the current state, expected to be synced among all nodes
	snapshot    backend.Snapshot // the latest snapshot, if any has been created

	// Network information.
	network   Network
	myAddress Address
}

func (c *DemoClient) Join(net Network) (err error) {
	// Register this client in the network.
	c.network = net
	defer func() {
		if err == nil {
			c.myAddress = net.Register(c)
		}
	}()

	// Sync with network state.
	addresses := net.GetAllAddresses()
	if len(addresses) == 0 {
		// This is the first client, initializing the network.
		c.blockHeight = 0
		c.state = newDemoState()
		var err error
		c.snapshot, err = c.state.CreateSnapshot()
		return err
	}

	// Start by getting the block height.
	peer := addresses[0]
	c.blockHeight = net.Call(peer, GetBlockHeightRequest{}).(GetBlockHeightResponse).BlockHeight

	// Sync to last snapshot in the network.
	c.state = newDemoState()
	remoteSnapshotData := newRemoteSnapshotData(net)

	metadata, err := remoteSnapshotData.GetMetaData()
	if err != nil {
		return err
	}

	// Check format of remote data and obtain verifier from state object.
	verifier, err := c.state.GetSnapshotVerifier(metadata)
	if err != nil {
		return err
	}

	// Verify the proof hierarchy of the remote snapshot data.
	// This fetches all proofs from remote nodes and verifies their correctness.
	rootProof, err := verifier.VerifyRootProof(remoteSnapshotData)
	if err != nil {
		return err
	}

	// Verify that the snapshot root proof is correct.
	// TODO: here some `trusted` source is required;
	rootProofData := net.Call(peer, GetSnapshotRootProofRequest{}).(GetSnapshotRootProofResponse).Data
	if !bytes.Equal(rootProofData, rootProof.ToBytes()) {
		return fmt.Errorf("invalid snapshot root hash")
	}

	// Configure the on-the-fly verification of snapshot parts during the Restore.
	remoteSnapshotData.SetPartVerifier(func(partNumber int, data []byte) error {
		// At this point those proofs should all be cached.
		proof, err := remoteSnapshotData.GetProofData(partNumber)
		if err != nil {
			return err
		}
		return verifier.VerifyPart(partNumber, proof, data)
	})

	// Restore the data.
	if err := c.state.Restore(remoteSnapshotData); err != nil {
		return err
	}

	// TODO: catch up to block height by processing missing updates

	return nil
}

func (c *DemoClient) GetStateProof() backend.Proof {
	proof, err := c.state.GetProof()
	if err != nil {
		panic(fmt.Sprintf("proof should alway be available in demo client, but got error %v", err))
	}
	return proof
}

func (c *DemoClient) Call(request Message) Message {
	switch r := request.(type) {
	case GetBlockHeightRequest:
		return GetBlockHeightResponse{c.blockHeight}
	case GetStateProofRequest:
		proof, err := c.state.GetProof()
		if err != nil {
			return ErrorMessage{err}
		}
		return GetStateProofResponse{proof}
	case GetSnapshotRootProofRequest:
		if c.snapshot == nil {
			return ErrorMessage{fmt.Errorf("no snapshot data available")}
		}
		return GetSnapshotRootProofResponse{c.snapshot.GetRootProof().ToBytes()}
	case GetSnapshotMetaDataRequest:
		if c.snapshot == nil {
			return ErrorMessage{fmt.Errorf("no snapshot data available")}
		}
		data, err := c.snapshot.GetData().GetMetaData()
		if err != nil {
			return ErrorMessage{err}
		}
		return GetSnapshotMetaDataResponse{data}
	case GetSnapshotProofRequest:
		if c.snapshot == nil {
			return ErrorMessage{fmt.Errorf("no snapshot data available")}
		}
		data, err := c.snapshot.GetData().GetProofData(r.PartNumber)
		if err != nil {
			return ErrorMessage{err}
		}
		return GetSnapshotProofResponse{data}
	case GetSnapshotPartRequest:
		if c.snapshot == nil {
			return ErrorMessage{fmt.Errorf("no snapshot data available")}
		}
		data, err := c.snapshot.GetData().GetPartData(r.PartNumber)
		if err != nil {
			return ErrorMessage{err}
		}
		return GetSnapshotPartResponse{data}
	}
	return ErrorMessage{fmt.Errorf("unsupported request")}
}

func (c *DemoClient) Observe(message Message) {
	switch msg := message.(type) {
	case BlockUpdateBroadcast:
		if c.blockHeight >= msg.block {
			return
		}
		c.blockHeight = msg.block
		for _, address := range msg.newAddresses {
			c.state.AddAddress(address)
		}
		for _, key := range msg.newKeys {
			c.state.AddKey(key)
		}
	case EndOfEpochBroadcast:
		var err error
		c.snapshot, err = c.state.CreateSnapshot()
		if err != nil {
			log.Printf("Failed to create snapshot: %v", err)
		}
	}
}
