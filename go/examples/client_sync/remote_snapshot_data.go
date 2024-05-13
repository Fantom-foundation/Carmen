// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package demo

import (
	"fmt"
	"log"
	"math/rand"
)

const numRetries = 5

type PartVerifier func(partNumber int, data []byte) error

type RemoteSnapshotData struct {
	net          Network
	metaData     []byte         // cached version of the meta data, for local reuse during verifications
	proofCache   map[int][]byte // cached version of the proof data, for local reuse during verifications
	partVerifier PartVerifier   // a function to verify the correctness of parts, may be nil
}

func newRemoteSnapshotData(network Network) *RemoteSnapshotData {
	return &RemoteSnapshotData{network, nil, map[int][]byte{}, nil}
}

func (d *RemoteSnapshotData) GetMetaData() ([]byte, error) {
	if d.metaData != nil {
		return d.metaData, nil
	}
	response := d.net.Call(d.getRandomPeer(), GetSnapshotMetaDataRequest{})
	switch r := response.(type) {
	case GetSnapshotMetaDataResponse:
		d.metaData = r.Data
		return r.Data, nil
	case ErrorMessage:
		return nil, r.Issue
	}
	return nil, fmt.Errorf("protocol error, unexpected response")
}

func (d *RemoteSnapshotData) GetProofData(partNumber int) ([]byte, error) {
	if data, exists := d.proofCache[partNumber]; exists {
		return data, nil
	}

	response := d.net.Call(d.getRandomPeer(), GetSnapshotProofRequest{partNumber})
	switch r := response.(type) {
	case GetSnapshotProofResponse:
		d.proofCache[partNumber] = r.Data
		return r.Data, nil
	case ErrorMessage:
		return nil, r.Issue
	}
	return nil, fmt.Errorf("protocol error, unexpected response")
}

func (d *RemoteSnapshotData) GetPartData(partNumber int) ([]byte, error) {
	for i := 0; i < numRetries; i++ {
		peer := d.getRandomPeer()
		response := d.net.Call(peer, GetSnapshotPartRequest{partNumber})
		switch r := response.(type) {
		case GetSnapshotPartResponse:
			if d.partVerifier != nil {
				if err := d.partVerifier(partNumber, r.Data); err != nil {
					log.Printf("Received invalid part from %d, error: %v", peer, err)
					continue
				}
			}
			return r.Data, nil
		case ErrorMessage:
			return nil, r.Issue
		}
		return nil, fmt.Errorf("protocol error, unexpected response")
	}
	return nil, fmt.Errorf("unable to obtain valid data for part %d", partNumber)
}

func (d *RemoteSnapshotData) SetPartVerifier(verifier PartVerifier) {
	d.partVerifier = verifier
}

func (d *RemoteSnapshotData) getRandomPeer() Address {
	all := d.net.GetAllAddresses()
	return all[rand.Intn(len(all))]
}
