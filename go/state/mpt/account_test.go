package mpt

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestAccountInfo_EncodingAndDecoding(t *testing.T) {
	infos := []AccountInfo{
		{},
		{common.Nonce{1, 2, 3}, common.Balance{4, 5, 6}, common.Hash{7, 8, 9}},
	}

	encoder := AccountInfoEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	for _, info := range infos {
		if err := encoder.Store(buffer[:], &info); err != nil {
			t.Fatalf("failed to encode info: %v", info)
		}
		restored := AccountInfo{}
		if err := encoder.Load(buffer[:], &restored); err != nil || restored != info {
			t.Fatalf("failed to decode info %v: got %v, err %v", info, restored, err)
		}
	}
}
