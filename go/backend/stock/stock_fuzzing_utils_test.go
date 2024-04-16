//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package stock

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestFuzzing_Campaign_Serialise_Deserialize(t *testing.T) {
	campaign := stockFuzzCampaign{true, nil}
	data := campaign.Init()
	var want []fuzzing.Operation[stockFuzzContext]
	var stream []byte
	for _, list := range data {
		for _, item := range list {
			want = append(want, item)
			stream = append(stream, item.Serialize()...)
		}
	}

	got := campaign.Deserialize(stream)
	assertOperationsEquals(t, got, want)
}

func TestFuzzing_Campaign_CanCreateAndClose(t *testing.T) {
	ctrl := gomock.NewController(t)

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)
	campaign.Cleanup(t, ctx)
}

func TestFuzzing_Campaign_CreateContextFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(nil)

	testingT := fuzzing.NewMockTestingT(ctrl)
	testingT.EXPECT().Fatalf(gomock.Any(), gomock.Any())
	testingT.EXPECT().TempDir().Return("")

	f := func(directory string) (Stock[int, int], error) {
		return stock, fmt.Errorf("expected error")
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(testingT)
	campaign.Cleanup(testingT, ctx)
}

func TestFuzzing_Operations_Applied(t *testing.T) {
	ctrl := gomock.NewController(t)

	indexSet := NewMockIndexSet[int](ctrl)
	indexSet.EXPECT().Contains(gomock.Any()).Return(true)

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(nil)
	stock.EXPECT().New().Return(0, nil)
	stock.EXPECT().Set(gomock.Any(), gomock.Any()).Return(nil)
	stock.EXPECT().Get(gomock.Any()).Return(0, nil)
	stock.EXPECT().Delete(gomock.Any()).Return(nil)
	stock.EXPECT().GetIds().Return(indexSet, nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)

	(&opNewId{}).Apply(t, ctx)
	(&opSet{0, 0}).Apply(t, ctx)
	(&opGet{0}).Apply(t, ctx)
	(&opGetIds{}).Apply(t, ctx)
	(&opDeleteId{0}).Apply(t, ctx)

	campaign.Cleanup(t, ctx)
}

func TestFuzzing_Operations_Applied_On_Failing_Stock(t *testing.T) {
	ctrl := gomock.NewController(t)

	testingT := fuzzing.NewMockTestingT(ctrl)
	testingT.EXPECT().Errorf(gomock.Any(), gomock.Any()).MinTimes(1)
	testingT.EXPECT().Fatalf(gomock.Any(), gomock.Any())

	indexSet := NewMockIndexSet[int](ctrl)
	indexSet.EXPECT().Contains(gomock.Any()).Return(true)

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(fmt.Errorf("expected error"))
	stock.EXPECT().New().Return(0, fmt.Errorf("expected error"))
	stock.EXPECT().Set(gomock.Any(), gomock.Any()).Return(fmt.Errorf("expected error"))
	stock.EXPECT().Get(gomock.Any()).Return(0, fmt.Errorf("expected error"))
	stock.EXPECT().Delete(gomock.Any()).Return(fmt.Errorf("expected error"))
	stock.EXPECT().GetIds().Return(indexSet, fmt.Errorf("expected error"))

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)

	(&opNewId{}).Apply(testingT, ctx)
	(&opSet{0, 0}).Apply(testingT, ctx)
	(&opGet{0}).Apply(testingT, ctx)
	(&opGetIds{}).Apply(testingT, ctx)
	(&opDeleteId{0}).Apply(testingT, ctx)

	campaign.Cleanup(testingT, ctx)
}

func TestFuzzing_Operations_Applied_Set_OutOfRange(t *testing.T) {
	ctrl := gomock.NewController(t)

	testingT := fuzzing.NewMockTestingT(ctrl)

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Set(gomock.Any(), gomock.Any()).Return(fmt.Errorf("index out of range"))
	stock.EXPECT().Close().Return(nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)

	(&opSet{0, 0}).Apply(testingT, ctx)

	campaign.Cleanup(testingT, ctx)
}

func TestFuzzing_Operations_Applied_Get_NotInShadow(t *testing.T) {
	ctrl := gomock.NewController(t)

	testingT := fuzzing.NewMockTestingT(ctrl)
	testingT.EXPECT().Errorf(gomock.Any(), gomock.Any())

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Get(gomock.Any()).Return(0, nil)
	stock.EXPECT().Close().Return(nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)

	ctx.values[0] = 10 // put in shadow, will be missing in the stock
	(&opGet{0}).Apply(testingT, ctx)

	campaign.Cleanup(testingT, ctx)
}

func TestFuzzing_Operations_Applied_GetIds_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)

	testingT := fuzzing.NewMockTestingT(ctrl)
	testingT.EXPECT().Errorf(gomock.Any(), gomock.Any())

	indexSet := NewMockIndexSet[int](ctrl)
	indexSet.EXPECT().Contains(gomock.Any()).Return(false) // no value will exist

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(nil)
	stock.EXPECT().GetIds().Return(indexSet, nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)

	ctx.ids[0] = true // put in shadow, will be missing in the stock
	(&opGetIds{}).Apply(testingT, ctx)

	campaign.Cleanup(testingT, ctx)
}

func TestFuzzing_Operations_Applied_NewId_AlreadyInShadow(t *testing.T) {
	ctrl := gomock.NewController(t)

	testingT := fuzzing.NewMockTestingT(ctrl)
	testingT.EXPECT().Errorf(gomock.Any(), gomock.Any())

	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(nil)
	stock.EXPECT().New().Return(0, nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, f}
	ctx := campaign.CreateContext(t)

	ctx.ids[0] = true // returned id already in the shadow
	(&opNewId{}).Apply(testingT, ctx)

	campaign.Cleanup(testingT, ctx)
}

func TestFuzzing_Can_Be_Started(t *testing.T) {
	ctrl := gomock.NewController(t)
	stock := NewMockStock[int, int](ctrl)
	stock.EXPECT().Close().Return(nil)

	f := func(directory string) (Stock[int, int], error) {
		return stock, nil
	}

	campaign := stockFuzzCampaign{true, nil}
	testingF := fuzzing.NewMockTestingF(ctrl)
	testingF.EXPECT().Add(gomock.Any()).Times(len(campaign.Init()))
	testingF.EXPECT().Fuzz(gomock.Any()).Do(func(ff func(*testing.T, []byte)) {
		ff(t, []byte{})
	})
	FuzzStockRandomOps(testingF, f, true)
}

func TestFuzzing_Deserialize_Expensive_ops_Skipped(t *testing.T) {
	data := []fuzzing.Operation[stockFuzzContext]{
		&opNewId{}, &opGetIds{}, &opGetIds{}, &opGetIds{}, &opGetIds{}, &opGet{0}}

	for i := 0; i < 30; i++ {
		data = append(data, &opGetIds{})
		data = append(data, &opGet{0})
	}

	var stream []byte
	for _, item := range data {
		stream = append(stream, item.Serialize()...)
	}

	// three expensive ops in row will be skipped, and all expensive after 20 occurances
	want := []fuzzing.Operation[stockFuzzContext]{
		&opNewId{}, &opGetIds{}, &opGetIds{}, &opGetIds{}, &opGet{0}}

	numExpensive := 4 // already four
	for i := 0; i < 30; i++ {
		if numExpensive < 20 {
			want = append(want, &opGetIds{})
		}
		want = append(want, &opGet{0})
		numExpensive++
	}

	got := parseOperations(stream)
	assertOperationsEquals(t, got, want)
}

func TestFuzzing_Malformed_ops(t *testing.T) {
	var empty []fuzzing.Operation[stockFuzzContext]

	get := []byte{byte(get), 0x2} // get with only one data payload
	assertOperationsEquals(t, parseOperations(get), empty)

	set := []byte{byte(set), 0x2, 0x3} // get with only two bytes data payload
	assertOperationsEquals(t, parseOperations(set), empty)
}

func assertOperationsEquals(t *testing.T, got, want []fuzzing.Operation[stockFuzzContext]) {
	t.Helper()

	if got, want := len(got), len(want); got != want {
		t.Fatalf("number of got and wanted operations do not match: %d != %d", got, want)
	}

	for i := 0; i < len(got); i++ {
		gotOp, gotPayload := getOpAndData(got[i])
		wantOp, wantPayload := getOpAndData(want[i])

		if gotOp != wantOp || wantPayload != gotPayload {
			t.Errorf("original and de/serialosed types do not match: %d != %d", got[i], want[i])
		}
	}
}

func getOpAndData(op fuzzing.Operation[stockFuzzContext]) (opType, int) {
	b := op.Serialize()
	opType := opType(b[0])
	var payload int
	if opType == get || opType == set || opType == deleteId {
		payload = int(binary.BigEndian.Uint16(b[0:2]))
	}
	if opType == set {
		payload = int(binary.BigEndian.Uint32(b[0:4]))
	}
	return opType, payload
}
