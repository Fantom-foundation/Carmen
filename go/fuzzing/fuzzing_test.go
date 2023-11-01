package fuzzing

import (
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/slices"
	"testing"
)

func TestFuzz_TwoFuzzingLoopOneCampaignSeedOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	campaign := NewMockCampaign[testContext](ctrl)
	testingF := NewMockTestingF(ctrl)

	noDataF := func(opType byte, t *testing.T, c *testContext) {
		*c = append(*c, opType)
	}

	serialise := func(data byte) []byte {
		return []byte{data}
	}

	dataF := func(opType byte, data byte, t *testing.T, c *testContext) {
		*c = append(*c, opType)
		*c = append(*c, data)
	}

	registry := NewRegistry[byte, testContext]()
	RegisterDataOp(registry, 0xA, serialise, dataF)
	RegisterEmptyDataOp(registry, 0xB, noDataF)
	RegisterEmptyDataOp(registry, 0xC, noDataF)

	RegisterEmptyDataOp(registry, 0x1, noDataF)
	RegisterEmptyDataOp(registry, 0x2, noDataF)
	RegisterEmptyDataOp(registry, 0x3, noDataF)

	opA := registry.CreateDataOp(0xA, byte(0xFF))
	opB := registry.CreateEmptyDataOp(0xB)
	opC := registry.CreateEmptyDataOp(0xC)

	op1 := registry.CreateEmptyDataOp(0x1)
	op2 := registry.CreateEmptyDataOp(0x2)
	op3 := registry.CreateEmptyDataOp(0x3)

	chain1 := []Operation[testContext]{opA, opB, opC}
	chain2 := []Operation[testContext]{op1, op2}
	chain3 := []Operation[testContext]{op3}
	chains := []OperationSequence[testContext]{chain1, chain2, chain3}

	terminalSymbol := byte(0xFA)

	// ini complete test campaign
	campaign.EXPECT().Init().Return(chains)
	// init every loop of the campaign
	context := testContext(make([]byte, 0, 6))
	campaign.EXPECT().CreateContext(t).Times(2).Return(&context) // two campaign loops
	campaign.EXPECT().Deserialize(gomock.Any()).Times(2).DoAndReturn(func(raw []byte) []Operation[testContext] {
		ops := make([]Operation[testContext], 0, len(raw))
		for i := 0; i < len(raw); i++ {
			var op Operation[testContext]
			if raw[i] == 0xA {
				op = registry.CreateDataOp(raw[i], raw[i+1])
				i++ // skip beyond the data part
			} else {
				op = registry.CreateEmptyDataOp(raw[i])
			}
			ops = append(ops, op)
		}
		return ops
	})
	campaign.EXPECT().Cleanup(t, gomock.Any()).Times(2).Do(func(t *testing.T, ctx *testContext) {
		*ctx = append(*ctx, terminalSymbol)
		terminalSymbol++
	})

	// initialisation of three chains expected, one fuzz campaign executed in total for all seed values.
	chainRawData := make([]byte, 0, 6)
	testingF.EXPECT().Add(gomock.Any()).Times(3).Do(func(rawData []byte) {
		chainRawData = append(chainRawData, rawData...)
	})
	// run fuzzing in two loops with the same seeds (no extra generated values)
	testingF.EXPECT().Fuzz(gomock.Any()).Times(1).Do(func(ff func(*testing.T, []byte)) {
		ff(t, chainRawData)
		ff(t, chainRawData)
	})

	// Run fuzzing
	Fuzz[testContext](testingF, campaign)

	// we test that all operations were called, and extended with closing symbol.
	want := []byte{
		0xA, 0xFF, 0xB, 0xC, 0x1, 0x2, 0x3, 0xFA, // first loop, includes data for opcode 0xA
		0xA, 0xFF, 0xB, 0xC, 0x1, 0x2, 0x3, 0xFB, // second loop - different terminal symbol
	}
	got := context

	if !slices.Equal(got, want) {
		t.Errorf("Executed chain of operations not valied: \n got: %v\n want: %v", got, want)
	}
}

type testContext []byte
