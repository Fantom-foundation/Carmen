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

	f := func(opType byte, data EmptyPayload, t *testing.T, c *testContext) {
		*c = append(*c, opType)
	}

	opA := NewOp(byte(0xA), EmptyPayload{}, f)
	opB := NewOp(byte(0xB), EmptyPayload{}, f)
	opC := NewOp(byte(0xC), EmptyPayload{}, f)

	op1 := NewOp(byte(0x1), EmptyPayload{}, f)
	op2 := NewOp(byte(0x2), EmptyPayload{}, f)
	op3 := NewOp(byte(0x3), EmptyPayload{}, f)

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
			ops = append(ops, NewOp(raw[i], EmptyPayload{}, f))
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
		0xA, 0xB, 0xC, 0x1, 0x2, 0x3, 0xFA, // first loop
		0xA, 0xB, 0xC, 0x1, 0x2, 0x3, 0xFB, // second loop - different terminal symbol
	}
	got := context

	if !slices.Equal(got, want) {
		t.Errorf("Executed chain of operations not valied: \n got: %v\n want: %v", got, want)
	}
}

type testContext []byte
