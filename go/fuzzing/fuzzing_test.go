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

	noDataF := func(opType byte, t TestingT, c *testContext) {
		*c = append(*c, opType)
	}

	serialise := func(data byte) []byte {
		return []byte{data}
	}
	deserialise := func(raw *[]byte) byte {
		r := (*raw)[0]
		*raw = (*raw)[1:]
		return r
	}

	dataF := func(opType byte, data byte, t TestingT, c *testContext) {
		*c = append(*c, opType)
		*c = append(*c, data)
	}

	registry := NewRegistry[byte, testContext]()
	RegisterDataOp(registry, 0x0, serialise, deserialise, dataF)
	RegisterNoDataOp(registry, 0x1, noDataF)
	RegisterNoDataOp(registry, 0x2, noDataF)

	RegisterNoDataOp(registry, 0x3, noDataF)
	RegisterNoDataOp(registry, 0x4, noDataF)
	RegisterNoDataOp(registry, 0x5, noDataF)

	op1 := registry.CreateDataOp(0x0, byte(0xFF))
	op2 := registry.CreateNoDataOp(0x1)
	op3 := registry.CreateNoDataOp(0x2)

	op4 := registry.CreateNoDataOp(0x3)
	op5 := registry.CreateNoDataOp(0x4)
	op6 := registry.CreateNoDataOp(0x5)

	chain1 := []Operation[testContext]{op1, op2, op3}
	chain2 := []Operation[testContext]{op4, op5}
	chain3 := []Operation[testContext]{op6}
	chains := []OperationSequence[testContext]{chain1, chain2, chain3}

	terminalSymbol := byte(0xFA)

	// ini complete test campaign
	campaign.EXPECT().Init().Return(chains)
	// init every loop of the campaign
	context := testContext(make([]byte, 0, 6))
	campaign.EXPECT().CreateContext(t).Times(2).Return(&context) // two campaign loops
	campaign.EXPECT().Deserialize(gomock.Any()).Times(2).DoAndReturn(func(raw []byte) []Operation[testContext] {
		ops := make([]Operation[testContext], 0, len(raw))
		for len(raw) > 0 {
			_, op := registry.ReadNextOp(&raw)
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
		0x0, 0xFF, 0x1, 0x2, 0x3, 0x4, 0x5, 0xFA, // first loop, includes data for opcode 0xA
		0x0, 0xFF, 0x1, 0x2, 0x3, 0x4, 0x5, 0xFB, // second loop - different terminal symbol
	}
	got := context

	if !slices.Equal(got, want) {
		t.Errorf("Executed chain of operations not valied: \n got: %v\n want: %v", got, want)
	}
}

type testOpType byte
type testContext []byte
type testData byte

const (
	set testOpType = iota
	get
	print
)

func TestFuzz_CanParseRegisteredOps(t *testing.T) {
	registry := NewRegistry[testOpType, testContext]()

	serialise := func(data testData) []byte {
		return []byte{byte(data)}
	}
	deserialise := func(raw *[]byte) testData {
		r := testData((*raw)[0])
		*raw = (*raw)[1:]
		return r
	}

	opSet := func(opType testOpType, data testData, _ TestingT, context *testContext) {
		*context = append(*context, byte(opType))
		*context = append(*context, byte(data))
	}
	opGet := func(opType testOpType, data testData, _ TestingT, context *testContext) {
		*context = append(*context, byte(opType))
		*context = append(*context, byte(data))
	}
	opPrint := func(opType testOpType, _ TestingT, context *testContext) {
		*context = append(*context, byte(opType))
	}

	RegisterDataOp(registry, set, serialise, deserialise, opSet)
	RegisterDataOp(registry, get, serialise, deserialise, opGet)
	RegisterNoDataOp(registry, print, opPrint)

	// create an expected chain of operations
	var expected []Operation[testContext]
	expected = append(expected, registry.CreateDataOp(set, testData(10)))
	expected = append(expected, registry.CreateDataOp(get, testData(10)))
	expected = append(expected, registry.CreateDataOp(set, testData(20)))
	expected = append(expected, registry.CreateDataOp(set, testData(30)))
	expected = append(expected, registry.CreateDataOp(get, testData(20)))
	expected = append(expected, registry.CreateNoDataOp(print))
	expected = append(expected, registry.CreateDataOp(get, testData(20)))

	// serialise
	var raw []byte
	for _, op := range expected {
		raw = append(raw, op.Serialize()...)
	}

	got := &testContext{}
	// deserialize and execute all ops
	for _, op := range registry.ReadAllOps(raw) {
		op.Apply(nil, got)
	}

	want := []byte{byte(set), 10, byte(get), 10, byte(set), 20, byte(set), 30, byte(get), 20, byte(print), byte(get), 20}

	if !slices.Equal(*got, want) {
		t.Errorf("exectued operations do not match: got: %v != want: %v", *got, want)
	}
}
