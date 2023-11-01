package fuzzing

import (
	"testing"
)

//go:generate mockgen -source fuzzing.go -destination fuzzing_mocks.go -package fuzzing

// Operation represents one step applied to the system under a fuzzing campaign.
// Each operation must be serialisable to a byte array, which allows for
// carrying the operation code and data. This serialised form is used
// for passing the operation into the fuzzer.
type Operation[T any] interface {
	// Apply is executed by the fuzzer for each operation to
	// perform a required action to the system under test.
	// The context passed to this method carries the system under test
	// and its state. It is state-full to move from one step to another.
	Apply(t *testing.T, context *T)

	// Serialize converts this operation to a byte array to be passed to the fuzzer.
	// The output format is not defined, but it must be readable by deserialiser
	// in Campaign.Deserialize().
	// Typically, it contains a code of this operation in the first byte, followed
	// by payload in next bytes.
	Serialize() []byte
}

// OperationSequence is a chain of operations.
type OperationSequence[T any] []Operation[T]

// Campaign maintains one fuzzing campaign. It contains methods
// to initialise and finalise data of the campaign.
// It is passed to the fuzzer as a factory to create Operations to seed the fuzzer,
// to create the context passed through each step of the fuzzing campaign,
// and finally allows for cleaning-up at the end of the campaign.
type Campaign[T any] interface {
	// Init is used for the initialization of the campaign before it starts.
	// In particular, a set of operation sequences is returned
	// to be used for seeding the fuzzer.
	// One OperationSequence is one seed for the fuzzer. Many seeds may be created,
	// i.e. this method returns an array.
	// This method is called once before the campaign starts.
	Init() []OperationSequence[T]

	// CreateContext creates a state full object that holds the system under fuzzing
	// plus state information that is needed to be hold between each steps of the campaign.
	// This context is passed to each Operation.Apply() function during the campaign.
	// This method is called once per campaign loop iteration. The campaign loop
	// is one execution exercising the system under test with Operations derived from the seed
	// created by Init().
	CreateContext(t *testing.T) *T

	// Deserialize interprets a byte array generated by the fuzzer out of the initial seed.
	// This byte array represents a list of operations that are decoded and returned.
	// The fuzzer loops over these operations
	// and applies changes defined in them by calling Operation.Apply().
	// This method is called once for each campaign loop to convert binary representation of Operations.
	Deserialize([]byte) []Operation[T]

	// Cleanup gets the context passed through this campaign and allows for closing and cleaning it.
	// This method is called once for each campaign loop.
	Cleanup(t *testing.T, context *T)
}

// TestingF is an interface covering some of the methods of testing.F struct.
// It is provided for easy mocking.
type TestingF interface {
	Add(args ...any)
	Fuzz(ff any)
}

// Fuzz performs a fuzzing campaign.
// The campaign is initialised by calling Campaign.Init to generate chains of operations.
// These operations are the seed of the campaign. which is then executed repeatedly and concurrently.
// Each execution runs a set of operations derived by the fuzzer and converted to operations by Campaign.Deserialize.
// The operations manipulate the system under test and update its state using the context
// produced by Campaign.CreateContext. Each execution is finalised by calling Campaign.Cleanup().
func Fuzz[T any](f TestingF, c Campaign[T]) {

	// convert operations to seed.
	for _, opSet := range c.Init() {
		var raw []byte
		for _, op := range opSet {
			raw = append(raw, op.Serialize()...)
		}
		f.Add(raw)
	}

	f.Fuzz(func(t *testing.T, rawData []byte) {
		fuzz(t, c, rawData)
	})
}

func fuzz[T any](t *testing.T, c Campaign[T], rawData []byte) {
	ctx := c.CreateContext(t)
	for _, op := range c.Deserialize(rawData) {
		op.Apply(t, ctx)
	}
	c.Cleanup(t, ctx)
}

type Serializable interface {
	Serialize() []byte
}

// FuzzOp is a default fuzzing Operation, which defines a callback
// method to implement an action for the operation.
// Furthermore, it defines initial data of this operation.
// This seed data is returned by the Serialise method, while the Apply method
// routes to the defined callback method.
type FuzzOp[T ~byte, C any, PAYLOAD Serializable] struct {
	opType T
	data   PAYLOAD
	apply  func(opType T, data PAYLOAD, t *testing.T, context *C)
}

// NewOp creates a new fuzzing operation with predefined action amd initial data.
// It gets identifier of this operation, initial data, fuzzing context, and the callback method
// executed every time this operation is executed.
func NewOp[T ~byte, C any, PAYLOAD Serializable](
	opType T,
	data PAYLOAD,
	apply func(opType T, data PAYLOAD, t *testing.T, context *C)) *FuzzOp[T, C, PAYLOAD] {

	return &FuzzOp[T, C, PAYLOAD]{
		opType: opType,
		data:   data,
		apply:  apply,
	}
}

// Serialize converts this operation to a byte array.
// It contains identifier of this operation in the first byte, followed
// by payload in consecutive bytes.
func (op *FuzzOp[T, C, PAYLOAD]) Serialize() []byte {
	return append([]byte{byte(op.opType)}, op.data.Serialize()...)
}

// Apply passes the call to the apply callback method.
func (op *FuzzOp[T, C, PAYLOAD]) Apply(t *testing.T, c *C) {
	op.apply(op.opType, op.data, t, c)
}

// EmptyPayload is a convenient implementation of empty payload
type EmptyPayload struct{}

func (p EmptyPayload) Serialize() []byte {
	return []byte{}
}

// SerialisedPayload that is a type of payload that is directly represented as a byte array,
// together with the original value.
type SerialisedPayload[T any] struct {
	val        T
	serialised []byte
}

// NewSerialisedPayload creates a SerialisedPayload that is a type of payload that is directly represented as a byte array,
// together with the original value.
func NewSerialisedPayload[T any](payload T, serialised []byte) SerialisedPayload[T] {
	return SerialisedPayload[T]{
		val:        payload,
		serialised: serialised,
	}
}

func (p SerialisedPayload[T]) Serialize() []byte {
	return p.serialised
}

func (p SerialisedPayload[T]) Value() T {
	return p.val
}

type opRegistration[C any] struct {
	// opFactory creates a new operation filling-in the input payload.
	// The payload is ignored for operations with no data.
	opFactory func(payload any) Operation[C]

	// deserialize converts input bytes into payload to be used in the operation.
	// The method should consume reguired number of bytes from the input and return
	// the rest as output.
	deserialize func([]byte) (any, []byte)
}

// OpsFactoryRegistry is a convenient implementation allowing the client to register factories
// to create fuzzing operations. The instances of operations can be obtained from this registry then.
type OpsFactoryRegistry[T ~byte, C any] map[T]opRegistration[C]

func NewRegistry[T ~byte, C any]() OpsFactoryRegistry[T, C] {
	return make(map[T]opRegistration[C])
}

func (r OpsFactoryRegistry[T, C]) register(opType T, reg opRegistration[C]) {
	r[opType] = reg
}

// CreateDataOp creates a new operation for the input type and the input data.
// It is expected that the operation was previously registered by calling RegisterDataOp.
func (r OpsFactoryRegistry[T, C]) CreateDataOp(opType T, data any) Operation[C] {
	return r[opType].opFactory(data)
}

// CreateNoDataOp creates a new operation with no payload.
// It is expected that the operation was previously registered by calling RegisterNoDataOp.
func (r OpsFactoryRegistry[T, C]) CreateNoDataOp(opType T) Operation[C] {
	return r[opType].opFactory(nil)
}

// ReadNextOp parses the next operation from the input stream.
// It expects opcode of the operation at the first byte followed by payload at next bytes.
// This method consumes the opcode and the payload from the input array and returns remaining part of the array
// at its output.
// Note: this method expects that operations were registered under opcodes using consecutive integers starting from zero
// (i.e. 0, 1, 2, ...). If this does not hold, the method will not parse the input correctly.
func (r OpsFactoryRegistry[T, C]) ReadNextOp(raw []byte) (T, Operation[C], []byte) {
	var op Operation[C]
	numOps := len(r)
	opType := T(raw[0] % byte(numOps))
	raw = raw[1:]
	rec := r[opType]
	if rec.deserialize == nil {
		op = rec.opFactory(nil)
	} else {
		var payload any
		payload, raw = rec.deserialize(raw)
		op = rec.opFactory(payload)
	}

	return opType, op, raw
}

// RegisterNoDataOp registers a factory to create a fuzzing operation based for an operation op-code.
// This method registers an operation that has only its opcode, but no payload.
func RegisterNoDataOp[T ~byte, C any](registry OpsFactoryRegistry[T, C], opType T, apply func(opType T, t *testing.T, context *C)) {
	reg := opRegistration[C]{
		func(payload any) Operation[C] {
			adapted := func(opType T, _ EmptyPayload, t *testing.T, context *C) {
				apply(opType, t, context)
			}
			return NewOp(opType, EmptyPayload{}, adapted)
		},
		nil,
	}
	registry.register(opType, reg)
}

// RegisterDataOp registers a factory to create a fuzzing operation for an operation op-code.
// This method registers an operation that has its opcode together with a payload.
// It furthermore registers two factories to serialize and deserialize payload of this operation to/from a byte array.
func RegisterDataOp[T ~byte, C any, D any](
	registry OpsFactoryRegistry[T, C],
	opType T,
	serialise func(data D) []byte,
	deserialize func([]byte) (D, []byte),
	applyOp func(opType T, data D, t *testing.T, context *C)) {

	reg := opRegistration[C]{
		func(payload any) Operation[C] {
			adapted := func(opType T, data SerialisedPayload[D], t *testing.T, context *C) {
				applyOp(opType, data.Value(), t, context)
			}
			return NewOp(opType, NewSerialisedPayload[D](payload.(D), serialise(payload.(D))), adapted)
		},
		func(b []byte) (any, []byte) {
			return deserialize(b)
		},
	}
	registry.register(opType, reg)
}
