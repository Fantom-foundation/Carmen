package archive

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// ------------------------------- Prototype ----------------------------------

// ---- Stand-in for an Ordered Map like implemented by LevelDB ----

type mySortedMapEntry[K comparable, V any] struct {
	key   K
	value V
}

type mySortedMap[K comparable, V any] struct {
	less func(a, b K) bool
	data []mySortedMapEntry[K, V]
}

func newMySortedMap[K comparable, V any](less func(a, b K) bool) mySortedMap[K, V] {
	return mySortedMap[K, V]{less, []mySortedMapEntry[K, V]{}}
}

func (m *mySortedMap[K, V]) Insert(key K, value V) {
	index := sort.Search(len(m.data), func(i int) bool {
		return m.less(key, m.data[i].key)
	})
	entry := mySortedMapEntry[K, V]{key, value}
	if index < len(m.data) {
		if m.data[index].key == key {
			return
		}
		m.data = append(m.data[:index+1], m.data[index:]...)
		m.data[index] = entry
	} else {
		m.data = append(m.data, entry)
	}
}

func (m *mySortedMap[K, V]) Get(key K) *V {
	pos := sort.Search(len(m.data), func(i int) bool {
		return !m.less(m.data[i].key, key)
	})
	if pos < len(m.data) && m.data[pos].key == key {
		return &m.data[pos].value
	}
	return nil
}

func (m *mySortedMap[K, V]) LowerBound(key K) mySortedMapIterator[K, V] {
	pos := sort.Search(len(m.data), func(i int) bool {
		return !m.less(m.data[i].key, key)
	})
	return mySortedMapIterator[K, V]{m.less, m.data, pos, len(m.data)}
}

func (m *mySortedMap[K, V]) VisitAll(visitor func(K, V) bool) {
	for _, cur := range m.data {
		if !visitor(cur.key, cur.value) {
			return
		}
	}
}

func (m *mySortedMap[K, V]) VisitRange(lowerBound, upperBound K, visitor func(K, V) bool) {
	pos := sort.Search(len(m.data), func(i int) bool {
		return !m.less(m.data[i].key, lowerBound)
	})
	for ; pos < len(m.data) && m.less(m.data[pos].key, upperBound) && visitor(m.data[pos].key, m.data[pos].value); pos++ {
	}
}

func (m *mySortedMap[K, V]) GetRange(lowerBound, upperBound K) mySortedMapIterator[K, V] {
	// Search lower and upper bounds of interval.
	low := sort.Search(len(m.data), func(i int) bool {
		return !m.less(m.data[i].key, lowerBound)
	})
	high := sort.Search(len(m.data), func(i int) bool {
		return !m.less(m.data[i].key, upperBound)
	})
	return mySortedMapIterator[K, V]{m.less, m.data, low, high}
}

type mySortedMapIterator[K comparable, V any] struct {
	less     func(a, b K) bool
	data     []mySortedMapEntry[K, V]
	cur, end int
}

func (i *mySortedMapIterator[K, V]) Done() bool {
	return i.cur >= i.end || i.cur < 0
}

func (i *mySortedMapIterator[K, V]) Key() K {
	return i.data[i.cur].key
}

func (i *mySortedMapIterator[K, V]) Value() V {
	return i.data[i.cur].value
}

func (i *mySortedMapIterator[K, V]) Next() bool {
	i.cur++
	return !i.Done()
}

func (i *mySortedMapIterator[K, V]) Priv() bool {
	i.cur--
	return !i.Done()
}

func (i *mySortedMapIterator[K, V]) Seek(key K) bool {
	data := i.data[i.cur:i.end]
	offset := sort.Search(len(data), func(j int) bool {
		return !i.less(data[j].key, key)
	})
	i.cur += offset
	return !i.Done()
}

func TestMySortedMap_InsertedKeysCanBeLookedUp(t *testing.T) {
	myMap := newMySortedMap[int, int](func(a, b int) bool { return a < b })
	myMap.Insert(1, 2)
	myMap.Insert(3, 1)
	myMap.Insert(2, 3)
	if ptr := myMap.Get(1); ptr == nil || *ptr != 2 {
		t.Errorf("failed to look up key 1")
	}
	if ptr := myMap.Get(2); ptr == nil || *ptr != 3 {
		t.Errorf("failed to look up key 1")
	}
	if ptr := myMap.Get(3); ptr == nil || *ptr != 1 {
		t.Errorf("failed to look up key 1")
	}
	if ptr := myMap.Get(0); ptr != nil {
		t.Errorf("lookup of 0 should fail")
	}
	if ptr := myMap.Get(4); ptr != nil {
		t.Errorf("lookup of 4 should fail")
	}
}

func TestMySortedMap_InsertCreatesOrderedContent(t *testing.T) {
	myMap := newMySortedMap[int, int](func(a, b int) bool { return a < b })
	myMap.Insert(1, 2)
	myMap.Insert(3, 1)
	myMap.Insert(2, 3)
	if !reflect.DeepEqual(myMap.data, []mySortedMapEntry[int, int]{{1, 2}, {2, 3}, {3, 1}}) {
		t.Errorf("invalid map content, got %v", myMap.data)
	}
}

func TestMySortedMap_RangeVisitCoversRange(t *testing.T) {
	myMap := newMySortedMap[int, int](func(a, b int) bool { return a < b })
	myMap.Insert(1, 2)
	myMap.Insert(3, 4)
	myMap.Insert(2, 3)
	myMap.Insert(4, 5)

	visited := []mySortedMapEntry[int, int]{}
	myMap.VisitRange(2, 4, func(k, v int) bool {
		visited = append(visited, mySortedMapEntry[int, int]{k, v})
		return true
	})

	if !reflect.DeepEqual(visited, []mySortedMapEntry[int, int]{{2, 3}, {3, 4}}) {
		t.Errorf("invalid visited range, got %v", visited)
	}
}

// ------------------------- Log Archive Prototype ----------------------------

type myLogArchive struct {
	// The main log store
	logs mySortedMap[logId, myLogValue]

	// an index for addresses
	addressIndex mySortedMap[myAddressKey, empty]

	// indexes for topics at the 5 possible positions
	topicIndex [5]mySortedMap[myTopicKey, empty]

	// the archive's hashes at various block heights
	hashes mySortedMap[uint64, common.Hash]
}

func newMyLogArchive() *myLogArchive {
	return &myLogArchive{
		logs:         newMySortedMap[logId, myLogValue](func(a, b logId) bool { return a.Less(b) }),
		addressIndex: newMySortedMap[myAddressKey, empty](func(a, b myAddressKey) bool { return a.Less(&b) }),
		topicIndex: [5]mySortedMap[myTopicKey, empty]{
			newMySortedMap[myTopicKey, empty](func(a, b myTopicKey) bool { return a.Less(&b) }),
			newMySortedMap[myTopicKey, empty](func(a, b myTopicKey) bool { return a.Less(&b) }),
			newMySortedMap[myTopicKey, empty](func(a, b myTopicKey) bool { return a.Less(&b) }),
			newMySortedMap[myTopicKey, empty](func(a, b myTopicKey) bool { return a.Less(&b) }),
			newMySortedMap[myTopicKey, empty](func(a, b myTopicKey) bool { return a.Less(&b) }),
		},
		hashes: newMySortedMap[uint64, common.Hash](func(a, b uint64) bool { return a < b }),
	}
}

type logId struct {
	// 5 bytes for the block, 3 bytes for the index in the block
	encoded uint64
}

func newLogId(block uint64, counter int) logId {
	return logId{block<<24 | uint64(counter&0xFFFFFF)}
}

func (i logId) GetBlock() uint64 {
	return i.encoded >> 24
}

func (i logId) GetIndex() int {
	return int(i.encoded & 0xFFFFFF)
}

func (i logId) Less(other logId) bool {
	return i.encoded < other.encoded
}

func (i logId) String() string {
	return fmt.Sprintf("%d-%d", i.GetBlock(), i.GetIndex())
}

type myLogValue struct {
	address common.Address
	topics  [5]Topic
	data    []byte
}

type myAddressKey struct {
	address common.Address
	log     logId
}

func (k *myAddressKey) Less(other *myAddressKey) bool {
	if r := bytes.Compare(k.address[:], other.address[:]); r < 0 {
		return true
	} else if r > 0 {
		return false
	}
	return k.log.Less(other.log)
}

type myTopicKey struct {
	topic Topic
	log   logId
}

func (k *myTopicKey) Less(other *myTopicKey) bool {
	if r := bytes.Compare(k.topic[:], other.topic[:]); r < 0 {
		return true
	} else if r > 0 {
		return false
	}
	return k.log.Less(other.log)
}

type empty struct{}

func (a *myLogArchive) Add(block uint64, logs []*Log) error {
	h := sha256.New()
	predecessorHash := a.GetHash(block)
	h.Write(predecessorHash[:])

	// Add and index the log entries.
	count := 0
	for _, log := range logs {
		// Add log to main table.
		id := newLogId(block, count)
		count++
		a.logs.Insert(id, myLogValue{log.Address, log.Topics, bytes.Clone(log.Data)})

		// Register log in address index.
		a.addressIndex.Insert(myAddressKey{log.Address, id}, empty{})

		// Register topics in indexes.
		for i := 0; i < 5; i++ {
			if log.Topics[i] != (Topic{}) { // ignore empty topics
				a.topicIndex[i].Insert(myTopicKey{log.Topics[i], id}, empty{})
			}
		}

		// Add logs to block hash.
		h.Write(log.Address[:])
		for i := range log.Topics {
			h.Write(log.Topics[i][:])
		}
		h.Write(log.Data)
	}

	// Log hash of log archive.
	var hash common.Hash
	h.Sum(hash[0:0])
	a.hashes.Insert(block, hash)

	return nil
}

func (a *myLogArchive) Get(filter *LogFilter) ([]*Log, error) {
	res := []*Log{}
	// Recursively enumerate disjunct patterns for the filter and fetch
	// all log entries for each of those.
	enumeratePatterns(filter, func(pattern logPattern) bool {
		res = append(res, a.get(&pattern)...)
		return true // < could be used to stop after a number of results
	})
	return res, nil
}

// get fetches all log entries matching the given pattern.
func (a *myLogArchive) get(pattern *logPattern) []*Log {
	lowerIdBound := newLogId(pattern.From, 0)
	upperIdBound := newLogId(pattern.To+1, 0)

	// If there is no filtering on addresses or topics, a query over the main table is most efficient.
	// The main table is sorted by block number, and can be effectively queried for it.
	res := []*Log{}
	if pattern.acceptsAllAddressesAndTopics() {
		a.logs.VisitRange(lowerIdBound, upperIdBound, func(key logId, value myLogValue) bool {
			res = append(res, toLog(key, &value))
			return true
		})
		return res
	}

	// If there are active topic filters, indexes can be used.
	// To use the indexes effectively, leap-frogging is used.
	iterators := make([]logIdIterator, 0, 6)
	if pattern.Address != nil {
		lowerBound := myAddressKey{*pattern.Address, lowerIdBound}
		upperBound := myAddressKey{*pattern.Address, upperIdBound}
		iterators = append(iterators, &addressIndexLogIdIterator{*pattern.Address, a.addressIndex.GetRange(lowerBound, upperBound)})
	}

	for i, topic := range pattern.Topics {
		if topic == nil {
			continue
		}
		lowerBound := myTopicKey{*pattern.Topics[i], lowerIdBound}
		upperBound := myTopicKey{*pattern.Topics[i], upperIdBound}
		iterators = append(iterators, &topicIndexLogIdIterator{*topic, a.topicIndex[i].GetRange(lowerBound, upperBound)})
	}

	// A utility function to find the id of the next log entry of the result, or upperIdBound, if there is none.
	findNext := func() logId {
		for {
			min := upperIdBound
			max := lowerIdBound
			for _, iter := range iterators {
				if iter.Done() {
					return upperIdBound
				}
				cur := iter.Get()
				if cur.Less(min) {
					min = cur
				}
				if max.Less(cur) {
					max = cur
				}
			}
			if min == max {
				// Move already to next position to prepare for next call.
				for _, iter := range iterators {
					iter.Next()
				}
				return min
			}
			for _, iter := range iterators {
				iter.Seek(max)
			}
		}
	}

	// Resolve all found log IDs to logs.
	for {
		if next := findNext(); next.Less(upperIdBound) {
			res = append(res, toLog(next, a.logs.Get(next)))
		} else {
			return res
		}
	}
}

// enumeratePatterns produces a list of patterns forming a partitioning of the
// provided filter. Thus, the sets of all Logs matched by the resulting patterns
// are dijunct and their union is equal to the set of log entries matched by the
// provided filter. Since the number of patterns can be exponential in the number
// of topics in the filter, patterns care consumed one-by-one, eliminating the
// risk of using excessive memory for the full list of patterns.
func enumeratePatterns(filter *LogFilter, consumer func(pattern logPattern) bool) {
	if filter.From > filter.To {
		return
	}

	addresses := []*common.Address{nil}
	if len(filter.Addresses) > 0 {
		seen := map[common.Address]bool{}
		addresses = make([]*common.Address, 0, len(filter.Addresses))
		for i := range filter.Addresses {
			if _, exists := seen[filter.Addresses[i]]; !exists {
				addresses = append(addresses, &filter.Addresses[i])
				seen[filter.Addresses[i]] = true
			}
		}
	}

	for _, addr := range addresses {
		keepGoing := enumerateTopicPatterns(filter.Topics[:], func(topics []*Topic) bool {
			return consumer(logPattern{filter.From, filter.To, addr, *(*[5]*Topic)(topics)})
		})
		if !keepGoing {
			return
		}
	}
}

// logPattern is a simplified filter that coveres a block range and for the
// address and each topic either a concrete value or a wildcard (*). Every
// LogFilter can be decomposed into a list of dijunct logPatterns matching
// the same set of log entries. Patterns are the granularity at which queries
// against the LogArchive are executed.
type logPattern struct {
	From, To uint64          // the block range
	Address  *common.Address // the address the log must have, nil = wildcard
	Topics   [5]*Topic       // a pattern of topics, nil = wildcard
}

func (f *logPattern) acceptsAllAddressesAndTopics() bool {
	if f.Address != nil {
		return false
	}
	for _, topic := range f.Topics {
		if topic != nil {
			return false
		}
	}
	return true
}

// enumerateTopicPatterns enumerates all patterns of combinations matching the given
// topic filter. Thus, the sets of all topic lists matched by the resulting patterns
// are dijunct and their union is equal to the set of topic lists matched by the
// provided filter. Since the number of patterns can be exponential in the number
// of topics in the filter, patterns care consumed one-by-one, eliminating the
// risk of using excessive memory for the full list of patterns.
func enumerateTopicPatterns(filter [][]Topic, consumer func([]*Topic) bool) bool {
	partial := make([]*Topic, 0, len(filter))
	return enumerateTopicPatternsInternal(filter, partial, consumer)
}

func enumerateTopicPatternsInternal(filter [][]Topic, partial []*Topic, consumer func([]*Topic) bool) bool {
	if len(filter) == 0 {
		return consumer(partial)
	}

	firsts := []*Topic{nil}
	if len(filter[0]) != 0 {
		seen := map[Topic]bool{}
		firsts = make([]*Topic, 0, len(filter[0]))
		for i := range filter[0] {
			if _, exists := seen[filter[0][i]]; !exists {
				firsts = append(firsts, &filter[0][i])
				seen[filter[0][i]] = true
			}
		}
	}

	for _, first := range firsts {
		partial = append(partial, first)
		if !enumerateTopicPatternsInternal(filter[1:], partial, consumer) {
			return false
		}
		partial = partial[0 : len(partial)-1]
	}
	return true
}

func (a *myLogArchive) GetHash(block uint64) common.Hash {
	iter := a.hashes.LowerBound(block)
	if iter.Done() {
		iter.Priv()
	}
	if iter.Done() {
		return common.Hash{}
	}
	if iter.Key() <= block {
		return iter.Value()
	}
	iter.Priv()
	if iter.Done() {
		return common.Hash{}
	}
	return iter.Value()
}

func (a *myLogArchive) Verify(block uint64) bool {
	panic("not implemented")
}

func TestLogCartessionProductOfTopics(t *testing.T) {
	t1 := Topic{1}
	t2 := Topic{2}
	t3 := Topic{3}
	t4 := Topic{4}

	tests := []struct {
		pattern [][]Topic
		result  []string
	}{
		{[][]Topic{}, []string{"[]"}},
		{[][]Topic{nil}, []string{"[*]"}},
		{[][]Topic{{}}, []string{"[*]"}},
		{[][]Topic{{t1}}, []string{"[t1]"}},
		{[][]Topic{nil, nil}, []string{"[*,*]"}},
		{[][]Topic{{t1, t2}}, []string{"[t1]", "[t2]"}},
		{[][]Topic{{t1}, nil, {t2}}, []string{"[t1,*,t2]"}},
		{[][]Topic{{t1}, {}, {t2}}, []string{"[t1,*,t2]"}},
		{[][]Topic{{t1}, {}, {t2, t3}}, []string{"[t1,*,t2]", "[t1,*,t3]"}},
		{[][]Topic{{t1, t2}, nil, {t3, t4}}, []string{"[t1,*,t3]", "[t1,*,t4]", "[t2,*,t3]", "[t2,*,t4]"}},
	}

	toString := func(topics []*Topic) string {
		res := "["
		for _, cur := range topics {
			if len(res) > 1 {
				res += ","
			}
			if cur == nil {
				res += "*"
			} else {
				res += fmt.Sprintf("t%d", (*cur)[0])
			}

		}
		res += "]"
		return res
	}

	getAllPatterns := func(filter [][]Topic) [][]*Topic {
		res := [][]*Topic{}
		enumerateTopicPatterns(filter, func(pattern []*Topic) bool {
			cpy := make([]*Topic, len(pattern))
			copy(cpy[:], pattern[:])
			res = append(res, cpy)
			return true
		})
		return res
	}

	for _, test := range tests {
		res := getAllPatterns(test.pattern)

		prints := []string{}
		for _, cur := range res {
			prints = append(prints, toString(cur))
		}
		if !reflect.DeepEqual(prints, test.result) {
			t.Errorf("invalid pattern expansion of %v, wanted %v, got %v", test.pattern, test.result, prints)
		}
	}
}

func (a *myLogArchive) GetAll() ([]*Log, error) {
	res := []*Log{}
	a.logs.VisitAll(func(key logId, value myLogValue) bool {
		res = append(res, toLog(key, &value))
		return true
	})
	return res, nil
}

func toLog(key logId, value *myLogValue) *Log {
	return &Log{
		Address: value.address,
		Topics:  value.topics,
		Data:    bytes.Clone(value.data),
		Block:   key.GetBlock(),
	}
}

type logIdIterator interface {
	Next() bool
	Done() bool
	Get() logId
	Seek(logId) bool
}

type addressIndexLogIdIterator struct {
	address common.Address
	iter    mySortedMapIterator[myAddressKey, empty]
}

func (i *addressIndexLogIdIterator) Next() bool {
	return i.iter.Next()
}

func (i *addressIndexLogIdIterator) Done() bool {
	return i.iter.Done()
}

func (i *addressIndexLogIdIterator) Get() logId {
	return i.iter.Key().log
}

func (i *addressIndexLogIdIterator) Seek(id logId) bool {
	return i.iter.Seek(myAddressKey{i.address, id})
}

type topicIndexLogIdIterator struct {
	topic Topic
	iter  mySortedMapIterator[myTopicKey, empty]
}

func (i *topicIndexLogIdIterator) Next() bool {
	return i.iter.Next()
}

func (i *topicIndexLogIdIterator) Done() bool {
	return i.iter.Done()
}

func (i *topicIndexLogIdIterator) Get() logId {
	return i.iter.Key().log
}

func (i *topicIndexLogIdIterator) Seek(id logId) bool {
	return i.iter.Seek(myTopicKey{i.topic, id})
}

func TestMyLogArchive_IsALogArchive(t *testing.T) {
	var _ LogArchive = &myLogArchive{}
}

var (
	addr1 = common.Address{1}
	addr2 = common.Address{2}
	addr3 = common.Address{3}
	addr4 = common.Address{4}

	topic1 = Topic{1}
	topic2 = Topic{2}
	topic3 = Topic{3}
)

func TestMyLogArchive_EmptyArchiveContainsNothing(t *testing.T) {

	archive := newMyLogArchive()

	logs, _ := archive.Get(&LogFilter{0, 10, []common.Address{}, [5][]Topic{}})
	if len(logs) != 0 {
		t.Errorf("empty archive should not contain any logs")
	}

	logs, _ = archive.Get(&LogFilter{0, 10, []common.Address{addr1, addr2}, [5][]Topic{}})
	if len(logs) != 0 {
		t.Errorf("empty archive should not contain any logs")
	}
}

func TestMyLogArchive_FilterReturnMatchingElements(t *testing.T) {
	archive := newMyLogArchive()
	archive.Add(2, []*Log{
		{Address: addr1, Topics: [5]Topic{topic1, topic2}, Data: []byte{0}},
		{Address: addr2, Topics: [5]Topic{topic2, topic3}, Data: []byte{0, 1}},
		{Address: addr3, Topics: [5]Topic{topic1, topic2, topic3}, Data: []byte{0, 1, 2}},
	})

	archive.Add(7, []*Log{
		{Address: addr1, Topics: [5]Topic{topic3, topic2, topic1}, Data: []byte{0}},
		{Address: addr2, Topics: [5]Topic{topic2}, Data: []byte{0, 1}},
	})

	tests := []struct {
		filter LogFilter
	}{
		// Filter based on block ranges.
		{LogFilter{0, 0, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 2, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 3, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 7, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 8, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{2, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{3, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{7, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{8, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{10, 10, []common.Address{}, [5][]Topic{}}},

		// Filter based on addresses.
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr1}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr2}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr3}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr4}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr1, addr2}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr4, addr2}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{addr1, addr1}, [5][]Topic{}}},

		// Filter for topics.
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{}}},
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{{topic1}}}},
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{{topic1, topic1}}}},
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{{topic1}, nil, {topic3}}}},
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{{topic1, topic2}, nil, {topic3}}}},
		{LogFilter{0, 10, []common.Address{}, [5][]Topic{{topic1, topic2}, nil, {topic3, topic1}}}},
	}

	allLogs, _ := archive.GetAll()
	for _, test := range tests {
		logs, _ := archive.Get(&test.filter)

		wantedMatches := 0
		for _, log := range allLogs {
			if test.filter.Match(log) {
				wantedMatches++
			}
		}
		if len(logs) != wantedMatches {
			t.Errorf("invalid number of results, wanted %d, got %d", wantedMatches, len(logs))
		}
		for _, log := range logs {
			if !test.filter.Match(log) {
				t.Errorf("invalid match, filter %v, log %v", test.filter, log)
			}
		}
	}
}

func TestMyLogArchive_LogArchiveHashesBasics(t *testing.T) {
	zero := common.Hash{}
	archive := newMyLogArchive()

	if archive.GetHash(0) != zero {
		t.Errorf("initial hash is not zero")
	}

	archive.Add(1, []*Log{
		{Address: addr1, Topics: [5]Topic{topic1, topic2}, Data: []byte{0}},
		{Address: addr2, Topics: [5]Topic{topic2, topic3}, Data: []byte{0, 1}},
		{Address: addr3, Topics: [5]Topic{topic1, topic2, topic3}, Data: []byte{0, 1, 2}},
	})

	hash1 := archive.GetHash(1)
	if hash1 == zero {
		t.Errorf("hash of block 0 is still 0 after insert")
	}

	archive.Add(4, []*Log{
		{Address: addr1, Topics: [5]Topic{topic3, topic2, topic1}, Data: []byte{0}},
		{Address: addr2, Topics: [5]Topic{topic2}, Data: []byte{0, 1}},
	})

	hash4 := archive.GetHash(4)
	if hash4 == zero {
		t.Errorf("hash of block 4 is still 0 after insert")
	}

	if hash1 == hash4 {
		t.Errorf("hash of block 1 and 4 must not be equal")
	}

	for i := 0; i < 10; i++ {
		want := zero
		if i >= 1 {
			want = hash1
		}
		if i >= 4 {
			want = hash4
		}

		if got := archive.GetHash(uint64(i)); want != got {
			t.Errorf("invalid hash of block %d, want %v, have %v", i, want, got)
		}
	}

}

func TestMyLogArchive_LogsForBlockZeroCanBeSet(t *testing.T) {
	zero := common.Hash{}
	archive := newMyLogArchive()

	if archive.GetHash(0) != zero {
		t.Errorf("initial hash is not zero")
	}

	archive.Add(0, []*Log{
		{Address: addr1, Topics: [5]Topic{topic1, topic2}, Data: []byte{0}},
		{Address: addr2, Topics: [5]Topic{topic2, topic3}, Data: []byte{0, 1}},
		{Address: addr3, Topics: [5]Topic{topic1, topic2, topic3}, Data: []byte{0, 1, 2}},
	})

	hash0 := archive.GetHash(0)
	if hash0 == zero {
		t.Errorf("hash of block 0 is still 0 after insert")
	}
}
