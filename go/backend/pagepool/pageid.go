package pagepool

import "fmt"

// PageId represents a Page in the page pool
// It is composed of two IDs, the id of a hash collision bucket
// and the id of items within the collision bucket.
// It is designed for the use in hash-map like structures, but can be used also
// in linear structures just setting the bucket to a constant and reflect only the overflow id
type PageId struct {
	bucket   int // bucket is a hash bucket this Page belongs to,
	overflow int // overflow is further index for pages from the same bucket. The very first Page in the bucket should have an index 0
}

func NewPageId(bucket, overflow int) PageId {
	return PageId{bucket, overflow}
}

func (id PageId) String() string {
	return fmt.Sprintf("PageId: %d.%d", id.bucket, id.overflow)
}

func (id PageId) Bucket() int {
	return id.bucket
}
func (id PageId) Overflow() int {
	return id.overflow
}

func (id PageId) IsOverFlowPage() bool {
	return id.Overflow() != 0
}

func (id PageId) Compare(other PageId) (res int) {
	res = other.bucket - id.bucket
	if res == 0 {
		res = other.overflow - id.overflow
	}
	return
}

type PageIdComparator struct{}

func (c PageIdComparator) Compare(a, b PageId) int {
	return a.Compare(b)
}
