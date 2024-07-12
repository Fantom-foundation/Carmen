package carmen

import (
	"fmt"
	"strings"
	"testing"
)

func TestMemoryFootprint(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("cannot open test database: %v", err)
	}
	fp := db.GetMemoryFootprint()

	if fp.Total() <= 0 {
		t.Error("memory footprint returned 0 for existing open database")
	}

	s := fmt.Sprintf("%s", fp)

	if !strings.Contains(s, "live") {
		t.Error("memory-footprint breakdown does not contain 'live' keyword even though database contains LiveDB")
	}

	if !strings.Contains(s, "archive") {
		t.Error("memory-footprint breakdown does not contain 'archive' keyword even though database contains Archive")
	}
}
