package stock

import "testing"

func TestComplementSet_LowerAndUpperBoundsAreRetained(t *testing.T) {
	set := MakeComplementSet[int](12, 15)
	if got, want := set.GetLowerBound(), 12; got != want {
		t.Errorf("invalid lower bound, wanted %d, got %d", want, got)
	}
	if got, want := set.GetUpperBound(), 15; got != want {
		t.Errorf("invalid upper bound, wanted %d, got %d", want, got)
	}
}

func TestComplementSet_ContainsElementInRange(t *testing.T) {
	set := MakeComplementSet[int](12, 15)
	for i := 0; i < 20; i++ {
		got := set.Contains(i)
		want := (i >= 12) && (i < 15)
		if got != want {
			t.Errorf("error in membership for element %d, wanted %t, got %t", i, want, got)
		}
	}
}

func TestComplementSet_DoesNotContainExcludedElements(t *testing.T) {
	set := MakeComplementSet[int](12, 19)
	set.Remove(10)
	set.Remove(14)
	set.Remove(16)
	set.Remove(20)
	for i := 0; i < 30; i++ {
		got := set.Contains(i)
		want := (i >= 12) && (i < 19) && (i != 14) && (i != 16)
		if got != want {
			t.Errorf("error in membership for element %d, wanted %t, got %t", i, want, got)
		}
	}
}
