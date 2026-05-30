package concurrency

import (
	"testing"
)

func TestNewClockReplacer(t *testing.T) {
	cr := NewClockReplacer(10)
	if cr == nil {
		t.Fatal("Failed to create ClockReplacer")
	}
	if cr.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cr.Size())
	}
}

func TestClockReplacer_Pin(t *testing.T) {
	cr := NewClockReplacer(10)

	cr.Unpin(FrameID(5))
	if cr.Size() != 1 {
		t.Errorf("Expected size 1 after unpin, got %d", cr.Size())
	}

	cr.Pin(FrameID(5))
	if cr.Size() != 0 {
		t.Errorf("Expected size 0 after pin, got %d", cr.Size())
	}
}

func TestClockReplacer_Unpin(t *testing.T) {
	cr := NewClockReplacer(10)

	for i := 0; i < 5; i++ {
		cr.Unpin(FrameID(i))
	}

	if cr.Size() != 5 {
		t.Errorf("Expected size 5, got %d", cr.Size())
	}
}

func TestClockReplacer_Victim(t *testing.T) {
	cr := NewClockReplacer(10)

	_, ok := cr.Victim()
	if ok {
		t.Error("Expected no victim when all frames are pinned")
	}

	for i := 0; i < 3; i++ {
		cr.Unpin(FrameID(i))
	}

	victim1, ok := cr.Victim()
	if !ok {
		t.Error("Expected to get a victim")
	}
	if cr.Size() != 2 {
		t.Errorf("Expected size 2 after victim, got %d", cr.Size())
	}

	victim2, ok := cr.Victim()
	if !ok {
		t.Error("Expected to get a second victim")
	}

	if victim1 == victim2 {
		t.Error("Expected different victims")
	}
}

func TestClockReplacer_ClockSweep(t *testing.T) {
	cr := NewClockReplacer(5)

	cr.Unpin(FrameID(0))
	cr.Unpin(FrameID(1))
	cr.Unpin(FrameID(2))

	victim, ok := cr.Victim()
	if !ok {
		t.Error("Expected to get a victim")
	}

	if victim != 0 && victim != 1 && victim != 2 {
		t.Errorf("Expected victim to be 0, 1, or 2, got %d", victim)
	}

	if cr.Size() != 2 {
		t.Errorf("Expected size 2 after victim, got %d", cr.Size())
	}
}

func TestClockReplacer_PinUnpinSequence(t *testing.T) {
	cr := NewClockReplacer(10)

	for i := 0; i < 5; i++ {
		cr.Unpin(FrameID(i))
	}
	if cr.Size() != 5 {
		t.Errorf("Expected size 5, got %d", cr.Size())
	}

	cr.Pin(FrameID(2))
	if cr.Size() != 4 {
		t.Errorf("Expected size 4 after pin, got %d", cr.Size())
	}

	cr.Unpin(FrameID(2))
	if cr.Size() != 5 {
		t.Errorf("Expected size 5 after unpin, got %d", cr.Size())
	}

	for i := 0; i < 5; i++ {
		_, ok := cr.Victim()
		if !ok {
			t.Errorf("Expected victim %d, but got none", i)
		}
	}

	if cr.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cr.Size())
	}

	_, ok := cr.Victim()
	if ok {
		t.Error("Expected no victim when empty")
	}
}

func TestClockReplacer_MultipleUnpins(t *testing.T) {
	cr := NewClockReplacer(10)

	cr.Unpin(FrameID(0))
	cr.Unpin(FrameID(0))
	cr.Unpin(FrameID(0))

	if cr.Size() != 1 {
		t.Errorf("Expected size 1 (same frame), got %d", cr.Size())
	}

	victim, ok := cr.Victim()
	if !ok || victim != FrameID(0) {
		t.Errorf("Expected victim 0, got %d or no victim", victim)
	}
}

func TestClockReplacer_LargeSet(t *testing.T) {
	cr := NewClockReplacer(100)

	for i := 0; i < 100; i++ {
		cr.Unpin(FrameID(i))
	}

	if cr.Size() != 100 {
		t.Errorf("Expected size 100, got %d", cr.Size())
	}

	evicted := make(map[FrameID]bool)
	for i := 0; i < 50; i++ {
		victim, ok := cr.Victim()
		if !ok {
			t.Errorf("Expected victim %d", i)
		}
		evicted[victim] = true
	}

	if cr.Size() != 50 {
		t.Errorf("Expected size 50, got %d", cr.Size())
	}

	for i := 0; i < 50; i++ {
		_, ok := cr.Victim()
		if !ok {
			t.Errorf("Expected victim for remaining frames")
		}
	}

	if cr.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cr.Size())
	}
}

func TestClockReplacer_AlternatingPinUnpin(t *testing.T) {
	cr := NewClockReplacer(10)

	for i := 0; i < 10; i++ {
		cr.Unpin(FrameID(i))
	}

	for i := 0; i < 10; i += 2 {
		cr.Pin(FrameID(i))
	}

	if cr.Size() != 5 {
		t.Errorf("Expected size 5 (odd frames), got %d", cr.Size())
	}

	for i := 0; i < 5; i++ {
		victim, ok := cr.Victim()
		if !ok {
			t.Errorf("Expected victim %d", i)
		}
		if victim%2 == 0 {
			t.Errorf("Expected odd frame, got %d", victim)
		}
	}
}
