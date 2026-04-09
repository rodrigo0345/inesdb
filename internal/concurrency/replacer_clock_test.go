package concurrency

import (
	"testing"
)

// TestNewClockReplacer tests Clock replacer initialization
func TestNewClockReplacer(t *testing.T) {
	cr := NewClockReplacer(10)
	if cr == nil {
		t.Fatal("Failed to create ClockReplacer")
	}
	if cr.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cr.Size())
	}
}

// TestClockReplacer_Pin tests pinning frames
func TestClockReplacer_Pin(t *testing.T) {
	cr := NewClockReplacer(10)

	// Unpin a frame to make it available
	cr.Unpin(FrameID(5))
	if cr.Size() != 1 {
		t.Errorf("Expected size 1 after unpin, got %d", cr.Size())
	}

	// Pin the frame
	cr.Pin(FrameID(5))
	if cr.Size() != 0 {
		t.Errorf("Expected size 0 after pin, got %d", cr.Size())
	}
}

// TestClockReplacer_Unpin tests unpinning frames
func TestClockReplacer_Unpin(t *testing.T) {
	cr := NewClockReplacer(10)

	// Unpin multiple frames
	for i := 0; i < 5; i++ {
		cr.Unpin(FrameID(i))
	}

	if cr.Size() != 5 {
		t.Errorf("Expected size 5, got %d", cr.Size())
	}
}

// TestClockReplacer_Victim tests basic victim selection
func TestClockReplacer_Victim(t *testing.T) {
	cr := NewClockReplacer(10)

	// No frames available
	_, ok := cr.Victim()
	if ok {
		t.Error("Expected no victim when all frames are pinned")
	}

	// Add frames
	for i := 0; i < 3; i++ {
		cr.Unpin(FrameID(i))
	}

	// Should be able to get victims
	victim1, ok := cr.Victim()
	if !ok {
		t.Error("Expected to get a victim")
	}
	if cr.Size() != 2 {
		t.Errorf("Expected size 2 after victim, got %d", cr.Size())
	}

	// Get another victim
	victim2, ok := cr.Victim()
	if !ok {
		t.Error("Expected to get a second victim")
	}

	// Ensure we got different victims
	if victim1 == victim2 {
		t.Error("Expected different victims")
	}
}

// TestClockReplacer_ClockSweep tests the clock-sweep algorithm behavior
func TestClockReplacer_ClockSweep(t *testing.T) {
	cr := NewClockReplacer(5)

	// Add frames 0, 1, 2 with reference bits set (true)
	cr.Unpin(FrameID(0))
	cr.Unpin(FrameID(1))
	cr.Unpin(FrameID(2))

	// The first victim should sweep through frames, clearing reference bits
	// On the second pass, it should find a victim
	victim, ok := cr.Victim()
	if !ok {
		t.Error("Expected to get a victim")
	}

	// The victim should be one of the frames we added
	if victim != 0 && victim != 1 && victim != 2 {
		t.Errorf("Expected victim to be 0, 1, or 2, got %d", victim)
	}

	if cr.Size() != 2 {
		t.Errorf("Expected size 2 after victim, got %d", cr.Size())
	}
}

// TestClockReplacer_PinUnpinSequence tests a sequence of pin/unpin operations
func TestClockReplacer_PinUnpinSequence(t *testing.T) {
	cr := NewClockReplacer(10)

	// Unpin 5 frames
	for i := 0; i < 5; i++ {
		cr.Unpin(FrameID(i))
	}
	if cr.Size() != 5 {
		t.Errorf("Expected size 5, got %d", cr.Size())
	}

	// Pin one frame
	cr.Pin(FrameID(2))
	if cr.Size() != 4 {
		t.Errorf("Expected size 4 after pin, got %d", cr.Size())
	}

	// Unpin it again
	cr.Unpin(FrameID(2))
	if cr.Size() != 5 {
		t.Errorf("Expected size 5 after unpin, got %d", cr.Size())
	}

	// Get victims until empty
	for i := 0; i < 5; i++ {
		_, ok := cr.Victim()
		if !ok {
			t.Errorf("Expected victim %d, but got none", i)
		}
	}

	// Should be empty now
	if cr.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cr.Size())
	}

	_, ok := cr.Victim()
	if ok {
		t.Error("Expected no victim when empty")
	}
}

// TestClockReplacer_MultipleUnpins tests unpinning the same frame multiple times
func TestClockReplacer_MultipleUnpins(t *testing.T) {
	cr := NewClockReplacer(10)

	// Unpin frame 0 multiple times
	cr.Unpin(FrameID(0))
	cr.Unpin(FrameID(0)) // Should set reference bit again
	cr.Unpin(FrameID(0)) // Should set reference bit again

	if cr.Size() != 1 {
		t.Errorf("Expected size 1 (same frame), got %d", cr.Size())
	}

	// Victim should be frame 0
	victim, ok := cr.Victim()
	if !ok || victim != FrameID(0) {
		t.Errorf("Expected victim 0, got %d or no victim", victim)
	}
}

// TestClockReplacer_LargeSet tests with many frames
func TestClockReplacer_LargeSet(t *testing.T) {
	cr := NewClockReplacer(100)

	// Add 100 frames
	for i := 0; i < 100; i++ {
		cr.Unpin(FrameID(i))
	}

	if cr.Size() != 100 {
		t.Errorf("Expected size 100, got %d", cr.Size())
	}

	// Evict half
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

	// Evict remaining
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

// TestClockReplacer_AlternatingPinUnpin tests alternating pin/unpin
func TestClockReplacer_AlternatingPinUnpin(t *testing.T) {
	cr := NewClockReplacer(10)

	// Add frames 0-9
	for i := 0; i < 10; i++ {
		cr.Unpin(FrameID(i))
	}

	// Pin even frames
	for i := 0; i < 10; i += 2 {
		cr.Pin(FrameID(i))
	}

	if cr.Size() != 5 {
		t.Errorf("Expected size 5 (odd frames), got %d", cr.Size())
	}

	// Victims should all be from odd frames
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
