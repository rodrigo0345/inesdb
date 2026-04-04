package buffermanager

import (
	"testing"
)

// TestNewLRUReplacer tests LRU replacer creation
func TestNewLRUReplacer(t *testing.T) {
	replacer := NewLRUReplacer(10)

	if replacer == nil {
		t.Fatal("expected replacer to be non-nil")
	}
	// Size is 0 initially since no frames are unpinned yet
	if replacer.Size() != 0 {
		t.Fatalf("expected size 0 initially (no unpinned frames), got %d", replacer.Size())
	}
}

// TestReplacer_Size tests size getter
func TestReplacer_Size(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Initially empty
	if replacer.Size() != 0 {
		t.Fatalf("expected size 0 initially, got %d", replacer.Size())
	}

	// Unpin some frames
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	if replacer.Size() != 3 {
		t.Fatalf("expected size 3 after unpinning 3 frames, got %d", replacer.Size())
	}
}

// TestReplacer_Victim tests victim selection
func TestReplacer_Victim(t *testing.T) {
	replacer := NewLRUReplacer(3)

	// No frames are unpinned, so no victim
	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim when no frames are unpinned")
	}

	// Unpin some frames
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	// Now we should be able to get victims
	victim1, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim to be available")
	}

	victim2, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected second victim to be available")
	}

	victim3, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected third victim to be available")
	}

	// All should have different IDs
	if victim1 == victim2 || victim2 == victim3 || victim1 == victim3 {
		t.Fatal("expected different victim frame IDs")
	}
}

// TestReplacer_VictimNoneAvailable tests victim when all frames are pinned
func TestReplacer_VictimNoneAvailable(t *testing.T) {
	replacer := NewLRUReplacer(2)

	// Pin all frames (or leave unpinned frames empty)
	replacer.Pin(0)
	replacer.Pin(1)

	// No victim should be available
	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim to be available when all frames are pinned")
	}
}

// TestReplacer_Pin tests pinning frames
func TestReplacer_Pin(t *testing.T) {
	replacer := NewLRUReplacer(3)

	// Unpin all frames
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	// Pin some frames
	replacer.Pin(0)
	replacer.Pin(1)

	// Size should be reduced
	if replacer.Size() != 1 {
		t.Fatalf("expected size 1 after pinning 2 of 3, got %d", replacer.Size())
	}
}

// TestReplacer_Unpin tests unpinning frames
func TestReplacer_Unpin(t *testing.T) {
	replacer := NewLRUReplacer(2)

	replacer.Pin(0)
	replacer.Pin(1)

	// No victim available (all pinned)
	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim when all frames are pinned")
	}

	// Unpin frame 0
	replacer.Unpin(0)

	// Now frame 0 should be a victim candidate
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim after unpinning")
	}
	if victim != 0 {
		t.Fatalf("expected victim to be frame 0, got %d", victim)
	}
}

// TestReplacer_PinUnpinSequence tests pin/unpin sequence
func TestReplacer_PinUnpinSequence(t *testing.T) {
	replacer := NewLRUReplacer(4)

	// Unpin all frames
	for i := 0; i < 4; i++ {
		replacer.Unpin(FrameID(i))
	}

	// All unpinned, so all available as victims
	initial_size := replacer.Size()
	if initial_size != 4 {
		t.Fatalf("expected size 4, got %d", initial_size)
	}

	// Pin some frames
	replacer.Pin(0)
	replacer.Pin(1)

	// Size should decrease
	if replacer.Size() != 2 {
		t.Fatalf("expected size 2 after pinning 2, got %d", replacer.Size())
	}

	// Get a victim (should be frame 2 or 3)
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim available")
	}
	if victim == 0 || victim == 1 {
		t.Fatalf("expected victim to not be a pinned frame, got %d", victim)
	}

	// Size should decrease after getting victim
	if replacer.Size() != 1 {
		t.Fatalf("expected size 1 after getting victim, got %d", replacer.Size())
	}
}

// TestReplacer_LRUOrdering tests LRU page ordering
func TestReplacer_LRUOrdering(t *testing.T) {
	replacer := NewLRUReplacer(3)

	// Unpin frames in sequence
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	// Access order: 0, 1, 2 (unpinned in this order)
	// The first victim should be frame 0 (accessed first)
	victim1, _ := replacer.Victim()
	if victim1 != 0 {
		t.Fatalf("expected first victim to be frame 0, got %d", victim1)
	}

	// Second victim should be frame 1
	victim2, _ := replacer.Victim()
	if victim2 != 1 {
		t.Fatalf("expected second victim to be frame 1, got %d", victim2)
	}

	// Third victim should be frame 2
	victim3, _ := replacer.Victim()
	if victim3 != 2 {
		t.Fatalf("expected third victim to be frame 2, got %d", victim3)
	}
}

// TestReplacer_MultiplePinUnpin tests multiple pin/unpin operations
func TestReplacer_MultiplePinUnpin(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Unpin all frames
	for i := 0; i < 5; i++ {
		replacer.Unpin(FrameID(i))
	}

	if replacer.Size() != 5 {
		t.Fatalf("expected size 5, got %d", replacer.Size())
	}

	// Pin some frames
	for i := 0; i < 3; i++ {
		replacer.Pin(FrameID(i))
	}

	if replacer.Size() != 2 {
		t.Fatalf("expected size 2 after pinning 3 of 5, got %d", replacer.Size())
	}

	// Get a victim
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim available")
	}

	// Pin the victim
	replacer.Pin(victim)

	// Size should decrease
	if replacer.Size() != 1 {
		t.Fatalf("expected size 1, got %d", replacer.Size())
	}
}

// TestReplacer_PinAfterVictim tests pinning frames that were victims
func TestReplacer_PinAfterVictim(t *testing.T) {
	replacer := NewLRUReplacer(2)

	// Unpin both frames
	replacer.Unpin(0)
	replacer.Unpin(1)

	victim1, _ := replacer.Victim()

	// Unpin the victim again
	replacer.Unpin(victim1)

	// Only the other frame should be victim
	victim2, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected second frame to be victim")
	}

	if victim1 == victim2 {
		t.Fatalf("expected different frames, got %d twice", victim1)
	}
}

// TestReplacer_Threshold tests replacer with different size operations
func TestReplacer_Threshold(t *testing.T) {
	replacer := NewLRUReplacer(10)

	// Unpin frames
	for i := 0; i < 10; i++ {
		replacer.Unpin(FrameID(i))
	}

	if replacer.Size() != 10 {
		t.Fatalf("expected size 10 after unpinning all, got %d", replacer.Size())
	}

	// Pin all frames
	for i := 0; i < 10; i++ {
		replacer.Pin(FrameID(i))
	}

	// No victim should be available
	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim when all frames are pinned")
	}

	if replacer.Size() != 0 {
		t.Fatalf("expected size 0 when all pinned, got %d", replacer.Size())
	}
}
