package concurrency

import (
	"testing"
)

func TestNewLRUReplacer(t *testing.T) {
	replacer := NewLRUReplacer(10)

	if replacer == nil {
		t.Fatal("expected replacer to be non-nil")
	}
	if replacer.Size() != 0 {
		t.Fatalf("expected size 0 initially (no unpinned frames), got %d", replacer.Size())
	}
}

func TestReplacer_Size(t *testing.T) {
	replacer := NewLRUReplacer(5)

	if replacer.Size() != 0 {
		t.Fatalf("expected size 0 initially, got %d", replacer.Size())
	}

	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	if replacer.Size() != 3 {
		t.Fatalf("expected size 3 after unpinning 3 frames, got %d", replacer.Size())
	}
}

func TestReplacer_Victim(t *testing.T) {
	replacer := NewLRUReplacer(3)

	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim when no frames are unpinned")
	}

	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

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

	if victim1 == victim2 || victim2 == victim3 || victim1 == victim3 {
		t.Fatal("expected different victim frame IDs")
	}
}

func TestReplacer_VictimNoneAvailable(t *testing.T) {
	replacer := NewLRUReplacer(2)

	replacer.Pin(0)
	replacer.Pin(1)

	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim to be available when all frames are pinned")
	}
}

func TestReplacer_Pin(t *testing.T) {
	replacer := NewLRUReplacer(3)

	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	replacer.Pin(0)
	replacer.Pin(1)

	if replacer.Size() != 1 {
		t.Fatalf("expected size 1 after pinning 2 of 3, got %d", replacer.Size())
	}
}

func TestReplacer_Unpin(t *testing.T) {
	replacer := NewLRUReplacer(2)

	replacer.Pin(0)
	replacer.Pin(1)

	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim when all frames are pinned")
	}

	replacer.Unpin(0)

	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim after unpinning")
	}
	if victim != 0 {
		t.Fatalf("expected victim to be frame 0, got %d", victim)
	}
}

func TestReplacer_PinUnpinSequence(t *testing.T) {
	replacer := NewLRUReplacer(4)

	for i := 0; i < 4; i++ {
		replacer.Unpin(FrameID(i))
	}

	initial_size := replacer.Size()
	if initial_size != 4 {
		t.Fatalf("expected size 4, got %d", initial_size)
	}

	replacer.Pin(0)
	replacer.Pin(1)

	if replacer.Size() != 2 {
		t.Fatalf("expected size 2 after pinning 2, got %d", replacer.Size())
	}

	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim available")
	}
	if victim == 0 || victim == 1 {
		t.Fatalf("expected victim to not be a pinned frame, got %d", victim)
	}

	if replacer.Size() != 1 {
		t.Fatalf("expected size 1 after getting victim, got %d", replacer.Size())
	}
}

func TestReplacer_LRUOrdering(t *testing.T) {
	replacer := NewLRUReplacer(3)

	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	victim1, _ := replacer.Victim()
	if victim1 != 0 {
		t.Fatalf("expected first victim to be frame 0, got %d", victim1)
	}

	victim2, _ := replacer.Victim()
	if victim2 != 1 {
		t.Fatalf("expected second victim to be frame 1, got %d", victim2)
	}

	victim3, _ := replacer.Victim()
	if victim3 != 2 {
		t.Fatalf("expected third victim to be frame 2, got %d", victim3)
	}
}

func TestReplacer_MultiplePinUnpin(t *testing.T) {
	replacer := NewLRUReplacer(5)

	for i := 0; i < 5; i++ {
		replacer.Unpin(FrameID(i))
	}

	if replacer.Size() != 5 {
		t.Fatalf("expected size 5, got %d", replacer.Size())
	}

	for i := 0; i < 3; i++ {
		replacer.Pin(FrameID(i))
	}

	if replacer.Size() != 2 {
		t.Fatalf("expected size 2 after pinning 3 of 5, got %d", replacer.Size())
	}

	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected victim available")
	}

	replacer.Pin(victim)

	if replacer.Size() != 1 {
		t.Fatalf("expected size 1, got %d", replacer.Size())
	}
}

func TestReplacer_PinAfterVictim(t *testing.T) {
	replacer := NewLRUReplacer(2)

	replacer.Unpin(0)
	replacer.Unpin(1)

	victim1, _ := replacer.Victim()

	replacer.Unpin(victim1)

	victim2, ok := replacer.Victim()
	if !ok {
		t.Fatal("expected second frame to be victim")
	}

	if victim1 == victim2 {
		t.Fatalf("expected different frames, got %d twice", victim1)
	}
}

func TestReplacer_Threshold(t *testing.T) {
	replacer := NewLRUReplacer(10)

	for i := 0; i < 10; i++ {
		replacer.Unpin(FrameID(i))
	}

	if replacer.Size() != 10 {
		t.Fatalf("expected size 10 after unpinning all, got %d", replacer.Size())
	}

	for i := 0; i < 10; i++ {
		replacer.Pin(FrameID(i))
	}

	_, ok := replacer.Victim()
	if ok {
		t.Fatal("expected no victim when all frames are pinned")
	}

	if replacer.Size() != 0 {
		t.Fatalf("expected size 0 when all pinned, got %d", replacer.Size())
	}
}
