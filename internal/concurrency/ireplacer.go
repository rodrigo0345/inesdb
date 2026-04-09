package concurrency

type FrameID uint32

type IReplacer interface {
	// Victim returns a frame ID to evict, or false if no frame can be evicted
	Victim() (FrameID, bool)
	// Pin marks a frame as in-use (pinned)
	Pin(frameID FrameID)
	// Unpin marks a frame as available for eviction
	Unpin(frameID FrameID)
	// Size returns the number of frames available for eviction
	Size() int
}
