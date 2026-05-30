package concurrency

type FrameID uint32

type IReplacer interface {
	Victim() (FrameID, bool)
	Pin(frameID FrameID)
	Unpin(frameID FrameID)
	Size() int
}
