package concurrency

import "sync"

type clockFrame struct {
	referenced bool
	inReplacer bool
}

type ClockReplacer struct {
	mu         sync.Mutex
	frames     []clockFrame
	clockHand  int
	poolSize   int
	activeSize int
}

func NewClockReplacer(poolSize int) *ClockReplacer {
	return &ClockReplacer{
		frames:    make([]clockFrame, poolSize),
		clockHand: 0,
		poolSize:  poolSize,
	}
}

// TODO: prefer not dirty pages for eviction, as they are faster to evict
func (cr *ClockReplacer) Victim() (FrameID, bool) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if cr.activeSize == 0 {
		return 0, false
	}

	for {
		frame := &cr.frames[cr.clockHand]

		if frame.inReplacer {
			if frame.referenced {
				// Second chance: clear bit and move on
				frame.referenced = false
			} else {
				// Victim found
				victimID := FrameID(cr.clockHand)
				frame.inReplacer = false
				cr.activeSize--

				// Advance hand for next call
				cr.clockHand = (cr.clockHand + 1) % cr.poolSize
				return victimID, true
			}
		}

		cr.clockHand = (cr.clockHand + 1) % cr.poolSize
	}
}

func (cr *ClockReplacer) Pin(frameID FrameID) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if int(frameID) >= cr.poolSize {
		return
	}

	if cr.frames[frameID].inReplacer {
		cr.frames[frameID].inReplacer = false
		cr.activeSize--
	}
}

func (cr *ClockReplacer) Unpin(frameID FrameID) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if int(frameID) >= cr.poolSize {
		return
	}

	if !cr.frames[frameID].inReplacer {
		cr.frames[frameID].inReplacer = true
		cr.activeSize++
	}
	cr.frames[frameID].referenced = true
}

func (cr *ClockReplacer) Size() int {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return cr.activeSize
}
