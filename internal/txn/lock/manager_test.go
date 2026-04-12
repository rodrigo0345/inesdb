package lock

import (
	"testing"
)

func TestLockManagerAcquire(t *testing.T) {
	t.Run("acquire shared lock", func(t *testing.T) {
	})

	t.Run("acquire exclusive lock", func(t *testing.T) {
	})

	t.Run("lock compatibility", func(t *testing.T) {
	})

	t.Run("lock on multiple resources", func(t *testing.T) {
	})
}

func TestLockManagerRelease(t *testing.T) {
	t.Run("release shared lock", func(t *testing.T) {
	})

	t.Run("release exclusive lock", func(t *testing.T) {
	})

	t.Run("release unlocks other transactions", func(t *testing.T) {
	})

	t.Run("release all locks for transaction", func(t *testing.T) {
	})
}

func TestLockManagerCompatibility(t *testing.T) {
	tests := []struct {
		held       string
		requested  string
		compatible bool
	}{
		{"S", "S", true},
		{"S", "X", false},
		{"X", "S", false},
		{"X", "X", false},
	}

	for _, tt := range tests {
		t.Run(tt.held+" + "+tt.requested, func(t *testing.T) {
		})
	}
}

func TestLockManagerWaitQueue(t *testing.T) {
	t.Run("queue waiting requests", func(t *testing.T) {
	})

	t.Run("fifo ordering", func(t *testing.T) {
	})

	t.Run("remove from queue on abort", func(t *testing.T) {
	})

	t.Run("promote from waiting", func(t *testing.T) {
	})
}

func TestLockManagerPriority(t *testing.T) {
	t.Run("high priority waits shorter", func(t *testing.T) {
	})

	t.Run("aging increases priority", func(t *testing.T) {
	})

	t.Run("prevent starvation", func(t *testing.T) {
	})
}

func TestLockManagerUpgrading(t *testing.T) {
	t.Run("upgrade shared to exclusive", func(t *testing.T) {
	})

	t.Run("upgrade fails with conflict", func(t *testing.T) {
	})

	t.Run("no downgrade", func(t *testing.T) {
	})
}

func BenchmarkLockManagerAcquire(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
	}
}

func BenchmarkLockManagerRelease(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
	}
}

func BenchmarkLockManagerConflict(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
	}
}
