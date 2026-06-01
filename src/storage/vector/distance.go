package vector

import "math"

// Metric identifies the distance function used by an HNSW index.
type Metric int

const (
	MetricL2     Metric = iota // squared Euclidean distance during traversal; sqrt applied to final results
	MetricCosine               // 1 − cos(a, b), range [0, 2]
	MetricDot                  // −dot(a, b); minimising = maximising similarity
)

// DistanceFunc computes a distance between two equal-length float32 vectors.
// Smaller value means more similar. For MetricL2 the value is the *squared*
// distance to avoid sqrt during graph traversal.
type DistanceFunc func(a, b []float32) float32

// L2Distance returns the squared Euclidean distance between a and b.
func L2Distance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// CosineDistance returns 1 − cos(a, b). Result is in [0, 2]; 0 means identical.
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	denom := float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB)))
	if denom == 0 {
		return 1
	}
	cos := dot / denom
	if cos > 1 {
		cos = 1
	} else if cos < -1 {
		cos = -1
	}
	return 1 - cos
}

// DotDistance returns the negative dot product of a and b.
// A smaller value means a higher dot product (more similar).
func DotDistance(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}

// MetricToFunc returns the DistanceFunc for the given metric.
func MetricToFunc(m Metric) DistanceFunc {
	switch m {
	case MetricCosine:
		return CosineDistance
	case MetricDot:
		return DotDistance
	default:
		return L2Distance
	}
}
