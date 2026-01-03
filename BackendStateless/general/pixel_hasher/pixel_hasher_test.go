package pixelhasher

import (
	"testing"
)

func TestPixelHasher(t *testing.T) {
	// Test with empty data
	if got := PixelHasher([]byte{}); got != 42 {
		t.Errorf("PixelHasher([]) = %v, want 42", got)
	}

	// Test with some data
	if got := PixelHasher([]byte{1, 2, 3}); got != 42 {
		t.Errorf("PixelHasher([1,2,3]) = %v, want 42", got)
	}
}

func TestPixelToKey_Uniqueness(t *testing.T) {
	seen := make(map[uint32]struct{})
	for x := uint16(0); x < 256; x++ {
		for y := uint16(0); y < 256; y++ {
			key := PixelToKey(x, y)
			if _, exists := seen[key]; exists {
				t.Fatalf("Duplicate key for (x=%d, y=%d): %v", x, y, key)
			}
			seen[key] = struct{}{}
		}
	}
}

func TestPixelToKey_DiffersForDifferentInputs(t *testing.T) {
	k1 := PixelToKey(100, 200)
	k2 := PixelToKey(200, 100)
	if k1 == k2 {
		t.Errorf("PixelToKey(100,200) == PixelToKey(200,100): %v", k1)
	}
}

func TestPermute32Bitwise_Bijective(t *testing.T) {
	// Test that permute32Bitwise is bijective for a small sample
	seen := make(map[uint32]struct{})
	for i := uint32(0); i < 100000; i += 1234 {
		out := permute32Bitwise(i)
		if _, exists := seen[out]; exists {
			t.Fatalf("permute32Bitwise is not bijective, duplicate output: %v", out)
		}
		seen[out] = struct{}{}
	}
}

func TestPermute32Bitwise_ChangesBits(t *testing.T) {
	x := uint32(0x12345678)
	y := permute32Bitwise(x)
	if x == y {
		t.Errorf("permute32Bitwise(%v) = %v, want different value", x, y)
	}
}
