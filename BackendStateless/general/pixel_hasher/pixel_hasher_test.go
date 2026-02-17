package pixelhasher

import (
	"testing"
)

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

func TestKeyToPixel_Inverse(t *testing.T) {
	for x := uint16(0); x < 256; x++ {
		for y := uint16(0); y < 256; y++ {
			key := PixelToKey(x, y)
			x2, y2 := KeyToPixel(key)
			if x != x2 || y != y2 {
				t.Errorf("KeyToPixel(PixelToKey(%d,%d)) = (%d,%d), want (%d,%d)", x, y, x2, y2, x, y)
			}
		}
	}
}
