// A package containing functions for hashing pixel data and converting pixel coordinates to keys.
package pixelhasher

func PixelHasher(data []byte) uint32 {
	return 42 // Placeholder implementation
}

// PixelToKey converts pixel coordinates (x, y) into a unique uint32 key.
func PixelToKey(x uint16, y uint16) uint32 {
	key := uint32(x)<<16 | uint32(y)

	// To avoid patterns that could lead to biased partitioning, we apply a bijective mapping.
	key = permute32Bitwise(key)

	return key
}

// Bijective permutation of uint32_t values.
// This function is a composition of invertible (bijective) operations
// over Z / 2^32 Z, therefore the whole function is bijective.
//
// Purpose:
//  - Strongly scramble bit patterns
//  - Destroy locality (near inputs -> far outputs)
//  - Suitable for partitioning / sharding
//
// NOTE:
//  - This is NOT a cryptographic hash
//  - Every step has a well-defined inverse
//
// Author: Implementation and comments by ChatGPT-5
func permute32Bitwise(x uint32) uint32 {
	// Step 1: XOR upper 16 bits into lower 16 bits
	//
	// x = [H | L]  ->  [H | L ⊕ H]
	//
	// This mixes high bits into low bits.
	// Invertible because XOR is its own inverse:
	//
	// Inverse: x ^= x >> 16;
	x ^= x >> 16

	// Step 2: Multiply by an odd constant modulo 2^32
	//
	// Multiplication by an odd number is invertible in Z / 2^32 Z
	// because gcd(constant, 2^32) = 1.
	//
	// This spreads bit dependencies across many bit positions.
	//
	// Inverse: x *= modular_inverse(0x7feb352d) mod 2^32;
	x *= 0x7feb352d

	// Step 3: XOR-shift to further mix bits
	//
	// Again, higher bits influence lower bits.
	// This is a linear, triangular transform over GF(2),
	// and is therefore invertible.
	//
	// Inverse: x ^= x >> 15;
	x ^= x >> 15

	// Step 4: Second odd multiplication for additional diffusion
	//
	// Same reasoning as Step 2.
	//
	// Inverse: x *= modular_inverse(0x846ca68b) mod 2^32;
	x *= 0x846ca68b

	// Step 5: Final XOR-shift to fold high bits into low bits again
	//
	// This ensures high-order input bits influence all output bits.
	//
	// Inverse: x ^= x >> 16;
	x ^= x >> 16

	// Since each step above is bijective, and a composition of
	// bijections is itself a bijection, this function is a
	// permutation of all 2^32 uint32_t values.
	return x
}
