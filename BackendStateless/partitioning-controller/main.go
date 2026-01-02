package main

import (
	"fmt"
	pixelhasher "general"
)

func main() {
	pixel := []uint8{255, 127, 63} // Example pixel data (RGB for red)
	hash := pixelhasher.PixelHasher(pixel)
	fmt.Printf("Pixel hash: %d\n", hash)
	fmt.Println("Hello, Partitioning Controller!")
}
