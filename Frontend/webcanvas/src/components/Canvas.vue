<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import ColorSelector from './ColorSelector.vue'

const canvasRef = ref<HTMLCanvasElement | null>(null)
let ctx: CanvasRenderingContext2D | null = null
let imageData: ImageData | null = null

// Define canvas dimensions 
const CANVAS_WIDTH = 256 //4096
const CANVAS_HEIGHT = 256 //4096

// Pan and zoom state
const zoom = ref(0.9) // 0.9 = 90% of viewport height
const panX = ref(0)
const panY = ref(0)
const isDragging = ref(false)
const dragStartX = ref(0)
const dragStartY = ref(0)
const isDrawing = ref(false)
const brushRadius = ref(1) // Brush radius in pixels

/**
 * Set brush size
 */
const setBrushSize = (size: number) => {
  brushRadius.value = size
}

/**
 * Prevent toolbar clicks from reaching canvas
 */
const handleToolbarClick = (e: MouseEvent) => {
  e.stopPropagation()
}

const handleToolbarMouseDown = (e: MouseEvent) => {
  e.stopPropagation()
}

// Current drawing color
const currentColor = ref({ r: 255, g: 255, b: 255, hex: '#FFFFFF' })

/**
 * Handle color change from color selector
 */
const handleColorChange = (color: { r: number; g: number; b: number; hex: string }) => {
  currentColor.value = color
}

/**
 * Calculate zoom level that fills the available height
 */
const getFillHeightZoom = (containerHeight: number): number => {
  return containerHeight / CANVAS_HEIGHT
}

// Computed style for canvas transform
const canvasStyle = computed(() => {
  const container = canvasRef.value?.parentElement
  if (!container) {
    return {
      transform: `translate(${panX.value}px, ${panY.value}px) scale(${zoom.value})`,
      transformOrigin: '0 0'
    }
  }
  
  const fillHeightZoom = getFillHeightZoom(container.clientHeight)
  const actualZoom = zoom.value * fillHeightZoom
  
  return {
    transform: `translate(${panX.value}px, ${panY.value}px) scale(${actualZoom})`,
    transformOrigin: '0 0'
  }
})

/**
 * Set a pixel at the given coordinates with the specified color
 * @param x - X coordinate (0 to 4095)
 * @param y - Y coordinate (0 to 4095)
 * @param r - Red component (0-255)
 * @param g - Green component (0-255)
 * @param b - Blue component (0-255)
 * @param a - Alpha component (0-255), default 255
 */
const setPixel = (x: number, y: number, r: number, g: number, b: number, a: number = 255) => {
  if (!imageData || !ctx) return
  
  if (x < 0 || x >= CANVAS_WIDTH || y < 0 || y >= CANVAS_HEIGHT) return
  
  const index = (y * CANVAS_WIDTH + x) * 4
  imageData.data[index] = r
  imageData.data[index + 1] = g
  imageData.data[index + 2] = b
  imageData.data[index + 3] = a
}

/**
 * Set a pixel using hex color
 * @param x - X coordinate
 * @param y - Y coordinate
 * @param hexColor - Hex color string (e.g., '#FF0000' or '#FF0000FF')
 */
const setPixelHex = (x: number, y: number, hexColor: string) => {
  const hex = hexColor.replace('#', '')
  const r = parseInt(hex.substring(0, 2), 16)
  const g = parseInt(hex.substring(2, 4), 16)
  const b = parseInt(hex.substring(4, 6), 16)
  const a = hex.length === 8 ? parseInt(hex.substring(6, 8), 16) : 255
  
  setPixel(x, y, r, g, b, a)
}

/**
 * Update the canvas with the current pixel data
 */
const render = () => {
  if (!ctx || !imageData) return
  ctx.putImageData(imageData, 0, 0)
}

/**
 * Clear the canvas to white
 */
const clear = () => {
  if (!ctx) return
  ctx.fillStyle = '#FFFFFF'
  ctx.fillRect(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT)
  if (imageData) {
    imageData = ctx.getImageData(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT)
  }
}

/**
 * Draw a circle of pixels at the given coordinates
 */
const drawCircle = (centerX: number, centerY: number, radius: number) => {
  for (let y = -radius; y <= radius; y++) {
    for (let x = -radius; x <= radius; x++) {
      if (x * x + y * y <= radius * radius) {
        setPixel(centerX + x, centerY + y, currentColor.value.r, currentColor.value.g, currentColor.value.b, 255)
      }
    }
  }
}

/**
 * Convert mouse event to canvas coordinates
 */
const getCanvasCoordinates = (e: MouseEvent): { x: number, y: number } | null => {
  if (!canvasRef.value) return null
  
  const rect = canvasRef.value.getBoundingClientRect()
  const clickX = e.clientX - rect.left
  const clickY = e.clientY - rect.top
  
  const scaleX = rect.width / CANVAS_WIDTH
  const scaleY = rect.height / CANVAS_HEIGHT
  
  const canvasX = Math.floor(clickX / scaleX)
  const canvasY = Math.floor(clickY / scaleY)
  
  return { x: canvasX, y: canvasY }
}

/**
 * Paint the canvas with a colorful pattern
 */
const paintPattern = () => {
  if (!imageData) return
  
  console.log('Starting to paint pattern...')
  
  for (let y = 0; y < CANVAS_HEIGHT; y++) {
    for (let x = 0; x < CANVAS_WIDTH; x++) {
      const r = (x * y) % 255
      const g = (x + y) % 255
      const b = (x * x + y) % 255
      
      setPixel(x, y, r, g, b, 255)
    }
  }
  
  console.log('Pattern painted, rendering...')
  render()
  console.log('Render complete!')
}

onMounted(() => {
  if (canvasRef.value) {
    console.log('Canvas mounted, dimensions:', CANVAS_WIDTH, 'x', CANVAS_HEIGHT)
    
    // Set canvas resolution to 4K 1:1
    canvasRef.value.width = CANVAS_WIDTH
    canvasRef.value.height = CANVAS_HEIGHT
    
    ctx = canvasRef.value.getContext('2d', { willReadFrequently: true })
    
    if (ctx) {
      console.log('Context acquired')
      // Get image data for pixel manipulation
      imageData = ctx.getImageData(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT)
      console.log('ImageData acquired, size:', imageData.data.length)
      
      // Paint the pattern
      paintPattern()
      
      // Center the canvas - calculate offset from top-left of container
      setTimeout(() => {
        resetView()
      }, 100)
    }
  }
})

/**
 * Get the canvas context for direct drawing operations
 */
const getContext = () => ctx

/**
 * Get the canvas element
 */
const getCanvas = () => canvasRef.value

/**
 * Reset view to center and initial zoom
 */
const resetView = () => {
  zoom.value = 0.9
  
  const container = canvasRef.value?.parentElement
  if (container && canvasRef.value) {
    const containerWidth = container.clientWidth
    const containerHeight = container.clientHeight
    const fillHeightZoom = getFillHeightZoom(containerHeight)
    const actualZoom = zoom.value * fillHeightZoom
    const scaledWidth = CANVAS_WIDTH * actualZoom
    const scaledHeight = CANVAS_HEIGHT * actualZoom
    
    panX.value = (containerWidth - scaledWidth) / 2
    panY.value = (containerHeight - scaledHeight) / 2
  }
}

// Pan and zoom event handlers
const handleWheel = (e: WheelEvent) => {
  e.preventDefault()
  
  if (!canvasRef.value) return
  
  // Smooth logarithmic zoom - smaller steps at higher zoom levels
  const zoomSpeed = 0.05 // Adjust for smoother/faster zoom
  const direction = e.deltaY > 0 ? -1 : 1
  const zoomDelta = direction * zoomSpeed * zoom.value
  
  const newZoom = Math.max(0.05, Math.min(100, zoom.value + zoomDelta))
  
  // Zoom toward center of screen
  const container = canvasRef.value.parentElement
  if (container) {
    const centerX = container.clientWidth / 2
    const centerY = container.clientHeight / 2
    
    // Adjust pan to keep center point fixed during zoom
    panX.value = centerX - (centerX - panX.value) * (newZoom / zoom.value)
    panY.value = centerY - (centerY - panY.value) * (newZoom / zoom.value)
  }
  
  zoom.value = newZoom
}

const handleMouseDown = (e: MouseEvent) => {
  // Check if clicking on canvas (not on toolbar)
  const target = e.target as HTMLElement
  if (target.closest('.toolbar')) {
    return // Ignore clicks on toolbar
  }
  
  if (e.button === 2) { // Right click for panning
    isDragging.value = true
    dragStartX.value = e.clientX - panX.value
    dragStartY.value = e.clientY - panY.value
  } else if (e.button === 0) { // Left click for drawing
    isDrawing.value = true
    const coords = getCanvasCoordinates(e)
    if (coords && coords.x >= 0 && coords.x < CANVAS_WIDTH && coords.y >= 0 && coords.y < CANVAS_HEIGHT) {
      drawCircle(coords.x, coords.y, brushRadius.value)
      render()
    }
  }
}

const handleMouseMove = (e: MouseEvent) => {
  if (isDragging.value) {
    // Panning
    panX.value = e.clientX - dragStartX.value
    panY.value = e.clientY - dragStartY.value
  } else if (isDrawing.value) {
    // Drawing
    const coords = getCanvasCoordinates(e)
    if (coords && coords.x >= 0 && coords.x < CANVAS_WIDTH && coords.y >= 0 && coords.y < CANVAS_HEIGHT) {
      drawCircle(coords.x, coords.y, brushRadius.value)
      render()
    }
  }
}

const handleMouseUp = () => {
  isDragging.value = false
  isDrawing.value = false
}

const handleMouseLeave = () => {
  isDragging.value = false
  isDrawing.value = false
}

const handleContextMenu = (e: MouseEvent) => {
  e.preventDefault() // Prevent right-click menu
}

// Expose methods to parent components
defineExpose({
  setPixel,
  setPixelHex,
  render,
  clear,
  getContext,
  getCanvas,
  brushRadius,
  width: CANVAS_WIDTH,
  height: CANVAS_HEIGHT
})
</script>

<template>
  <div 
    class="canvas-container"
    @wheel="handleWheel"
    @mousedown="handleMouseDown"
    @mousemove="handleMouseMove"
    @mouseup="handleMouseUp"
    @mouseleave="handleMouseLeave"
    @contextmenu="handleContextMenu"
  >
    <canvas 
      ref="canvasRef" 
      class="canvas"
      :style="canvasStyle"
    ></canvas>
    
    <div class="toolbar" @mousedown="handleToolbarMouseDown" @click="handleToolbarClick">
      <ColorSelector @color-change="handleColorChange" />
      
      <div class="separator"></div>
      
      <div class="brush-size-selector">
        <button 
          v-for="size in [0, 1, 2]" 
          :key="size"
          class="brush-size-button"
          :class="{ active: brushRadius === size }"
          @click="setBrushSize(size)"
          :title="size === 0 ? 'Single pixel' : `Radius ${size}`"
        >
          <div class="brush-preview" :style="{ width: `${(size * 2 + 8)}px`, height: `${(size * 2 + 8)}px` }"></div>
        </button>
      </div>
      
      <div class="separator"></div>
      
      <button class="reset-button" @click="resetView" title="Reset view"></button>
    </div>
  </div>
</template>

<style scoped>
.canvas-container {
  position: relative;
  width: 100%;
  height: 100%;
  background-color: #1f1f1f;
  overflow: hidden;
  cursor: crosshair;
}

.canvas {
  border: 0px solid #ffffff;
  box-shadow: 5px 4px 6px rgba(0, 0, 0, 0.7);
  position: absolute;
  top: 0;
  left: 0;
  image-rendering: pixelated;
  image-rendering: crisp-edges;
}

.toolbar {
  position: absolute;
  bottom: 30px;
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  background-color: rgba(35, 35, 35, 0.95);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 50px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4), 0 0 0 1px rgba(255, 255, 255, 0.05);
  backdrop-filter: blur(10px);
  z-index: 10;
}

.separator {
  width: 1px;
  height: 24px;
  background-color: rgba(255, 255, 255, 0.15);
  margin: 0 4px;
}

.brush-size-selector {
  display: flex;
  gap: 4px;
  align-items: center;
}

.brush-size-button {
  width: 32px;
  height: 32px;
  padding: 0;
  background-color: transparent;
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 50%;
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  justify-content: center;
}

.brush-size-button:hover {
  background-color: rgba(255, 255, 255, 0.1);
  border-color: rgba(255, 255, 255, 0.4);
  transform: scale(1.1);
}

.brush-size-button.active {
  background-color: rgba(255, 255, 255, 0.15);
  border-color: rgba(255, 255, 255, 0.5);
}

.brush-size-button:active {
  transform: scale(0.95);
}

.brush-preview {
  background-color: rgba(255, 255, 255, 0.8);
  border-radius: 50%;
  transition: all 0.2s ease;
}

.brush-size-button:hover .brush-preview {
  background-color: rgba(255, 255, 255, 1);
}

.reset-button {
  width: 32px;
  height: 32px;
  padding: 0;
  background-color: transparent;
  color: rgba(255, 255, 255, 0.7);
  border: none;
  border-radius: 50%;
  cursor: pointer;
  font-size: 18px;
  font-family: inherit;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  justify-content: center;
}

.reset-button::before {
  content: '⟲';
  font-size: 20px;
}

.reset-button:hover {
  background-color: rgba(255, 255, 255, 0.1);
  color: rgba(255, 255, 255, 1);
  transform: scale(1.1);
}

.reset-button:active {
  transform: scale(0.95);
}</style>