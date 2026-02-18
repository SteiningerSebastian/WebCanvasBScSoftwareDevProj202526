<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed } from 'vue'
import * as signalR from '@microsoft/signalr'
import ColorSelector from './ColorSelector.vue'

const canvasRef = ref<HTMLCanvasElement | null>(null)
let ctx: CanvasRenderingContext2D | null = null
let imageData: ImageData | null = null

// Define canvas dimensions 
const CANVAS_WIDTH = 256 //4096
const CANVAS_HEIGHT = 256 //4096

// SignalR connection
let connection: signalR.HubConnection | null = null
const connectionStatus = ref<'disconnected' | 'connecting' | 'connected'>('disconnected')
const pixelsReceived = ref(0)
const pixelsSent = ref(0)

// Pan and zoom state
const zoom = ref(0.9) // 0.9 = 90% of viewport height
const panX = ref(0)
const panY = ref(0)
const isDragging = ref(false)
const dragStartX = ref(0)
const dragStartY = ref(0)
const isDrawing = ref(false)
const brushRadius = ref(1) // Brush radius in pixels

// Touch state
let lastTouchDistance = 0
let lastTouchX = 0
let lastTouchY = 0
const isTouching = ref(false)
const touchMode = ref<'draw' | 'pan' | 'zoom'>('draw')

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
const getFillZoom = (containerHeight: number, containerWidth: number): number => {
  const zoomHeight = containerHeight / CANVAS_HEIGHT
  const zoomWidth = containerWidth / CANVAS_WIDTH
  return Math.min(zoomHeight, zoomWidth)
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
  
  const fillHeightZoom = getFillZoom(container.clientHeight, container.clientWidth)
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
 * @returns true if pixel was changed, false if it was already the target color
 */
const setPixel = (x: number, y: number, r: number, g: number, b: number, a: number = 255): boolean => {
  if (!imageData || !ctx) return false
  
  if (x < 0 || x >= CANVAS_WIDTH || y < 0 || y >= CANVAS_HEIGHT) return false
  
  const index = (y * CANVAS_WIDTH + x) * 4
  
  // Check if pixel is already the target color
  if (imageData.data[index] === r && 
      imageData.data[index + 1] === g && 
      imageData.data[index + 2] === b && 
      imageData.data[index + 3] === a) {
    return false // No change needed
  }
  
  imageData.data[index] = r
  imageData.data[index + 1] = g
  imageData.data[index + 2] = b
  imageData.data[index + 3] = a
  return true
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
        const pixelX = centerX + x
        const pixelY = centerY + y
        
        // Set pixel locally - only proceed if pixel actually changed
        const changed = setPixel(pixelX, pixelY, currentColor.value.r, currentColor.value.g, currentColor.value.b, 255)
        
        // Send to hub asynchronously if pixel changed and we're connected
        if (changed && connection && connectionStatus.value === 'connected') {
          // Fire and forget - don't await
          connection.invoke('SetPixel', {
            x: pixelX,
            y: pixelY,
            color: {
              R: currentColor.value.r,
              G: currentColor.value.g,
              B: currentColor.value.b
            }
          }).then(() => {
            pixelsSent.value++
          }).catch((error) => {
            console.error('Failed to send pixel to hub:', error)
          })

          console.log(`Pixel set at (${pixelX}, ${pixelY}) with color (${currentColor.value.r}, ${currentColor.value.g}, ${currentColor.value.b})`)
        }
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
 * Convert touch event to canvas coordinates
 */
const getCanvasCoordinatesFromTouch = (touch: Touch): { x: number, y: number } | null => {
  if (!canvasRef.value) return null
  
  const rect = canvasRef.value.getBoundingClientRect()
  const clickX = touch.clientX - rect.left
  const clickY = touch.clientY - rect.top
  
  const scaleX = rect.width / CANVAS_WIDTH
  const scaleY = rect.height / CANVAS_HEIGHT
  
  const canvasX = Math.floor(clickX / scaleX)
  const canvasY = Math.floor(clickY / scaleY)
  
  return { x: canvasX, y: canvasY }
}

/**
 * Calculate distance between two touch points
 */
const getTouchDistance = (touches: TouchList): number => {
  if (touches.length < 2) return 0
  const touch0 = touches[0]
  const touch1 = touches[1]
  if (!touch0 || !touch1) return 0
  const dx = touch0.clientX - touch1.clientX
  const dy = touch0.clientY - touch1.clientY
  return Math.sqrt(dx * dx + dy * dy)
}

/**
 * Get center point between two touches
 */
const getTouchCenter = (touches: TouchList): { x: number, y: number } | null => {
  const touch0 = touches[0]
  const touch1 = touches[1]
  if (!touch0 || !touch1) return null
  return {
    x: (touch0.clientX + touch1.clientX) / 2,
    y: (touch0.clientY + touch1.clientY) / 2
  }
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

/**
 * Initialize SignalR connection
 */
const connectToHub = async () => {
  try {
    console.log('Initializing SignalR connection to /canvas...')
    connectionStatus.value = 'connecting'

    connection = new signalR.HubConnectionBuilder()
      .withUrl('http://localhost:8080/canvas')
      .withAutomaticReconnect()
      .configureLogging(signalR.LogLevel.Information)
      .build()

    // Register event handlers
    connection.on('PixelReceived', (pixel: { x: number; y: number; color: { r: number; g: number; b: number } }) => {
      pixelsReceived.value++
      setPixel(pixel.x, pixel.y, pixel.color.r, pixel.color.g, pixel.color.b, 255)
      render()
    })

    connection.on('PixelUpdated', (response: { x: number; y: number; color: { r?: number; g?: number; b?: number; R?: number; G?: number; B?: number } }) => {
      // Handle both camelCase (r,g,b) and PascalCase (R,G,B) from SignalR
      const r = response.color.r ?? response.color.R ?? 0
      const g = response.color.g ?? response.color.G ?? 0
      const b = response.color.b ?? response.color.B ?? 0

      console.log(`Pixel updated at (${response.x}, ${response.y}): (${r}, ${g}, ${b})`)
      
      setPixel(response.x, response.y, r, g, b, 255)
      render()
    })

    connection.on('CanvasStreamComplete', (count: number) => {
      console.log(`Canvas stream complete: ${count} pixels received`)
      render()
    })

    connection.on('CanvasStreamFailed', (error: string) => {
      console.error(`Canvas stream failed: ${error}`)
    })

    connection.onreconnecting(() => {
      console.warn('Connection lost, reconnecting...')
      connectionStatus.value = 'connecting'
    })

    connection.onreconnected(() => {
      console.log('Reconnected successfully')
      connectionStatus.value = 'connected'
      // Request canvas stream to sync state
      streamCanvas()
    })

    connection.onclose((error) => {
      console.error(`Connection closed: ${error || 'No error details'}`)
      connectionStatus.value = 'disconnected'
    })

    // Start connection
    await connection.start()
    console.log('Connected successfully to CanvasHub')
    connectionStatus.value = 'connected'
    
    // Request initial canvas state
    await streamCanvas()

  } catch (error) {
    console.error('Failed to connect to hub:', error)
    connectionStatus.value = 'disconnected'
  }
}

/**
 * Request canvas stream from server
 */
const streamCanvas = async () => {
  if (!connection || connectionStatus.value !== 'connected') {
    console.warn('Cannot stream canvas: not connected')
    return
  }
  
  try {
    console.log('Requesting canvas stream...')
    await connection.invoke('StreamCanvas')
  } catch (error) {
    console.error('Failed to stream canvas:', error)
  }
}

/**
 * Disconnect from SignalR hub
 */
const disconnectFromHub = async () => {
  if (connection) {
    try {
      await connection.stop()
      console.log('Disconnected from hub')
    } catch (error) {
      console.error('Error disconnecting from hub:', error)
    }
  }
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
      // Only for Debug: paintPattern()
      clear() // Start with a blank canvas
      
      // Center the canvas - calculate offset from top-left of container
      setTimeout(() => {
        resetView()
      }, 100)
      
      // Connect to SignalR hub
      connectToHub()
    }
  }
})

onUnmounted(() => {
  disconnectFromHub()
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
    const fillZoom = getFillZoom(containerHeight, containerWidth)
    const actualZoom = zoom.value * fillZoom
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

// Touch event handlers
const handleTouchStart = (e: TouchEvent) => {
  // Check if touching toolbar
  const target = e.target as HTMLElement
  if (target.closest('.toolbar') || target.closest('.status-badge')) {
    return // Let toolbar handle its own events
  }

  e.preventDefault()
  isTouching.value = true
  
  if (e.touches.length === 1) {
    // Single touch - prepare for drawing (but don't draw yet)
    touchMode.value = 'draw'
    const touch = e.touches[0]
    if (!touch) return
    lastTouchX = touch.clientX
    lastTouchY = touch.clientY
    // Don't draw immediately - wait for touchMove to confirm it's a drag
  } else if (e.touches.length === 2) {
    // Two touches - start pinch zoom
    touchMode.value = 'zoom'
    isDrawing.value = false
    lastTouchDistance = getTouchDistance(e.touches)
    const center = getTouchCenter(e.touches)
    if (!center) return
    lastTouchX = center.x
    lastTouchY = center.y
  }
}

const handleTouchMove = (e: TouchEvent) => {
  if (!isTouching.value) return
  
  // Check if touching toolbar - let toolbar handle its own events
  const target = e.target as HTMLElement
  if (target.closest('.toolbar') || target.closest('.status-badge')) {
    return
  }
  
  e.preventDefault()
  
  if (e.touches.length === 1 && touchMode.value === 'draw') {
    // Single touch drawing
    const touch = e.touches[0]
    if (!touch) return
    const coords = getCanvasCoordinatesFromTouch(touch)
    if (coords && coords.x >= 0 && coords.x < CANVAS_WIDTH && coords.y >= 0 && coords.y < CANVAS_HEIGHT) {
      isDrawing.value = true
      drawCircle(coords.x, coords.y, brushRadius.value)
      render()
    }
    lastTouchX = touch.clientX
    lastTouchY = touch.clientY
  } else if (e.touches.length === 2) {
    // Two finger gestures
    const currentDistance = getTouchDistance(e.touches)
    const center = getTouchCenter(e.touches)
    if (!center) return
    
    if (touchMode.value === 'draw') {
      // Switched from drawing to two-finger gesture
      touchMode.value = 'zoom'
      isDrawing.value = false
      lastTouchDistance = currentDistance
      lastTouchX = center.x
      lastTouchY = center.y
      return
    }
    
    // Pinch to zoom
    if (lastTouchDistance > 0 && currentDistance > 0) {
      const distanceDelta = currentDistance - lastTouchDistance
      const zoomDelta = (distanceDelta / 200) * zoom.value // Scale zoom based on current zoom level
      const newZoom = Math.max(0.05, Math.min(100, zoom.value + zoomDelta))
      
      // Zoom toward pinch center
      if (canvasRef.value) {
        panX.value = center.x - (center.x - panX.value) * (newZoom / zoom.value)
        panY.value = center.y - (center.y - panY.value) * (newZoom / zoom.value)
      }
      
      zoom.value = newZoom
    }
    
    // Pan with two fingers
    const deltaX = center.x - lastTouchX
    const deltaY = center.y - lastTouchY
    panX.value += deltaX
    panY.value += deltaY
    
    lastTouchDistance = currentDistance
    lastTouchX = center.x
    lastTouchY = center.y
  }
}

const handleTouchEnd = (e: TouchEvent) => {
  // Check if touching toolbar - let toolbar handle its own events
  const target = e.target as HTMLElement
  if (target.closest('.toolbar') || target.closest('.status-badge')) {
    return // Let toolbar handle its own click events
  }
  
  e.preventDefault()
  
  if (e.touches.length === 0) {
    // All touches ended
    isTouching.value = false
    isDrawing.value = false
    isDragging.value = false
    lastTouchDistance = 0
    touchMode.value = 'draw'
  } else if (e.touches.length === 1 && touchMode.value === 'zoom') {
    // Went from two fingers to one - switch back to draw mode
    touchMode.value = 'draw'
    lastTouchDistance = 0
    const touch = e.touches[0]
    if (touch) {
      lastTouchX = touch.clientX
      lastTouchY = touch.clientY
    }
  }
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
    @touchstart="handleTouchStart"
    @touchmove="handleTouchMove"
    @touchend="handleTouchEnd"
    @touchcancel="handleTouchEnd"
  >
    <canvas 
      ref="canvasRef" 
      class="canvas"
      :style="canvasStyle"
    ></canvas>
    
    <!-- Connection Status Badge -->
    <div class="status-badge" :class="connectionStatus">
      <div class="status-dot"></div>
      <span>{{ connectionStatus === 'connected' ? 'Connected' : connectionStatus === 'connecting' ? 'Connecting...' : 'Disconnected' }}</span>
      <div class="status-stats" v-if="connectionStatus === 'connected'">
        <span>↓{{ pixelsReceived }}</span>
        <span>↑{{ pixelsSent }}</span>
      </div>
    </div>
    
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
  height: calc(100dvh - 72px);
  background-color: #1f1f1f;
  overflow: hidden;
  cursor: crosshair;
  touch-action: none;
  -webkit-user-select: none;
  user-select: none;
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
  touch-action: auto;
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
}

.status-badge {
  position: absolute;
  top: 20px;
  right: 20px;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background-color: rgba(35, 35, 35, 0.95);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 20px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
  backdrop-filter: blur(10px);
  z-index: 11;
  font-size: 12px;
  color: rgba(255, 255, 255, 0.8);
  touch-action: auto;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #f48771;
  animation: pulse 2s ease-in-out infinite;
}

.status-badge.connecting .status-dot {
  background-color: #dcdcaa;
}

.status-badge.connected .status-dot {
  background-color: #4ec9b0;
  animation: none;
}

.status-stats {
  display: flex;
  gap: 8px;
  margin-left: 4px;
  padding-left: 8px;
  border-left: 1px solid rgba(255, 255, 255, 0.15);
  font-family: 'Consolas', 'Courier New', monospace;
  font-size: 11px;
  color: rgba(255, 255, 255, 0.6);
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.3;
  }
}
</style>