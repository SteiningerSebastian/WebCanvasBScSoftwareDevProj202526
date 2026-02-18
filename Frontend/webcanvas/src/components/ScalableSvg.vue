<template>
  <div class="scalable-svg-container">
    <img 
      :src="src" 
      :alt="alt" 
      class="scalable-svg" 
      @click="openFullscreen"
      role="button"
      tabindex="0"
      @keydown.enter="openFullscreen"
      @keydown.space="openFullscreen"
    />
    
    <!-- Fullscreen Modal -->
    <Teleport to="body">
      <div v-if="isFullscreen" class="fullscreen-overlay" @click.self="closeFullscreen">
        <button class="close-button" @click="closeFullscreen" aria-label="Close fullscreen view">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
        
        <div class="zoom-controls">
          <button @click="zoomIn" aria-label="Zoom in">+</button>
          <button @click="resetZoom" aria-label="Reset zoom">Reset</button>
          <button @click="zoomOut" aria-label="Zoom out">−</button>
        </div>
        
        <div 
          class="fullscreen-content" 
          ref="contentRef"
          @wheel.prevent="handleWheel"
          @mousedown="handleMouseDown"
          @touchstart="handleTouchStart"
          @touchmove="handleTouchMove"
          @touchend="handleTouchEnd"
        >
          <img 
            :src="src" 
            :alt="alt" 
            class="fullscreen-svg"
            :style="imageStyle"
            draggable="false"
          />
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'

interface Props {
  src: string
  alt: string
}

defineProps<Props>()

const isFullscreen = ref(false)
const scale = ref(1)
const translateX = ref(0)
const translateY = ref(0)
const contentRef = ref<HTMLElement | null>(null)

let isDragging = false
let startX = 0
let startY = 0
let lastTouchDistance = 0

const imageStyle = computed(() => ({
  transform: `translate(${translateX.value}px, ${translateY.value}px) scale(${scale.value})`,
  transformOrigin: 'center center',
  transition: isDragging ? 'none' : 'transform 0.2s ease-out'
}))

const openFullscreen = () => {
  isFullscreen.value = true
  document.body.style.overflow = 'hidden'
}

const closeFullscreen = () => {
  isFullscreen.value = false
  document.body.style.overflow = ''
  resetZoom()
}

const zoomIn = () => {
  scale.value = Math.min(scale.value * 1.2, 5)
}

const zoomOut = () => {
  scale.value = Math.max(scale.value / 1.2, 0.5)
}

const resetZoom = () => {
  scale.value = 1
  translateX.value = 0
  translateY.value = 0
}

const handleWheel = (e: WheelEvent) => {
  e.preventDefault()
  const delta = e.deltaY > 0 ? 0.9 : 1.1
  scale.value = Math.min(Math.max(scale.value * delta, 0.5), 5)
}

const handleMouseDown = (e: MouseEvent) => {
  isDragging = true
  startX = e.clientX - translateX.value
  startY = e.clientY - translateY.value
  
  const handleMouseMove = (e: MouseEvent) => {
    if (isDragging) {
      translateX.value = e.clientX - startX
      translateY.value = e.clientY - startY
    }
  }
  
  const handleMouseUp = () => {
    isDragging = false
    document.removeEventListener('mousemove', handleMouseMove)
    document.removeEventListener('mouseup', handleMouseUp)
  }
  
  document.addEventListener('mousemove', handleMouseMove)
  document.addEventListener('mouseup', handleMouseUp)
}

const getTouchDistance = (touches: TouchList) => {
  if (touches.length < 2) return 0
  const touch0 = touches.item(0)
  const touch1 = touches.item(1)
  if (!touch0 || !touch1) return 0
  const dx = touch0.clientX - touch1.clientX
  const dy = touch0.clientY - touch1.clientY
  return Math.sqrt(dx * dx + dy * dy)
}

const handleTouchStart = (e: TouchEvent) => {
  if (e.touches.length === 2) {
    lastTouchDistance = getTouchDistance(e.touches)
  } else if (e.touches.length === 1) {
    const touch = e.touches.item(0)
    if (!touch) return
    isDragging = true
    startX = touch.clientX - translateX.value
    startY = touch.clientY - translateY.value
  }
}

const handleTouchMove = (e: TouchEvent) => {
  e.preventDefault()
  
  if (e.touches.length === 2) {
    // Pinch to zoom
    const distance = getTouchDistance(e.touches)
    if (lastTouchDistance > 0) {
      const delta = distance / lastTouchDistance
      scale.value = Math.min(Math.max(scale.value * delta, 0.5), 5)
    }
    lastTouchDistance = distance
  } else if (e.touches.length === 1 && isDragging) {
    // Pan
    const touch = e.touches.item(0)
    if (!touch) return
    translateX.value = touch.clientX - startX
    translateY.value = touch.clientY - startY
  }
}

const handleTouchEnd = () => {
  isDragging = false
  lastTouchDistance = 0
}
</script>

<style scoped>
.scalable-svg-container {
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  overflow: hidden;
}

.scalable-svg {
  width: auto;
  height: auto;
  max-width: 100%;
  object-fit: contain;
  cursor: pointer;
  transition: opacity 0.2s;
}

.scalable-svg:hover {
  opacity: 0.9;
}

.scalable-svg:focus {
  outline: 2px solid #5a9efc;
  outline-offset: 2px;
}

.fullscreen-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  background: rgba(0, 0, 0, 0.95);
  z-index: 10000;
  display: flex;
  justify-content: center;
  align-items: center;
}

.fullscreen-content {
  width: 100%;
  height: 100%;
  overflow: auto;
  display: flex;
  justify-content: center;
  align-items: center;
  cursor: grab;
  touch-action: none;
}

.fullscreen-content:active {
  cursor: grabbing;
}

.fullscreen-svg {
  max-width: none;
  max-height: none;
  user-select: none;
  pointer-events: none;
}

.close-button {
  position: absolute;
  top: 1rem;
  right: 1rem;
  width: 3rem;
  height: 3rem;
  background: rgba(255, 255, 255, 0.1);
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-radius: 50%;
  color: white;
  cursor: pointer;
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 10001;
  transition: all 0.2s;
}

.close-button:hover {
  background: rgba(255, 255, 255, 0.2);
  border-color: rgba(255, 255, 255, 0.5);
}

.close-button svg {
  width: 1.5rem;
  height: 1.5rem;
}

.zoom-controls {
  position: absolute;
  bottom: 2rem;
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  gap: 0.5rem;
  z-index: 10001;
  background: rgba(0, 0, 0, 0.7);
  padding: 0.5rem;
  border-radius: 0.5rem;
  border: 1px solid rgba(255, 255, 255, 0.2);
}

.zoom-controls button {
  width: 3rem;
  height: 3rem;
  background: rgba(255, 255, 255, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.3);
  border-radius: 0.25rem;
  color: white;
  font-size: 1.5rem;
  cursor: pointer;
  display: flex;
  justify-content: center;
  align-items: center;
  transition: all 0.2s;
  font-weight: bold;
}

.zoom-controls button:nth-child(2) {
  font-size: 0.875rem;
  width: 4rem;
  font-weight: normal;
}

.zoom-controls button:hover {
  background: rgba(255, 255, 255, 0.2);
  border-color: rgba(255, 255, 255, 0.5);
}

@media screen and (max-width: 600px) {
  .close-button {
    top: 0.5rem;
    right: 0.5rem;
    width: 2.5rem;
    height: 2.5rem;
  }
  
  .close-button svg {
    width: 1.25rem;
    height: 1.25rem;
  }
  
  .zoom-controls {
    bottom: 3rem;
    bottom: max(3rem, env(safe-area-inset-bottom, 0px) + 1rem);
  }
  
  .zoom-controls button {
    width: 2.5rem;
    height: 2.5rem;
    font-size: 1.25rem;
  }
  
  .zoom-controls button:nth-child(2) {
    font-size: 0.75rem;
    width: 3.5rem;
  }
}
</style>
