<script setup lang="ts">
import { ref, computed } from 'vue'

const emit = defineEmits<{
  colorChange: [color: { r: number; g: number; b: number; hex: string }]
}>()

// Expanded state
const isExpanded = ref(false)

// HSV color model
const hue = ref(0) // 0-360
const saturation = ref(0) // 0-100
const value = ref(100) // 0-100

// Predefined colors
const presetColors = [
  '#FFFFFF', '#000000', '#FF0000', '#00FF00', '#0000FF',
  '#FFFF00', '#FF00FF', '#00FFFF', '#FFA500', '#800080'
]

/**
 * Convert HSV to RGB
 */
const hsvToRgb = (h: number, s: number, v: number): { r: number; g: number; b: number } => {
  s = s / 100
  v = v / 100
  
  const c = v * s
  const x = c * (1 - Math.abs(((h / 60) % 2) - 1))
  const m = v - c
  
  let r = 0, g = 0, b = 0
  
  if (h >= 0 && h < 60) {
    r = c; g = x; b = 0
  } else if (h >= 60 && h < 120) {
    r = x; g = c; b = 0
  } else if (h >= 120 && h < 180) {
    r = 0; g = c; b = x
  } else if (h >= 180 && h < 240) {
    r = 0; g = x; b = c
  } else if (h >= 240 && h < 300) {
    r = x; g = 0; b = c
  } else {
    r = c; g = 0; b = x
  }
  
  return {
    r: Math.round((r + m) * 255),
    g: Math.round((g + m) * 255),
    b: Math.round((b + m) * 255)
  }
}

/**
 * Convert RGB to hex
 */
const rgbToHex = (r: number, g: number, b: number): string => {
  return '#' + [r, g, b].map(x => {
    const hex = x.toString(16)
    return hex.length === 1 ? '0' + hex : hex
  }).join('').toUpperCase()
}

/**
 * Current color in RGB
 */
const currentColor = computed(() => {
  const rgb = hsvToRgb(hue.value, saturation.value, value.value)
  return {
    ...rgb,
    hex: rgbToHex(rgb.r, rgb.g, rgb.b)
  }
})

/**
 * Emit color change
 */
const emitColorChange = () => {
  emit('colorChange', currentColor.value)
}

/**
 * Set color from preset
 */
const setPresetColor = (hexColor: string) => {
  const r = parseInt(hexColor.substring(1, 3), 16)
  const g = parseInt(hexColor.substring(3, 5), 16)
  const b = parseInt(hexColor.substring(5, 7), 16)
  
  // Convert RGB to HSV
  const rNorm = r / 255
  const gNorm = g / 255
  const bNorm = b / 255
  
  const max = Math.max(rNorm, gNorm, bNorm)
  const min = Math.min(rNorm, gNorm, bNorm)
  const delta = max - min
  
  // Value
  value.value = max * 100
  
  // Saturation
  saturation.value = max === 0 ? 0 : (delta / max) * 100
  
  // Hue
  if (delta === 0) {
    hue.value = 0
  } else if (max === rNorm) {
    hue.value = 60 * (((gNorm - bNorm) / delta) % 6)
  } else if (max === gNorm) {
    hue.value = 60 * (((bNorm - rNorm) / delta) + 2)
  } else {
    hue.value = 60 * (((rNorm - gNorm) / delta) + 4)
  }
  
  if (hue.value < 0) hue.value += 360
  
  emitColorChange()
}

// Gradient style for hue slider
const hueGradient = computed(() => {
  return 'linear-gradient(to right, #ff0000 0%, #ffff00 17%, #00ff00 33%, #00ffff 50%, #0000ff 67%, #ff00ff 83%, #ff0000 100%)'
})

// Gradient style for saturation/value preview
const saturationGradient = computed(() => {
  const baseColor = hsvToRgb(hue.value, 100, 100)
  const baseHex = rgbToHex(baseColor.r, baseColor.g, baseColor.b)
  return `linear-gradient(to right, #ffffff 0%, ${baseHex} 100%)`
})

const valueGradient = computed(() => {
  return 'linear-gradient(to right, #000000 0%, #ffffff 100%)'
})

/**
 * Toggle expanded state
 */
const toggleExpanded = () => {
  isExpanded.value = !isExpanded.value
}
</script>

<template>
  <div class="color-selector-wrapper">
    <!-- Color circle - always visible -->
    <div 
      class="color-circle"
      :style="{ backgroundColor: currentColor.hex }"
      @click="toggleExpanded"
      :title="isExpanded ? 'Close color picker' : 'Open color picker'"
    ></div>
    
    <!-- Expanded view: full color picker -->
    <div v-if="isExpanded" class="color-selector">
      <div class="color-header">
        <div class="color-display" :style="{ backgroundColor: currentColor.hex }">
          <span class="color-hex">{{ currentColor.hex }}</span>
        </div>
        <button class="close-button" @click="toggleExpanded">×</button>
      </div>
      
      <div class="slider-group">
        <label>Hue</label>
      <input 
        type="range" 
        min="0" 
        max="360" 
        v-model.number="hue"
        @input="emitColorChange"
        class="slider hue-slider"
        :style="{ background: hueGradient }"
      />
    </div>
    
    <div class="slider-group">
      <label>Saturation</label>
      <input 
        type="range" 
        min="0" 
        max="100" 
        v-model.number="saturation"
        @input="emitColorChange"
        class="slider saturation-slider"
        :style="{ background: saturationGradient }"
      />
    </div>
    
    <div class="slider-group">
      <label>Brightness</label>
      <input 
        type="range" 
        min="0" 
        max="100" 
        v-model.number="value"
        @input="emitColorChange"
        class="slider value-slider"
        :style="{ background: valueGradient }"
      />
    </div>
    
    <div class="preset-colors">
      <div class="preset-label">Presets:</div>
      <div class="preset-grid">
        <button
          v-for="color in presetColors"
          :key="color"
          class="preset-color"
          :style="{ backgroundColor: color }"
          @click="setPresetColor(color)"
        ></button>
      </div>
    </div>
    </div>
  </div>
</template>

<style scoped>
.color-selector-wrapper {
  position: relative;
  display: flex;
  align-items: center;
}

.color-circle {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: 2px solid rgba(255, 255, 255, 0.3);
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.3), inset 0 1px 2px rgba(0, 0, 0, 0.2);
  cursor: pointer;
  transition: all 0.2s ease;
}

.color-circle:hover {
  transform: scale(1.15);
  border-color: rgba(255, 255, 255, 0.6);
  box-shadow: 0 3px 10px rgba(0, 0, 0, 0.4), inset 0 1px 2px rgba(0, 0, 0, 0.2);
}

.color-circle:active {
  transform: scale(0.95);
}

.color-selector {
  position: absolute;
  bottom: 60px;
  left: 50%;
  transform: translateX(-50%);
  background-color: rgba(35, 35, 35, 0.98);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 12px;
  padding: 20px;
  width: 280px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.6), 0 0 0 1px rgba(255, 255, 255, 0.05);
  backdrop-filter: blur(10px);
}

.color-header {
  display: flex;
  gap: 10px;
  align-items: stretch;
  margin-bottom: 15px;
}

.color-display {
  flex: 1;
  height: 60px;
  border-radius: 4px;
  border: 2px solid #444;
  display: flex;
  align-items: flex-end;
  justify-content: center;
  padding-bottom: 5px;
}

.close-button {
  width: 30px;
  background-color: #333;
  color: #fff;
  border: 1px solid #555;
  border-radius: 4px;
  cursor: pointer;
  font-size: 24px;
  line-height: 1;
  transition: background-color 0.2s;
  padding: 0;
}

.close-button:hover {
  background-color: #444;
}

.close-button:active {
  background-color: #222;
}

.color-hex {
  background-color: rgba(0, 0, 0, 0.5);
  color: white;
  padding: 4px 8px;
  border-radius: 3px;
  font-family: monospace;
  font-size: 14px;
  font-weight: bold;
}

.slider-group {
  margin-bottom: 12px;
}

.slider-group label {
  display: block;
  color: #ccc;
  font-size: 12px;
  margin-bottom: 5px;
  font-weight: 500;
}

.slider {
  width: 100%;
  height: 20px;
  border-radius: 10px;
  outline: none;
  -webkit-appearance: none;
  appearance: none;
  cursor: pointer;
  border: 1px solid #555;
}

.slider::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 18px;
  height: 18px;
  border-radius: 50%;
  background: white;
  border: 2px solid #333;
  cursor: pointer;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
}

.slider::-moz-range-thumb {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  background: white;
  border: 2px solid #333;
  cursor: pointer;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
}

.preset-colors {
  margin-top: 15px;
  padding-top: 15px;
  border-top: 1px solid #444;
}

.preset-label {
  color: #ccc;
  font-size: 12px;
  margin-bottom: 8px;
  font-weight: 500;
}

.preset-grid {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 6px;
}

.preset-color {
  width: 100%;
  aspect-ratio: 1;
  border: 2px solid #555;
  border-radius: 4px;
  cursor: pointer;
  transition: transform 0.1s, border-color 0.1s;
  padding: 0;
}

.preset-color:hover {
  transform: scale(1.1);
  border-color: #888;
}

.preset-color:active {
  transform: scale(0.95);
}
</style>
