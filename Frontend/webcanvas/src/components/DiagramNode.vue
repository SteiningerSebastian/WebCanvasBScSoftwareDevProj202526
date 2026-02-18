<template>
  <div 
    class="diagram-node"
    :style="{ left: position.x + 'px', top: position.y + 'px'}"
    :id="`node-${props.id}`"
    @mouseenter="handleMouseEnter"
    @mouseleave="handleMouseLeave"
    @click="handleClick"
  >
    <div class="node-content">
      <div>
        <div class="node-icon" v-html="icon"></div>
        <div class="node-title">{{ title }}</div>
        <span v-if="count && count > 1" class="node-badge">{{ count }}</span>
      </div>
    </div>
    
    <Transition name="tooltip">
      <div v-if="showTooltip" class="tooltip" :class="`tooltip-${tooltipPosition}`">
        <div class="tooltip-header">{{ title }}</div>
        <div class="tooltip-section">
          <div class="tooltip-label">Type:</div>
          <div class="tooltip-value">{{ nodeType }}</div>
        </div>
        <div v-if="language" class="tooltip-section">
          <div class="tooltip-label">Language:</div>
          <div class="tooltip-value">{{ language }}</div>
        </div>
        <div v-if="count && count > 1" class="tooltip-section">
          <div class="tooltip-label">Instances:</div>
          <div class="tooltip-value">{{ count }}</div>
        </div>
        <div v-if="scaling" class="tooltip-section">
          <div class="tooltip-label">Scaling:</div>
          <div class="tooltip-value">{{ scaling }}</div>
        </div>
        <div class="tooltip-section tooltip-description">
          <div class="tooltip-label">Description:</div>
          <div class="tooltip-value">{{ description }}</div>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'

interface Position {
  x: number
  y: number
}

interface Props {
  id: string
  title: string
  description: string
  icon: string
  position: Position
  targetSection?: string
  count?: number
  nodeType?: string
  language?: string
  scaling?: string
}

const props = defineProps<Props>()
const emit = defineEmits<{
  click: [id: string, targetSection?: string]
  hover: [id: string, isHovered: boolean]
}>()

const showTooltip = ref(false)

const tooltipPosition = computed(() => {
  // Check if tooltip would go off-screen and adjust
  const leftPos = props.position.x
  if (leftPos < 150) {
    return 'right'
  } else if (leftPos > 950) {
    return 'left'
  }
  return 'center'
})

const handleMouseEnter = () => {
  showTooltip.value = true
  emit('hover', props.id, true)
}

const handleMouseLeave = () => {
  showTooltip.value = false
  emit('hover', props.id, false)
}

const handleClick = () => {
  emit('click', props.id, props.targetSection)
  
  if (props.targetSection) {
    const element = document.getElementById(props.targetSection)
    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'start', inline: 'nearest' })
    }
  }
}
</script>

<style scoped>
.diagram-node {
  position: absolute;
  cursor: pointer;
  transition: transform 0.2s ease;
  z-index: 10;
  border: transparent solid 10px;
}

.diagram-node:hover {
  transform: scale(1.05);
  z-index: 20;
}

.node-content {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.5rem;
  background: transparent;
  border: none;
  border-radius: 8px;
  padding: 0.75rem;
  min-width: 150px;
  min-height: 130px;
}

.node-content div{
  vertical-align: middle;
  margin: auto;
}

.node-icon {
  width: 60px;
  height: auto;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 2rem;
  margin: 0 auto;
}

.node-icon :deep(svg) {
  width: 100%;
  height: auto;
  fill: #ffffff;
  stroke: #ffffff;
  display: block;
  margin: 0 auto;
}

.node-title {
  font-size: 0.75rem;
  font-weight: 600;
  color: #ffffff;
  text-align: center;
  line-height: 1.3;
  width: 100%;
  text-align: center;
  padding-top: 1rem;
}

.node-badge {
  position: absolute;
  top: -8px;
  right: -8px;
  background: #ffffff;
  color: #000000;
  border-radius: 50%;
  width: 22px;
  height: 22px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.7rem;
  font-weight: bold;
  border: 2px solid #000000;
}

.tooltip {
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  margin-top: 0.8rem;
  padding: 1rem;
  background: rgba(255, 255, 255, 0.98);
  border: 2px solid #ffffff;
  border-radius: 8px;
  color: #000000;
  font-size: 0.85rem;
  line-height: 1.5;
  min-width: 280px;
  max-width: 380px;
  white-space: normal;
  box-shadow: 0 4px 16px rgba(255, 255, 255, 0.2);
  pointer-events: none;
  z-index: 100;
}

.tooltip-left {
  left: auto;
  right: 100%;
  top: 50%;
  transform: translateY(-50%);
  margin-top: 0;
  margin-right: 0.8rem;
}

.tooltip-left::before {
  left: 100%;
  top: 50%;
  transform: translateY(-50%);
  border: 8px solid transparent;
  border-left-color: #ffffff;
  border-bottom-color: transparent;
}

.tooltip-right {
  left: 100%;
  right: auto;
  top: 50%;
  transform: translateY(-50%);
  margin-top: 0;
  margin-left: 0.8rem;
}

.tooltip-right::before {
  right: 100%;
  left: auto;
  top: 50%;
  transform: translateY(-50%);
  border: 8px solid transparent;
  border-right-color: #ffffff;
  border-bottom-color: transparent;
}

.tooltip::before {
  content: '';
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  border: 8px solid transparent;
  border-bottom-color: #ffffff;
}

.tooltip-header {
  font-size: 1rem;
  font-weight: bold;
  margin-bottom: 0.75rem;
  padding-bottom: 0.5rem;
  border-bottom: 2px solid #cccccc;
}

.tooltip-section {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
}

.tooltip-section.tooltip-description {
  flex-direction: column;
  gap: 0.25rem;
  margin-top: 0.75rem;
  padding-top: 0.75rem;
  border-top: 1px solid #cccccc;
}

.tooltip-label {
  font-weight: bold;
  min-width: 80px;
  color: #333333;
}

.tooltip-value {
  color: #000000;
}

.tooltip-enter-active,
.tooltip-leave-active {
  transition: opacity 0.2s ease;
}

.tooltip-enter-from,
.tooltip-leave-to {
  opacity: 0;
}

</style>
