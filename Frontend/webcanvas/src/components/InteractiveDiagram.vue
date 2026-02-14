<template>
  <div class="interactive-diagram" ref="diagramContainer" :style="{ '--scale': scale }">
    <div class="diagram-content" :style="{ transform: `scale(${scale})`, transformOrigin: 'top left' }">
    <svg class="connections-layer" width="1300" height="auto" viewBox="0 0 1300 550">
      <defs>
        <marker
          id="arrowhead"
          markerWidth="10"
          markerHeight="10"
          refX="9"
          refY="3"
          orient="auto"
        >
          <polygon points="0 0, 10 3, 0 6" fill="#ffffff" opacity="1" />
        </marker>
      </defs>
      
      <!-- HPA Group Boxes -->
      <g v-for="group in hpaGroups" :key="group.id">
        <rect
          :x="group.x"
          :y="group.y"
          :width="group.width"
          :height="group.height"
          fill="none"
          stroke="#ffffff"
          stroke-width="2"
          stroke-dasharray="8,4"
          rx="20"
          ry="20"
          opacity="0.3"
        />
        <text
          :x="group.x + group.width / 2"
          :y="group.y - 5"
          class="hpa-label"
          text-anchor="middle"
        >
          {{ group.label }}
        </text>
      </g>
      
      <!-- Connections -->
      <g v-for="connection in connections" :key="`${connection.from}-${connection.to}`">
        <path
          :d="getConnectionPath(connection)"
          stroke="#ffffff"
          :stroke-width="connection.bold ? '3' : '1.5'"
          fill="none"
          :stroke-dasharray="connection.dashed ? '5,5' : 'none'"
          :opacity="connection.faded ? '0.1' : '0.6'"
          marker-end="url(#arrowhead)"
          :class="{ 
            'connection-active': activeNode === connection.from || activeNode === connection.to,
            'connection-hover': hoveredNode === connection.from || hoveredNode === connection.to
          }"
        />
        <text
          v-if="connection.label"
          :x="getConnectionLabelPosition(connection).x"
          :y="getConnectionLabelPosition(connection).y"
          class="connection-label"
          text-anchor="middle"
        >
          {{ connection.label }}
        </text>
      </g>
    </svg>
    
    <DiagramNode
      v-for="node in nodes"
      :key="node.id"
      :id="node.id"
      :title="node.title"
      :description="node.description"
      :icon="node.icon"
      :position="node.position"
      :target-section="node.targetSection"
      :count="node.count"
      :node-type="node.nodeType"
      :language="node.language"
      :scaling="node.scaling"
      @click="handleNodeClick"
      @hover="handleNodeHover"
    />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import DiagramNode from './DiagramNode.vue'

interface Position {
  x: number
  y: number
}

interface Node {
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

interface Connection {
  from: string
  to: string
  label?: string
  dashed?: boolean
  bold?: boolean
  faded?: boolean
}

interface HPAGroup {
  id: string
  label: string
  x: number
  y: number
  width: number
  height: number
}

interface Props {
  nodes: Node[]
  connections: Connection[]
  hpaGroups?: HPAGroup[]
}

const props = defineProps<Props>()

const diagramContainer = ref<HTMLElement | null>(null)
const activeNode = ref<string | null>(null)
const hoveredNode = ref<string | null>(null)
const containerWidth = ref(1300)

const scale = computed(() => {
  return containerWidth.value / 1300
})

const handleNodeClick = (nodeId: string, targetSection?: string) => {
  activeNode.value = nodeId
  setTimeout(() => {
    activeNode.value = null
  }, 1000)
}

const handleNodeHover = (nodeId: string, isHovered: boolean) => {
  hoveredNode.value = isHovered ? nodeId : null
}

const getNodeCenter = (nodeId: string): Position => {
  const node = props.nodes.find(n => n.id === nodeId)
  if (!node) return { x: 0, y: 0 }
  
  return {
    x: node.position.x + 85,
    y: node.position.y + 60
  }
}

// Calculate the point where a line intersects with the node border
const getNodeBorderPoint = (nodeId: string, fromCenter: Position): Position => {
  const center = getNodeCenter(nodeId)
  const dx = fromCenter.x - center.x
  const dy = fromCenter.y - center.y
  const angle = Math.atan2(dy, dx)

  const nodeWidth = 100
  const nodeHeight = 105
  const radiusX = nodeWidth / 2
  const radiusY = nodeHeight / 2
  
  // Calculate intersection with ellipse border
  const cos = Math.cos(angle)
  const sin = Math.sin(angle)
  const scale = Math.sqrt(1 / ((cos * cos) / (radiusX * radiusX) + (sin * sin) / (radiusY * radiusY)))
  
  return {
    x: center.x + cos * scale,
    y: center.y + sin * scale
  }
}

const getConnectionPath = (connection: Connection): string => {
  const fromCenter = getNodeCenter(connection.from)
  const toCenter = getNodeCenter(connection.to)
  
  // Calculate border points instead of using centers
  const from = getNodeBorderPoint(connection.from, toCenter)
  const to = getNodeBorderPoint(connection.to, fromCenter)
  
  // Create a curved path
  const dx = to.x - from.x
  const dy = to.y - from.y
  const controlPointOffset = Math.abs(dx) * 0.3

  if (connection.label === "Register" && connection.from === 'backend') {
    return `M ${from.x} ${from.y} Q ${from.x + controlPointOffset} ${from.y + dy*2.5}, ${to.x} ${to.y + 30}`
  } else if (connection.label === "Consensus") {
    return `M ${from.x+10} ${from.y+20} Q ${from.x + 10 + controlPointOffset} ${from.y + dy*0.5}, ${to.x+10} ${to.y}`
  } else if (connection.label === "Forward") {
    return `M ${from.x-10} ${from.y} Q ${from.x - 10 + controlPointOffset} ${from.y + dy*0.5-20}, ${to.x-10} ${to.y+20}`
  } else if (connection.label === "Global Clock") {
    return `M ${from.x} ${from.y+20} Q ${from.x + controlPointOffset*-2} ${from.y +20 + dy*0.5}, ${to.x} ${to.y+20}`
  }
  
  return `M ${from.x} ${from.y} Q ${from.x + controlPointOffset} ${from.y + dy / 2}, ${to.x} ${to.y}`
}

const getConnectionLabelPosition = (connection: Connection): Position => {
  const from = getNodeCenter(connection.from)
  const to = getNodeCenter(connection.to)

  if (connection.label == "W:3 / R:2") {
    return {
      x: (from.x + to.x) / 2 + 150,
      y: (from.y + to.y) / 2 -12
    }
  } else if (connection.label == "Consensus") {
    return {
      x: (from.x + to.x) / 2 + 50,
      y: (from.y + to.y) / 2 -10
    }
  }else if (connection.label == "Forward") {
    return {
      x: (from.x + to.x) / 2 -40,
      y: (from.y + to.y) / 2 +10
    }
  }else if (connection.label == "Register" && connection.from === 'backend') {
    return {
      x: (from.x + to.x) / 2+130,
      y: (from.y + to.y) / 2 +90
    }
  } else if (connection.label == "Register" && connection.from === 'controller') {
    return {
      x: (from.x + to.x) / 2,
      y: (from.y + to.y) / 2 -5
    }
  }else if (connection.label == "Global Clock") {
    return {
      x: (from.x + to.x) / 2 ,
      y: (from.y + to.y) / 2 +45
    }
  }
  
  return {
    x: (from.x + to.x) / 2 ,
    y: (from.y + to.y) / 2 - 12
  }
}

const updateContainerSize = () => {
  if (diagramContainer.value) {
    containerWidth.value = diagramContainer.value.offsetWidth
  }
}

onMounted(() => {
  updateContainerSize()
  window.addEventListener('resize', updateContainerSize)
})

onUnmounted(() => {
  window.removeEventListener('resize', updateContainerSize)
})
</script>

<style scoped>
.interactive-diagram {
  position: relative;
  width: 100%;
  background: transparent;
  overflow: visible;
  scrollbar-width: thin;
  padding: 0rem;
  height: calc(550px * var(--scale, 1));
}

.diagram-content {
  position: relative;
  width: 1300px;
  height: 550px;
  transition: transform 0.2s ease;
  overflow: visible;
}

.connections-layer {
  position: absolute;
  top: 0;
  left: 0;
  pointer-events: none;
  z-index: 1;
}

.connection-active {
  stroke: #ffffff !important;
  stroke-width: 3;
  opacity: 1 !important;
  animation: pulse 0.5s ease-in-out;
}

.connection-hover {
  stroke: #ffffff !important;
  opacity: 1 !important;
  stroke-width: 2.5;
  transition: all 0.2s ease;
}

.connection-label {
  font-size: 0.75rem;
  fill: #ffffff;
  font-weight: 600;
  pointer-events: none;
  opacity: 0.8;
}

.hpa-label {
  font-size: 0.8rem;
  fill: #ffffff;
  font-weight: bold;
  opacity: 0.7;
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.5;
  }
}
</style>
