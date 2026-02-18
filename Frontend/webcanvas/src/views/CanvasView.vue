<script setup lang="ts">
import Canvas from '../components/Canvas.vue'

</script>

<template>
  <main>
    <Canvas />
  </main>
</template>

<style scoped>
  main {
    display: flex;
    justify-content: center;
    align-items: center;
    height: calc(100dvh - 72px); 
    margin: 0;
  }
</style>