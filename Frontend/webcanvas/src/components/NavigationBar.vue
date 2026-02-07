<template>
  <nav class="navigation-bar">
    <div class="nav-container">
      <div class="nav-brand">
        <h1>WebCanvas</h1>
      </div>
      <div class="nav-links">
        <a href="#canvas" class="nav-link">Canvas</a>
        <a href="#about" class="nav-link">About</a>
      </div>
    </div>
  </nav>
</template>

<script setup lang="ts">
</script>

<style scoped>
.navigation-bar {
  background-color: #2c3e50;
  padding: 1rem 0;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.nav-container {
  margin: 0 auto;
  padding: 0 2rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 40px;
}

.nav-brand h1 {
  color: #ffffff;
  font-size: 1.5rem;
  margin: 0;
  font-weight: 600;
}

.nav-links {
  display: flex;
  gap: 2rem;
}

.nav-link {
  color: #ecf0f1;
  text-decoration: none;
  font-size: 1rem;
  font-weight: 500;
  transition: color 0.3s ease;
}

.nav-link:hover {
  color: #3498db;
}
</style>
