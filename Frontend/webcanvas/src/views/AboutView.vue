<template>
  <div class="about">
    <h1>Distributed Systems: WebCanvas</h1>
    <section class="project-attribution">
      <p>
        <strong class="highlighted">Notice on AI Assistance:</strong> 
        Large Language Models (LLMs), including Claude 4.5, ChatGPT, and Gemini, were utilized as pair-programming collaborators to assist in the development and documentation of this project. 
      </p>
      
      <p>
        <strong>Division of Labor:</strong> 
        The core system architecture and the implementation of high-complexity distributed systems—specifically <strong>Veritas</strong> and <strong>NoReDB</strong> (Rust), and the <strong>Partitioning Controller</strong> (Go) were authored entirely by the developer. 
        LLMs were primarily used to generate boilerplate code, technical documentation, and generic frontend components (Vue.js and SignalR). In this workflow, the models generated code based on the developer's strict specifications, which was then reviewed, refactored, and integrated manually to ensure it met project standards.
      </p>

      <hr />

      <p class="description">
        <strong>WebCanvas</strong> is a collaborative online drawing application built using Vue.js for the frontend and SignalR for real-time communication. 
        It allows multiple users to draw on a shared canvas simultaneously, with changes reflected in real-time across all connected clients. 
        The project was implemented for a University of Applied Sciences to demonstrate proficiency in real-time collaboration, system design, distributed databases, and modern web technologies.
      </p>
    </section>
    <section class="features">
      <h2>Features</h2>
      <ul>
        <li>Real-time collaborative drawing</li>
        <li>Color selection with HSL sliders and preset colors</li>
        <li>Responsive design for desktop and mobile devices</li>
        <li>Built with Vue.js and SignalR for seamless real-time updates</li>
        <li>Demonstrates real-time collaboration, system design, and distributed databases</li>
        <li>Persistent canvas state stored in a distributed quorum based database for durability and scalability</li>
        <li>Deployed on Kubernetes for scalability and reliability</li>
      </ul>
    </section>
    <section class="technologies">
      <h2>Technologies Used</h2>
      <ul>
        <li><strong>Vue.js:</strong> A progressive JavaScript framework for building user interfaces.</li>
        <li><strong>SignalR:</strong> A library for ASP.NET that enables real-time web functionality.</li>
        <li><strong>TypeScript:</strong> A typed superset of JavaScript that compiles to plain JavaScript.</li>
        <li><strong>CSS:</strong> Used for styling the application and ensuring a responsive design.</li>
        <li><strong>HTML:</strong> The standard markup language for creating web pages and applications.</li>
        <li><strong>Kubernetes:</strong> An open-source system for automating the deployment, scaling, and management of containerized applications.</li>
        <li><strong>Docker:</strong> A platform for developing, shipping, and running applications in containers.</li>
        <li><strong>ASP.NET Core:</strong> A cross-platform framework for building modern, cloud-based, and internet-connected applications.</li>
        <li><strong>Go:</strong> A statically typed, compiled programming language designed for simplicity and performance.</li>
        <li><strong>Rust:</strong> A systems programming language focused on safety, speed, and concurrency.</li>
      </ul>
    </section>

    <section class="development-process">
      <h2>Development Process</h2>
      <h3>Project Planning and Design</h3>
      <p>
        Whether working solo or in small teams on educational, personal, or professional contracts, I have explored various methodologies including Agile, Scrum, <a href="https://publications.lib.chalmers.se/records/fulltext/147143.pdf" target="_blank">Agile Solo</a>, and <a href="https://arxiv.org/pdf/2209.14263" target="_blank">GLUX</a>. 
        While I am a strong advocate for Agile principles, I’ve found that the administrative burden of formal frameworks often outweighs the benefits in a solo context. This led me to adopt a lean, self-directed approach.
      </p>

      <div class="challenges-opportunities">
        <h4>Unique Solo Dynamics</h4>
        <ul>
          <li><strong>Autonomous Scheduling:</strong> Minimal coordination needs allow for extreme flexibility, though it demands high self-discipline.</li>
          <li><strong>Resource Constraints:</strong> As the sole developer, time is the most finite resource; every hour spent on administration is an hour taken from development.</li>
          <li><strong>Streamlined Execution:</strong> Without dependencies on others, the "speed of thought" can more closely match the "speed of code."</li>
        </ul>
      </div>

      <div class="adaptation-flexibility">
        <h4>Adaptation and Flexibility</h4>
        <p>
          Traditional time estimation is primarily a coordination tool. In solo projects, precise estimates are often unjustifiable because there are no external dependencies to synchronize. 
          Forcing fixed-size sprints can create artificial pressure that leads to burnout rather than productivity. Instead, I focus on <strong>Adaptive Workflow</strong>—addressing the most critical tasks without the constraints of a rigid schedule.
        </p>

        <p>
          To support this, I utilize the <a href="https://learn.microsoft.com/en-us/azure/devops/boards/work-items/guidance/choose-process?view=azure-devops&tabs=agile-process" target="_blank">Azure DevOps Basic Process</a>. 
          This provides a lightweight framework for tracking progress without excessive overhead:
        </p>

        <ul>
          <li><strong>Backlog of Epics:</strong> High-level goals are defined as Epics. I use rough Story Point estimates solely to provide stakeholders with a general sense of project scope.</li>
          <li><strong>Goal-Oriented Phases:</strong> Rather than traditional time-boxed sprints, I work in "Phases" (e.g., Design, Backend, Frontend). These are flexible periods focused on achieving specific milestones.</li>
          <li><strong>Just-in-Time Refinement:</strong> I only break Epics into specific <em>Issues</em> and <em>Tasks</em> when I am actively working on that Phase. This prevents "planning rot" and ensures tasks reflect the current state of the project.</li>
        </ul>

        <p>
          This approach excels at handling emerging requirements. When new challenges or opportunities arise—as they often do during development—I simply add them to the active Phase and prioritize them immediately. 
          By maintaining this flexibility, I can adapt to customer feedback and technical discoveries far more effectively than a fixed plan would allow.
        </p>

        <strong>Summary:</strong> By reducing administrative friction, I maximize "maker time." This adaptive approach ensures that the project remains responsive to change while maintaining a steady, sustainable velocity.
      </div>
    </section>

    <section class="project-structure">
      <div class="project-structure">
        <h2>Project Structure</h2>
        <ul>
          <li>The project is organized into a frontend, backendstateless and backendstateful.</li>
          <li>The frontend is built with Vue.js and includes components for the canvas, color selector, and navigation bar.</li>
          <li>The backendstateless is implemented in C# and Go and handles real-time communication using SignalR and GRPC.</li>
          <li>The backendstateful is implemented in Rust and manages the persistent canvas state using a distributed quorum based database.</li>
        </ul>
      </div>

      <div class="system-overview">
        <h4>System Overview</h4>
        <p>
          The frontend Vue.js application connects to the backend using SignalR for real-time updates. 
          When a user draws on the canvas, the frontend sends pixel data to the backend, which then broadcasts it to all connected clients. 
          The backend also persists the canvas state in a distributed database to ensure durability and scalability. 
          The entire system is deployed on Kubernetes to handle varying loads and ensure high availability.
        </p>

        <strong>Key Components:</strong>
        <ul>
          <li><strong>Vue.js - Frontend:</strong> Handles the user interface and interactions, including the canvas, color selector, and navigation bar.</li>
          <li><strong>ASP.NET - Backend and SignalR:</strong> Manages real-time communication between the frontend and backend and cache invalidation between Backend instances.</li>
          <li><strong>Go - Partitioning Controller:</strong> Manages the distribution of canvas state across multiple backend instances to ensure consistency and fault tolerance.</li>
          <li><strong>Rust - NoReDB:</strong> Implements the distributed quorum based database for persistent canvas state storage.</li>
          <li><strong>Rust - Veritas:</strong> Handles consensus and coordination for the distributed system.</li>
        </ul>

        <div class="interactive-diagram"> 
          <h4 id="system-diagram">Interactive System Diagram</h4>
          <p>
            Below is a minimalistic interactive diagram illustrating the distributed architecture of WebCanvas. 
            Hover over each component to see detailed statistics including language, instance count, and role.
            Click to jump to relevant sections. The diagram shows the complete distributed system with HPA-managed services and consensus coordination.
          </p>
          
          <InteractiveDiagram
            :nodes="diagramNodes"
            :connections="diagramConnections"
            :hpa-groups="hpaGroups"
          />
        </div>
      </div>
    </section>

    <!-- Component Details Sections -->
    <section id="client-web" class="component-detail">
      <h2>Web Client</h2>
      <p>
        The web client is a Vue.js application accessible from any modern browser on desktop, laptop, mobile, or tablet devices. 
        The responsive interface adapts seamlessly between mouse-based and touch-based interaction.
      </p>
      
      <h3>SignalR Hub Connection</h3>
      <p>
        Upon loading, the client establishes a persistent WebSocket connection to the ASP.NET Backend's SignalR hub. 
        This bidirectional connection enables real-time communication with sub-millisecond latency, allowing instantaneous 
        synchronization of drawing actions across all connected clients.
      </p>
      
      <h3>Pixel-Level Canvas Updates</h3>
      <p>
        When a user draws on the canvas, the application captures each pixel modification event:
      </p>
      <ul>
        <li><strong>Sending Updates:</strong> The client transmits pixel data (coordinates, color values) to the SignalR hub immediately as the user draws</li>
        <li><strong>Receiving Updates:</strong> The client listens for incoming pixel updates from other users via the hub and renders them in real-time</li>
        <li><strong>Local Rendering:</strong> Changes are applied to the HTML5 canvas element instantly for responsive user feedback</li>
      </ul>
      
      <h3>Interface Features</h3>
      <ul>
        <li>Full-screen canvas with mouse and touch support</li>
        <li>HSL color picker with preset color palette</li>
        <li>Brush size adjustment</li>
        <li>Responsive layout adapting to all screen sizes</li>
        <li>Real-time connection status indicator</li>
      </ul>
    </section>

    <section id="aspnet-backend" class="component-detail">
      <h2>ASP.NET Backend (SignalR Hub)</h2>
      <p>
        The ASP.NET Core backend serves as the central real-time communication hub for WebCanvas. Running as a horizontally 
        scalable service with 2-10 pods managed by Kubernetes HPA, each instance handles WebSocket connections from multiple 
        clients simultaneously while coordinating with the distributed database layer for persistence.
      </p>

      <h3>Architecture Overview</h3>
      <p>
        As illustrated in Figure 1 below, the backend architecture follows a multi-layered approach where clients connect via WebSocket to the SignalR hub. 
        When a pixel update arrives, the backend first persists it to the distributed database via the Partitioning Controller using gRPC, 
        then broadcasts the update to all connected clients. When multiple backend instances scale horizontally, 
        a custom in-memory cache ensures all instances broadcast updates to their respective clients, maintaining consistency 
        across the distributed system.
      </p>
      <div class="diagram-container">
        <ScalableSvg :src="AspNetBackendComponents" alt="ASP.NET Backend Components Diagram" />
        <p class="diagram-caption">Figure 1: ASP.NET Backend component architecture and communication flows</p>
      </div>

      <h3>SignalR Hub: Real-Time Communication</h3>
      <p>
        The SignalR hub manages WebSocket connections and message broadcasting for the collaborative canvas demonstration. 
        Each client establishes a persistent WebSocket connection to a specific backend instance, where the hub tracks all active 
        connections. When a client sends pixel update data, the hub first ensures the data is persisted to the database, then 
        broadcasts the update to all other connected clients on the same instance. Built-in reconnection logic handles temporary 
        network disruptions transparently. The canvas serves as a demonstration vehicle to showcase the distributed database's 
        capabilities in handling real-time, concurrent updates.
      </p>

      <h3>Multi-Instance Coordination</h3>
      <p>
        Figure 2 below illustrates the sequence of operations when multiple backend instances run concurrently under HPA scaling. 
        After a backend instance successfully persists a pixel update to the database, it uses a custom in-memory cache to notify 
        other backend instances (B, C, etc.) to broadcast the same update to their connected clients. This architecture ensures 
        all clients see updates regardless of which instance they're connected to, while no single instance holds authoritative 
        state—all coordinate through the Partitioning Controller and NoReDB cluster.
      </p>
      <div class="diagram-container">
        <ScalableSvg :src="AspNetBackendSequenceDiagram" alt="ASP.NET Backend Sequence Diagram" />
        <p class="diagram-caption">Figure 2: Multi-instance coordination sequence showing cache invalidation flow</p>
      </div>

      <h3>gRPC Communication with Partitioning Controller</h3>
      <p>
        For persistence, the backend communicates with the Partitioning Controller via gRPC as shown in the diagrams above. 
        When pixel updates arrive, the pixel data is sent to the Partitioning Controller using Protocol Buffers for efficient 
        serialization. The backend waits for database write confirmation before broadcasting updates to clients and peers, ensuring data durability. 
        Multiple pixel updates are batched within a time window before transmission to optimize invalidation call overhead.
        If the Partitioning Controller becomes unavailable, Kubernetes automatically manages failover through its HPA and Service 
        mechanisms, routing requests to healthy controller instances while maintaining real-time functionality for connected clients.
      </p>

      <h3>Service Registration with Veritas</h3>
      <p>
        Each backend instance registers itself with the Veritas consensus cluster for service discovery and health monitoring. 
        On startup, the instance sends a registration request to any Veritas follower, which forwards it to the leader. 
        Periodic heartbeats confirm the instance is alive; missed heartbeats trigger automatic deregistration. Load balancing 
        is handled through Kubernetes Service mechanisms or via random selection. During graceful shutdown, the instance 
        deregisters itself before closing connections, allowing clients to reconnect to healthy instances.
      </p>
    </section>

    <section id="partitioning-controller" class="component-detail">
      <h2>Partitioning Controller</h2>
      <p>Content coming soon...</p>
    </section>

    <section id="veritas-leader" class="component-detail">
      <h2>Veritas Leader</h2>
      <p>Content coming soon...</p>
    </section>

    <section id="veritas-followers" class="component-detail">
      <h2>Veritas Followers</h2>
      <p>Content coming soon...</p>
    </section>

    <section id="noredb-cluster" class="component-detail">
      <h2>NoReDB Cluster</h2>
      <p>Content coming soon...</p>
    </section>
  </div>
</template>

<script setup lang="ts">
import InteractiveDiagram from '@/components/InteractiveDiagram.vue'
import ScalableSvg from '@/components/ScalableSvg.vue'
import AspNetBackendSequenceDiagram from '@/assets/AspNetBackendSequenceDiagram.drawio.svg'
import AspNetBackendComponents from '@/assets/AspNetBackendComponents.drawio.svg'

const diagramNodes = [
  // Client Devices
  {
    id: 'user-pc',
    title: 'Desktop',
    description: 'Desktop browser client accessing WebCanvas via WebSocket connection to backend',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect x="2" y="3" width="20" height="14" rx="2" fill="none" stroke="currentColor" stroke-width="2"/><path d="M8 21h8M12 17v4" stroke="currentColor" stroke-width="2"/></svg>',
    position: { x: 0, y: 120 },
    targetSection: 'client-web',
    nodeType: 'Client Device',
    count: 1
  },
  {
    id: 'user-mobile',
    title: 'Mobile',
    description: 'Mobile smartphone client accessing WebCanvas with touch interface',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect x="6" y="2" width="12" height="20" rx="2" fill="none" stroke="currentColor" stroke-width="2"/><circle cx="12" cy="19" r="1" fill="currentColor"/></svg>',
    position: { x: 0, y: 350 },
    targetSection: 'client-web',
    nodeType: 'Client Device',
    count: 1
  },
  
  // Backend Services (C# ASP.NET) - 3 instances in HPA
  {
    id: 'backend',
    title: 'ASP.NET Backend',
    description: 'Handles real-time WebSocket connections via SignalR, broadcasts pixel updates, manages cache invalidation between instances',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2"/><path d="M2 12h20M12 2c-2.5 4-2.5 16 0 20M12 2c2.5 4 2.5 16 0 20" stroke="currentColor" stroke-width="2" fill="none"/></svg>',
    position: { x: 250, y: 240 },
    targetSection: 'aspnet-backend',
    count: 3,
    nodeType: 'API Service',
    language: 'C# ASP.NET Core',
    scaling: 'Kubernetes HPA (2-10 pods)'
  },
  
  // Partitioning Controllers (Go) - 3 instances in HPA
  {
    id: 'controller',
    title: 'Partitioning Controller',
    description: 'Manages canvas state distribution and partitioning logic, routes requests to appropriate NoReDB instances based on hash partitioning',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 2L2 7v10c0 5 10 7 10 7s10-2 10-7V7z" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 12v10M12 12L2 7M12 12l10-5" stroke="currentColor" stroke-width="2"/></svg>',
    position: { x: 520, y: 240 },
    targetSection: 'partitioning-controller',
    count: 3,
    nodeType: 'Service Router',
    language: 'Go',
    scaling: 'Kubernetes HPA (2-8 pods)'
  },
  
  // Veritas Nodes (Rust) - 5 instances with leader
  {
    id: 'veritas-leader',
    title: 'Veritas Leader',
    description: 'Consensus leader node using custom protocol for service registration, health checking, and coordination. Maintains distributed system state',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 2l2 7h7l-5.5 4.5L17 21l-5-4-5 4 1.5-7.5L3 9h7z" fill="currentColor"/></svg>',
    position: { x: 785, y: 100 },
    targetSection: 'veritas-leader',
    count: 1,
    nodeType: 'Consensus Leader',
    language: 'Rust',
    scaling: 'StatefulSet (5 replicas)'
  },
  {
    id: 'veritas-followers',
    title: 'Veritas Followers',
    description: 'Follower nodes participating in custom consensus protocol, providing redundancy and automatic leader failover',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2"/><circle cx="12" cy="12" r="4" fill="currentColor"/></svg>',
    position: { x: 785, y: 310 },
    targetSection: 'veritas-followers',
    count: 4,
    nodeType: 'Consensus Follower',
    language: 'Rust',
    scaling: 'StatefulSet (5 replicas)'
  },
  
  // NoReDB Instances (Rust) - 9 instances
  {
    id: 'noredb',
    title: 'NoReDB Cluster',
    description: 'Distributed quorum-based database. Write-N:3 replicas per partition, Read-M:2 for quorum. Each partition maps to 3 random instances from the cluster',
    icon: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2" fill="none" stroke="currentColor" stroke-width="2"/><path d="M3 9h18M3 15h18M9 3v18" stroke="currentColor" stroke-width="2"/></svg>',
    position: { x: 1100, y: 240 },
    targetSection: 'noredb-cluster',
    count: 9,
    nodeType: 'Database',
    language: 'Rust',
    scaling: 'StatefulSet (9 replicas)'
  }
]

const hpaGroups = [
  {
    id: 'backend-hpa',
    label: 'HPA: ASP.NET Backend',
    x: 215,
    y: 210,
    width: 240,
    height: 210
  },
  {
    id: 'controller-hpa',
    label: 'HPA: Partitioning Controllers',
    x: 485,
    y: 210,
    width: 240,
    height: 210
  },
  {
    id: 'veritas-cluster',
    label: 'Veritas Consensus Cluster',
    x: 750,
    y: 60,
    width: 240,
    height: 420
  }
]

const diagramConnections = [
  // Clients to Backends (WebSocket) - main path highlighted
  { from: 'user-pc', to: 'backend', label: 'WebSocket', bold: true },
  { from: 'user-mobile', to: 'backend', label: 'WebSocket', bold: true },
  
  // Backend to Controllers (gRPC)
  { from: 'backend', to: 'controller', label: 'gRPC', bold: true },
  
  // Backend registers with Veritas followers (they forward to leader)
  { from: 'backend', to: 'veritas-followers', label: 'Register', dashed: true, faded: true },
  
  // Controllers register with Veritas followers (they forward to leader)
  { from: 'controller', to: 'veritas-followers', label: 'Register', dashed: true, faded: true },
  
  // Veritas followers forward to leader (consensus protocol)
  { from: 'veritas-followers', to: 'veritas-leader', label: 'Forward', dashed: true, faded: true },
  { from: 'veritas-leader', to: 'veritas-followers', label: 'Consensus' },
  
  // Veritas to NoReDB (coordination and registration)
  { from: 'veritas-leader', to: 'noredb', label: '', faded: true },
  { from: 'veritas-followers', to: 'noredb', label: '', faded: true },
  
  // Controller to NoReDB (data operations - main path)
  { from: 'controller', to: 'noredb', label: 'W:3 / R:2', bold: true },

  // Clock connections
  { from: 'veritas-followers', to: 'controller', label: 'Global Clock', dashed: true, faded: false },
]
</script>

<style>
.about {
  margin: 1rem auto;
  padding: 0 2rem;
  max-width: 1280px;
  color: #ffffff;
  overflow: scroll;
  height: calc(100% - 72px);
  padding-bottom: 2em;
  scrollbar-width: none;
}

p {
  margin-bottom: 1.5rem;
}

.description {
  margin-top: 1rem;
  font-size: 1.1rem;
  line-height: 1.6;
}

h2 {
  font-size: 1.5rem;
  margin-top: 2rem;
  margin-bottom: 0.5rem;
}

h3 {
  font-size: 1.2rem;
  margin-top: 1.5rem;
  margin-bottom: 0.5rem;
}

h4 {
  font-size: 1.1rem;
  margin-top: 1.5rem;
  margin-bottom: 0.5rem;
}

strong {
  font-weight: bold;
}

.highlighted {
  color: #5a9efc;
}

a {
  color: #3498db;
  text-decoration: underline;
}

ul {
  margin-bottom: 1rem;
}

.interactive-diagram {
  margin-top: 2rem;
  margin-bottom: 2rem;
}

.component-detail {
  margin-top: 3rem;
  padding: 0rem;
  border-radius: 4px;
}

.component-detail h2 {
  margin-top: 0;
}

.diagram-container {
  margin: 2rem 0;
  text-align: center;
}

.diagram-caption {
  font-size: 0.9rem;
  font-style: italic;
  color: #b0c4de;
  margin-top: 0.5rem;
}

@media screen and (max-width: 600px) {
  .about {
    padding: 1rem;
    height: calc(100% - 64px);
  }

  h1 {
    font-size: 1.5rem;
  }

  h2 {
    font-size: 1.25rem;
  }
}

</style>
