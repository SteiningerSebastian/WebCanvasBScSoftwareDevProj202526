<template>
  <div class="page">
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

        <h3>Language Choice: C# and ASP.NET Core</h3>
        <p>
          C# and ASP.NET Core were chosen for the backend to leverage the mature ecosystem for building business logic and 
          real-time web applications. SignalR, which is tightly integrated with the ASP.NET framework, provides first-class 
          support for WebSocket management with features like automatic reconnection, connection grouping, and backplane integration 
          that would require significant custom development in other languages. The .NET runtime's managed memory model and 
          comprehensive standard library accelerate development of complex stateful services, while the framework's built-in 
          dependency injection, logging, and configuration systems reduce boilerplate. For an educational project focused on 
          distributed systems architecture rather than low-level performance optimization, C#'s productivity advantages and 
          SignalR's real-time capabilities make it the pragmatic choice for handling WebSocket connections and orchestrating 
          cache invalidation logic.
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
        <p>
          The Partitioning Controller is a Go-based service that manages the distribution of data across the NoReDB cluster. 
          Running as a horizontally scalable service with 2-8 pods under Kubernetes HPA, it implements a leader-follower architecture 
          where one instance acts as the leader responsible for partition management and assignment decisions, while follower instances 
          stand ready for automatic failover. The controller acts as an intelligent router, translating pixel coordinates into 
          partition IDs and directing read/write operations to the appropriate NoReDB instances based on the current partition mapping.
        </p>

        <h3>Language Choice: Go for Performance and Scalability</h3>
        <p>
          Go was selected for the Partitioning Controller due to its exceptional characteristics for building fast, lightweight 
          coordination services. The language's lightweight goroutines enable handling thousands of concurrent requests with minimal 
          memory overhead—critical when coordinating operations across multiple NoReDB instances and managing parallel reads and writes. 
          Go's fast compilation and small binary sizes (typically 10-20 MB) enable rapid container startup times, allowing Kubernetes 
          HPA to spin up new controller instances in seconds to meet demand spikes. The language's built-in concurrency primitives 
          (channels, goroutines, and the <code>sync</code> package) make it natural to express the controller's parallel operations—such 
          as broadcasting writes to multiple replicas or racing reads against a quorum—without complex threading libraries or callback 
          chains. Additionally, Go's strong static typing and simple error handling model reduce bugs in the critical path of data 
          operations, while its efficient garbage collector ensures predictable latency even under heavy load. For a component that must 
          scale elastically and route requests with minimal overhead, Go's performance-per-watt and operational simplicity make it ideal.
        </p>

        <h3>Understanding NoSQL and Distributed Data Storage</h3>
        <p>
          Before diving into the partitioning mechanics, it's essential to understand what NoReDB represents. NoSQL databases differ 
          fundamentally from traditional relational databases by eschewing fixed schemas and ACID guarantees in favor of horizontal 
          scalability and high availability. NoReDB (No-Relational Database) is a custom-built key-value store—the simplest form of 
          NoSQL database—where data is stored as simple key-value pairs rather than in structured tables with relationships. This 
          architectural choice enables massive scalability: instead of a single powerful server, the database can spread data across 
          many commodity servers, with each handling a subset of the total workload.
        </p>

        <div class="diagram-container">
          <ScalableSvg :src="GoPartitioningControllerComponents" alt="Partitioning Controller Components Diagram" />
          <p class="diagram-caption">Figure 3: Partitioning Controller architecture showing leader election, partition management, and NoReDB cluster coordination</p>
        </div>

        <h3>Partitioning: Dividing Data Across Nodes</h3>
        <p>
          Partitioning (also called sharding) is the technique of dividing a dataset into smaller, manageable chunks called partitions, 
          with each partition stored on different physical nodes. In WebCanvas, the e.g. 65,536 possible pixel coordinates (e.g. 256×256 canvas) 
          are divided into a configurable number of partitions. The controller uses a deterministic hash-based approach: each pixel's 16-bit 
          coordinates are hashed into a 32-bit key using <code>PixelToKey(x, y)</code>, then mapped to a partition using modulo arithmetic. 
          This ensures that the same pixel always maps to the same partition, regardless of which controller instance handles the request.
        </p>

        <p>
          The partition assignment strategy illustrated in Figure 3 shows how the system maintains a configuration mapping each partition 
          to multiple NoReDB instances. For example, partition 0 might be stored on NoReDB instances 1, 4, and 7, while partition 1 
          resides on instances 2, 5, and 8. This mapping is stored in Veritas and can be dynamically updated by the leader to rebalance 
          load or handle instance failures. The key advantage of this indirection is that partitions (logical divisions) remain fixed 
          while their assignment to physical nodes can change, enabling seamless scaling and failover without data reshuffling.
        </p>

        <h3>Quorum-Based Replication and Consistency</h3>
        <p>
          To ensure data durability and availability even when individual nodes fail, NoReDB uses quorum-based replication. Each partition 
          is replicated across multiple NoReDB instances (typically N=3 replicas). When writing data, the system requires confirmation 
          from W replicas (e.g., W=3 for strong consistency), and when reading, it queries R replicas (e.g., R=2), returning the value 
          with the latest timestamp. The quorum rule—where W + R &gt; N—guarantees that reads always overlap with the most recent writes, 
          ensuring consistency without requiring all replicas to be available simultaneously.
        </p>

        <p>
          The controller implements this through parallel operations as shown in the code below. For writes, it generates a hybrid logical 
          timestamp combining a global clock (synchronized via Veritas) and a local monotonic counter, ensuring total ordering of operations 
          across the distributed system. For reads, it performs read-repair: if replicas disagree on a value, the controller detects this 
          by comparing timestamps and writes the latest value back to outdated replicas, healing inconsistencies in the background.
        </p>

        <h3>Leader Election and Partition Management</h3>
        <p>
          The controller's leader election mechanism, visible in the <code>startLeaderElection</code> function, uses Veritas as a 
          coordination service. Each controller instance attempts to become leader by atomically updating a shared variable in Veritas 
          with its unique ID. The instance that successfully writes its ID becomes the leader and begins partition management duties. 
          The leader continuously monitors the health of NoReDB instances through Veritas service registration timestamps, automatically 
          removing instances that haven't sent heartbeats within a 5-minute window. When the cluster topology changes—nodes joining, 
          leaving, or failing—the leader recalculates partition assignments to maintain balance and replication guarantees.
        </p>

        <p>
          The rebalancing logic, triggered every 10 seconds as shown in <code>startLeadershipDuties</code>, calculates the current 
          partition distribution and checks for imbalance. If any NoReDB instance has 16 or more partitions than another, the leader 
          initiates rebalancing by invoking <code>transferPartition</code>, which streams all key-value pairs for a partition from the 
          old instance to the new one. This live migration happens transparently while the system continues serving requests, as the 
          controller maintains both old and new mappings during the transfer and only updates the authoritative configuration in Veritas 
          once migration completes.
        </p>

        <h3>Code Deep Dive: Quorum Writes</h3>
        <p>
          The <code>Set</code> method demonstrates the quorum write implementation. When a pixel update arrives, the controller first 
          determines the partition ID using <code>getPartition(x, y)</code>, which applies modulo arithmetic to the hashed coordinates. 
          It then retrieves the NoReDB clients responsible for that partition via <code>getClientsForPartition</code>, which looks up 
          the partition-to-instance mapping from the in-memory configuration cache. The controller generates a hybrid logical timestamp 
          by incrementing its local clock and capturing the current global clock value from Veritas:
        </p>

        <pre><code>func (pc *PartitioningController) getCurrentTimestamp() Timestamp {
      return Timestamp{
          LocalClock:  pc.local_time.Add(1),  // Increment local clock
          GlobalClock: pc.global_time.Load(), // Use current global clock
      }
  }</code></pre>

        <p>
          This timestamp is attached to the pixel data and sent in parallel to all replica instances using goroutines. Each write operation 
          runs asynchronously with a timeout, incrementing an atomic counter <code>completed_writes</code> upon success. A separate goroutine 
          waits for either the write quorum (W=3) to be achieved or the context timeout to expire. If quorum is reached, the operation 
          succeeds and the <code>doneChan</code> is closed to signal completion; otherwise, an error is sent to <code>errorChan</code>. 
          This pattern allows the controller to return success as soon as the minimum required replicas acknowledge the write, without 
          waiting for slower replicas, thus maintaining low latency while ensuring durability.
        </p>

        <h3>Design Philosophy: Specialization vs. Separation of Concerns</h3>
        <p>
          A fundamental tension in distributed systems design lies between two competing goals: optimizing for performance through 
          specialization, or optimizing for maintainability and reusability through separation of concerns. This project deliberately 
          chooses the latter approach, making it an educational case study in modular architecture rather than a production system 
          squeezed for every millisecond of performance. Throughout the codebase, you'll find design decisions that prioritize clean 
          abstractions and component independence over tightly optimized, application-specific solutions.
        </p>

        <p>
          In production systems, especially at scale, specialization often wins. Companies like Facebook and Google build databases 
          custom-tailored to specific workloads—a social graph database understands friend relationships natively, a time-series database 
          optimizes for append-heavy temporal data, and a real-time collaboration database might embed application logic directly into 
          the storage layer for minimal latency. These systems achieve incredible performance by sacrificing generality: they do one 
          thing exceptionally well but are difficult to repurpose. The database and application become intertwined, sharing data structures, 
          protocols, and assumptions that make them inseparable.
        </p>

        <p>
          WebCanvas takes the opposite approach. Each component—ASP.NET backends, Partitioning Controller, NoReDB, and Veritas—operates 
          as an independent service with clear, well-defined interfaces. NoReDB doesn't "know" it's storing canvas pixels; it's a generic 
          key-value store that could just as easily power a user profile service, a distributed cache, or a configuration management system. 
          The Partitioning Controller doesn't have WebCanvas-specific logic; it's a general partitioning and quorum coordination layer. 
          This modularity comes with costs: extra network hops for the global clock synchronization, separate timestamp management instead 
          of piggybacking on existing messages, and additional serialization overhead at service boundaries.
        </p>

        <h3>Concrete Example: Hybrid Clocks vs. Vector Clocks</h3>
        <p>
          The timestamp mechanism illustrates this philosophy concretely. The system uses hybrid logical clocks (global + local) managed 
          by the Partitioning Controller, rather than vector clocks embedded in cache invalidation messages between ASP.NET backend instances. 
          In a specialized approach, each backend would maintain a version vector and include it with every cache invalidation message, 
          allowing recipients to determine causal ordering without external coordination. This would eliminate the dependency on Veritas 
          for global clock distribution, reduce latency by a few milliseconds, and cut the number of network requests per operation.
        </p>

        <p>
          However, this optimization would create tight coupling between the cache layer and the database's consistency model. The ASP.NET 
          backends would need to understand timestamps, versioning, and conflict resolution—concerns that properly belong to the database 
          layer. If you wanted to swap NoReDB for a different storage backend, or reuse the caching infrastructure with a different database 
          that uses last-write-wins semantics instead of versioned values, you'd need to refactor significant portions of the backend code. 
          By centralizing timestamp generation in the Partitioning Controller, WebCanvas maintains a clean separation: the database provides 
          timestamped storage, the controller manages coordination, and the backends simply forward operations and invalidate caches based 
          on completion signals—they never need to understand how consistency is achieved.
        </p>

        <p>
          This separation enables independent evolution of each component. NoReDB can change its timestamp format, switch from hybrid clocks 
          to true vector clocks, or even implement a completely different consistency model without touching the ASP.NET backend code. 
          The Partitioning Controller could be replaced with a consistent hashing library or a different coordination service without 
          modifying the database. Each service has a single responsibility and communicates through stable, minimal interfaces. From an 
          educational perspective, this architecture teaches the value of abstraction boundaries and the tradeoffs inherent in distributed 
          system design: you can have maximum performance or maximum flexibility, but rarely both simultaneously.
        </p>

        <h3>Code Deep Dive: Quorum Reads with Repair</h3>
        <p>
          The <code>Get</code> method implements quorum reads with built-in read-repair. Similar to writes, it maps coordinates to a 
          partition and retrieves the responsible clients. It then issues parallel read requests to all replicas, with each spawned 
          goroutine tracking successful responses in a shared <code>readValues</code> slice protected by a mutex. Once R=2 responses 
          arrive, a goroutine analyzes the results using <code>getMostCurrentVersion</code>, which compares timestamps to identify the 
          latest value. If all responding replicas agree, the value is immediately returned. However, if replicas have diverged—indicating 
          previous partial failures—the controller triggers read-repair:
        </p>

        <pre><code>// If not all clients have the latest value, perform read repair
  for i, client := range clients {
      if clientRead[i] == 1 && /* client has stale data */ {
          // Write latest value back to stale replica
          client.Set(ctx, x, y, latestValue.Color, timestamp)
      }
  }</code></pre>

        <p>
          This automatic healing mechanism ensures that temporary network partitions or instance crashes don't leave permanent 
          inconsistencies in the system. By piggybacking repair on reads, the system converges toward consistency without requiring 
          separate background processes or anti-entropy protocols, a design choice that reduces operational complexity while maintaining 
          strong eventually-consistent semantics.
        </p>

        <h3>Service Registration and Discovery</h3>
        <p>
          As depicted in Figure 3, each Partitioning Controller instance registers itself with Veritas on startup through the 
          <code>handleServiceRegistration</code> function in <code>main.go</code>. The registration includes the instance's hostname 
          and Kubernetes-resolvable DNS address (<code>hostname.service-name.namespace.svc.cluster.local</code>), enabling dynamic 
          service discovery. A background goroutine updates this registration every 10 seconds with a fresh timestamp, allowing other 
          components to distinguish between active and failed instances. The leader uses this same mechanism to monitor NoReDB instance 
          health, automatically removing stale registrations after 5 minutes to prevent routing requests to dead nodes.
        </p>
      </section>

      <section id="veritas-consensus" class="component-detail">
        <h2>Veritas: Atomic Operations for Coordination</h2>
        <p>
          Veritas is a Rust-based coordination service that provides linearizable atomic operations to enable distributed coordination 
          across WebCanvas. Rather than implementing a full-featured consensus system like Raft or Paxos, Veritas focuses on a minimal 
          set of atomic primitives—<code>get</code>, <code>set</code>, <code>compare_set</code>, and <code>get_add</code>—that higher-level 
          components use to build coordination mechanisms. Running as a 5-node StatefulSet with leader-follower replication in Kubernetes, 
          Veritas maintains a replicated key-value store (a simple in-memory map) and ensures that these operations execute atomically and 
          in a totally ordered sequence, even in the presence of concurrent requests from multiple clients. Notably, these atomic operations 
          are only available after Veritas has successfully elected a leader—the leader election process itself uses a separate mechanism 
          based on heartbeats and voting rather than relying on the atomic operations it provides.
        </p>

        <p>
          Veritas's role in WebCanvas is specifically to provide coordination primitives for other components: the <strong>Partitioning Controller</strong> 
          (written in Go) uses Veritas's <code>get_add</code> operation to obtain unique global clock values for timestamp generation and uses 
          <code>compare_set</code> for its own leader election. The <strong>ASP.NET Backend</strong> instances register themselves with Veritas and 
          query it for service discovery. The <strong>NoReDB cluster</strong> stores partition metadata and configuration in Veritas. Each component 
          has distinct responsibilities: Veritas handles coordination primitives (leader-based), Partitioning Controller manages data distribution and 
          routing (leader-based for coordination), ASP.NET Backend handles business logic and WebSocket connections, and NoReDB stores the actual canvas 
          pixel data using a leaderless architecture for reads and writes.
        </p>

        <p>
          Importantly, Veritas assumes all participants are well-behaved and operates within a trusted system boundary. It does not defend 
          against Byzantine faults (malicious actors sending corrupt or contradictory messages), undefined behavior from buggy clients, or 
          adversarial attacks. This simplification is appropriate for an educational project where all components are controlled and audited, 
          allowing the implementation to focus on demonstrating core distributed systems principles without the complexity of Byzantine fault 
          tolerance. In a production environment serving untrusted clients, additional validation, authentication, and Byzantine-resistant 
          protocols would be necessary.
        </p>

        <div class="diagram-container">
          <ScalableSvg :src="VeritasComponents" alt="Veritas Components Diagram" />
          <p class="diagram-caption">Figure 4: Veritas architecture showing leader-follower replication and client interaction patterns</p>
        </div>

        <h3>Leader-Based vs. Leaderless Systems</h3>
        <p>
          Distributed systems face a fundamental design choice: use a leader-based architecture or distribute coordination across all nodes in 
          a leaderless fashion. <strong>Leader-based systems</strong> (like Veritas and the Partitioning Controller) designate one node as the 
          authoritative coordinator responsible for serializing operations, resolving conflicts, and maintaining global state. This simplifies 
          reasoning about consistency—all writes flow through a single point that can enforce a total ordering—but creates a potential bottleneck 
          and single point of failure. If the leader crashes, the system must pause operations and elect a new leader before resuming.
        </p>

        <p>
          <strong>Leaderless systems</strong> (like Dynamo, Cassandra, or Riak) distribute coordination across all nodes, allowing any node to 
          accept writes without consulting a leader. This eliminates the bottleneck and provides better availability during network partitions—even 
          if half the cluster is unreachable, the remaining nodes continue serving requests. However, leaderless systems sacrifice strong consistency: 
          concurrent writes to different replicas create conflicts that must be resolved later through techniques like last-write-wins, vector clocks, 
          or application-specific merge logic. Reads may return stale data or multiple conflicting versions that the application must reconcile.
        </p>

        <p>
          WebCanvas intentionally demonstrates both approaches to illustrate their tradeoffs in a single system. <strong>Veritas</strong> uses a leader-based 
          architecture for coordination primitives because operations like timestamp generation require strict total ordering and linearizability—having a 
          single leader serialize these operations is the simplest way to guarantee consistency. The <strong>Partitioning Controller</strong> also uses leader 
          election to coordinate partition assignments and routing decisions, ensuring all controllers have a consistent view of which NoReDB instances own 
          which data ranges.
        </p>

        <p>
          However, the <strong>NoReDB cluster</strong> uses a <strong>leaderless architecture</strong> for actual data reads and writes. When a Partitioning 
          Controller receives a pixel write request, it determines which partition owns that pixel and sends the write to multiple replicas in parallel 
          without consulting a leader. Replicas independently accept writes and use timestamps to resolve conflicts. Read requests similarly contact multiple 
          replicas in parallel, returning once a quorum responds. This leaderless design maximizes availability—even if some NoReDB instances are unreachable, 
          the system continues serving requests as long as a quorum is available—and eliminates the bottleneck of a single leader processing all data operations. 
          The tradeoff is that the Partitioning Controller must implement conflict resolution logic (using timestamps to determine the latest write) rather 
          than relying on a leader to serialize operations.
        </p>

        <p>
          This hybrid approach demonstrates a key distributed systems principle: different components can use different consistency models based on their 
          requirements. Coordination primitives (Veritas, Partitioning Controller) benefit from leader-based strong consistency, while high-throughput data 
          operations (NoReDB) benefit from leaderless availability and parallelism. By implementing both patterns, WebCanvas serves as an educational platform 
          showing when to choose each approach and how they can coexist in a single system.
        </p>

        <h3>Understanding ACID and Consistency Guarantees</h3>
        <p>
          Before exploring how Veritas works, it's essential to understand the consistency guarantees distributed systems strive to provide. 
          Traditional databases advertise <strong>ACID</strong> properties: <strong>Atomicity</strong> (operations complete fully or not at all), 
          <strong>Consistency</strong> (data moves from one valid state to another), <strong>Isolation</strong> (concurrent transactions don't 
          interfere), and <strong>Durability</strong> (committed data survives failures). While ACID provides strong guarantees for single-node 
          databases, distributed systems face additional challenges: network delays, partial failures, and concurrent operations across nodes 
          create scenarios where maintaining all ACID properties becomes prohibitively expensive or impossible.
        </p>

        <p>
          Veritas provides <strong>linearizability</strong>, the strongest single-object consistency model. A system is linearizable if every 
          operation appears to execute atomically at some point between its invocation and completion, and all operations can be ordered in a 
          sequence that respects real-time causality. In simpler terms: if operation A completes before operation B begins (according to real 
          wall-clock time), then the system state must reflect A's effects before B executes. This is stronger than eventual consistency (where 
          replicas converge "eventually" but may temporarily diverge) and stronger than serializability (which allows reordering operations as 
          long as the final result matches some serial execution, even if that order violates real-time causality). Linearizability is crucial 
          for coordination primitives like leader election and global clocks because it prevents anomalies where, for example, two nodes both 
          believe they are the leader simultaneously.
        </p>

        <h3>Total Ordering vs. Partial Ordering</h3>
        <p>
          Distributed systems must reason about the ordering of events across nodes that don't share a clock. A <strong>partial order</strong> 
          captures causality: if event A causally precedes event B (e.g., A sends a message that B receives), we write A → B. However, events 
          on different nodes with no causal relationship are <strong>concurrent</strong> and can't be ordered—we can't say whether A happened 
          "before" or "after" B in any meaningful sense. Vector clocks extend this partial order by tracking causality explicitly, allowing 
          any observer to determine if two events are causally related or concurrent.
        </p>

        <p>
          A <strong>total order</strong> goes further by imposing a strict sequence on all events, including concurrent ones. Every pair of 
          events has a defined ordering, even if they're causally unrelated. Veritas enforces total ordering by routing all writes through a 
          single leader that assigns each operation a monotonically increasing sequence number. This sequence number creates a total order: 
          operation with sequence 100 always precedes operation with sequence 101, regardless of which client issued them or when they were 
          initiated. This total ordering is critical for WebCanvas's timestamp generation—the global clock obtained via <code>get_add</code> 
          ensures no two writes across different Partitioning Controller instances can receive the same global timestamp, establishing a 
          global happens-before relationship across the entire distributed system.
        </p>

        <p>
          The hybrid timestamp scheme (global + local clock) creates a two-level total order. The global clock from Veritas ensures total 
          ordering <em>across nodes</em>: writes from different controllers are totally ordered by their global timestamps. The local clock 
          ensures total ordering <em>within a single node</em>: writes from the same controller are ordered by their local sequence. Together, 
          these form a composite key (global, local) that totally orders all writes system-wide, enabling conflict-free convergence even when 
          replicas temporarily diverge.
        </p>

        <h3>Atomic Operations: The Building Blocks</h3>
        <p>
          Veritas exposes four HTTP-based operations that higher-level components invoke. Figure 5 shows the sequence of operations for typical 
          get/set interactions. The <code>get(key)</code> operation retrieves the current value for a key from the leader's key-value store, 
          forwarding the request to the leader if invoked on a follower. The <code>set(key, value)</code> operation atomically updates a key-value 
          pair, first persisting the value to the leader's key-value store, then replicating to a quorum of followers who each persist to their 
          own key-value stores, and only then acknowledging success to the client. This ensures durability—the operation is not considered complete 
          until multiple replicas have persisted the value. Both operations are linearizable: the leader serializes all operations by processing 
          them sequentially and updating its in-memory map, and followers apply updates in the same order to maintain consistency.
        </p>

        <div class="diagram-container">
          <ScalableSvg :src="VeritasGetSetSequence" alt="Veritas Get/Set Sequence Diagram" />
          <p class="diagram-caption">Figure 5: Sequence diagram showing get/set operations with leader-follower replication</p>
        </div>

        <p>
          The <code>compare_set(key, expected, new_value)</code> operation is a compare-and-swap primitive: it atomically updates the value 
          only if the current value matches <code>expected</code>, persisting the new value to the leader's key-value store and replicating 
          to followers before returning true if successful, or returning false immediately if the comparison fails. This enables lock-free 
          synchronization—the Partitioning Controller (a separate component written in Go) uses compare_set for its own leader election: a 
          controller attempts to atomically claim leadership by writing its ID to a well-known key, succeeding only if no other controller has 
          claimed it first. The operation's atomicity ensures exactly one controller succeeds even if multiple attempt simultaneously.
        </p>

        <p>
          Most critically for WebCanvas, <code>get_add(key)</code> atomically reads the integer value at <code>key</code>, increments it by 1, 
          persists the new value to the leader's key-value store, replicates to followers (who also persist), and only then returns the old value. 
          Each Partitioning Controller instance calls <code>get_add("global_clock")</code> to obtain a unique global timestamp. Because the operation 
          is atomic and the leader serializes all <code>get_add</code> calls, no two invocations can return the same value—the global clock is strictly 
          monotonic across all controllers. As shown in the code below, the leader retrieves the current value from its key-value map, parses it as an 
          integer, increments it, persists the new value, replicates to followers, and only after all persistence completes returns the old value:
        </p>

        <pre><code>async fn get_add_for_leader(key: &str, data: web::Data&lt;AppState&lt;F&gt;&gt;) -> Result&lt;String, Error&gt; {
      // Ensure we have latest value from followers
      let current_value = Self::get_for_leader(key, data.clone()).await?;
      Self::set_if_newer(key, &current_value, &data.kv_store)?;

      // Read from key-value store (in-memory map)
      let mut transaction = data.kv_store.begin_transaction()?;
      let current_value = transaction.get(key).unwrap_or("0".to_string());
      
      let old_value = current_value.parse::&lt;i64&gt;().unwrap_or(0);
      let new_value = old_value + 1;
      
      // Persist to leader's key-value store
      transaction.set(key, &new_value.to_string())?;
      transaction.commit()?;
      
      // Replicate to followers (they persist to their key-value stores)
      Self::replicate_to_followers(&data, key, &new_value.to_string()).await?;
      
      // Return only after persistence completes
      Ok(old_value.to_string())
  }</code></pre>

        <h3>Leader Election and Replication</h3>
        <p>
          Veritas's leader election mechanism is separate from the atomic operations it provides—in fact, the atomic operations require an elected 
          leader to function. As illustrated in Figure 6, the election process uses health checks, candidate selection with loyalty preferences, 
          voting, and result publishing. Each node continuously runs an election cycle (typically every few seconds) where it first checks which 
          other nodes are reachable and healthy by sending heartbeat messages and awaiting responses. This health check establishes which nodes are 
          currently available to participate in consensus.
        </p>

        <p>
          After determining which nodes are reachable, each node independently selects its <strong>preferred candidate</strong> using a deterministic 
          algorithm: among all reachable nodes, prefer the one with the lowest ID, which ensures all nodes typically agree on the same candidate. 
          However, the algorithm includes a critical <strong>loyalty</strong> mechanism—if the current leader from the previous election cycle is 
          still reachable and healthy, the node remains loyal to it and continues supporting it as the preferred candidate. This loyalty prevents 
          unnecessary leadership changes when the current leader is functioning properly, maintaining stability even when network conditions fluctuate.
        </p>

        <p>
          Only nodes that believe <em>they themselves</em> are the preferred candidate initiate a vote request—other nodes remain passive. The 
          candidate broadcasts a vote request to all nodes, and each node grants its vote <strong>only if</strong> the requesting candidate matches 
          the preferred candidate it calculated independently. This ensures vote consistency: if all nodes see the same set of healthy nodes, they 
          all calculate the same preferred candidate and all grant their votes to that single candidate. The candidate counts the votes it receives; 
          if it collects votes from a majority (3 out of 5 nodes in WebCanvas's deployment), it has won the election.
        </p>

        <p>
          When a candidate wins the election, the protocol distinguishes between two scenarios. If the candidate is already the current leader 
          (re-elected in the current cycle), it immediately broadcasts the election result and continues processing operations. However, if there is 
          a <strong>leadership change</strong>—a new leader taking over from an old one—the new leader waits for one full election cycle period before 
          assuming leadership duties, giving the old leader time to step down gracefully. After this cooldown, the new leader asks for votes again to 
          confirm it still has majority support. Only after confirming the second vote does it officially assume leadership, update its internal state, 
          and broadcast the election result to all nodes. Other nodes <strong>accept</strong> the published result (not necessarily agreeing with the 
          decision, but acknowledging the leader's authority), updating their own internal leader ID accordingly.
        </p>

        <p>
          Once a leader is established, it begins processing atomic operations (<code>get</code>, <code>set</code>, <code>compare_set</code>, 
          <code>get_add</code>), serializing all client requests through its key-value store. Followers continue replicating updates from the leader, 
          maintaining consistent copies of the key-value map. This two-phase safety mechanism—health checks with loyalty, followed by a cooldown on 
          leadership transitions—ensures stable leadership while preventing split-brain scenarios where multiple nodes believe they are simultaneously 
          the leader.
        </p>

        <div class="diagram-container">
          <ScalableSvg :src="VeritasLeaderElectionSequence" alt="Veritas Leader Election Sequence" />
          <p class="diagram-caption">Figure 6: Leader election sequence using compare_set for atomic leadership claim</p>
        </div>

        <p>
          The leader replicates every write to a quorum of followers (typically 3 out of 5 nodes), waiting for each follower to persist the value 
          to its own key-value store, before acknowledging the client. This ensures durability—even if the leader immediately crashes after 
          acknowledging a write, the new leader will have the committed value in its key-value store and can continue serving it. Followers apply 
          updates in the order received from the leader, maintaining consistency. Read operations can be served by any node (reading from its local 
          key-value map), but only the leader processes writes, ensuring the total ordering property critical for linearizability.
        </p>

        <h3>Global Clock Generation</h3>
        <p>
          The Partitioning Controller maintains a cached copy of the global clock value and periodically refreshes it by calling Veritas's 
          <code>get_add</code> endpoint. When generating a timestamp for a write operation, the controller calls <code>getCurrentTimestamp()</code>, 
          which increments its local monotonic counter and combines it with the current cached global clock:
        </p>

        <pre><code>func (pc *PartitioningController) getCurrentTimestamp() Timestamp {
      return Timestamp{
          LocalClock:  pc.local_time.Add(1),  // Monotonic local sequence
          GlobalClock: pc.global_time.Load(), // Cached Veritas clock
      }
  }

  func (pc *PartitioningController) startUpdateGlobalTime(ctx context.Context) {
      ticker := time.NewTicker(MAX_CLOCK_TICK) // e.g., 100ms
      for {
          select {
          case &lt;-ticker.C:
              newGlobalTime := vClient.GetAdd(ctx, TIME_KEY) // Atomic get_add from Veritas
              pc.global_time.Store(newGlobalTime)
          case &lt;-ctx.Done():
              return
          }
      }
  }</code></pre>

        <p>
          This design minimizes the load on Veritas—controllers don't call <code>get_add</code> for every write, only every 100ms. The local 
          clock handles high-frequency writes within a single controller instance, while the global clock (refreshed periodically from Veritas) 
          ensures global ordering across controllers. The guarantee: if Controller A's global clock is 100 and Controller B's is 101, all writes 
          from A with global=100 happened-before all writes from B with global=101, regardless of the local clock values.
        </p>
      </section>

      <section id="noredb-cluster" class="component-detail">
        <h2>NoReDB Cluster: Architecture and Implementation</h2>
        <p>
          NoReDB is a custom-built distributed key-value store written in Rust that serves as the persistent storage layer for WebCanvas. 
          Each NoReDB instance is a high-performance database server capable of storing millions of pixel entries with microsecond read/write 
          latency. The cluster operates in a leaderless, peer-to-peer architecture where each instance independently handles requests routed 
          to it by the Partitioning Controller. This section provides an in-depth look at the internal architecture of a single NoReDB instance, 
          examining the sophisticated storage engine that enables both durability and performance.
        </p>

        <h3>Storage Architecture: Components and Design Rationale</h3>
        <p>
          At its core, each NoReDB instance implements a <strong>Canvas Database (CanvasDB)</strong> that manages pixel data through three 
          specialized components working in concert: a <strong>Write-Ahead Log (WAL)</strong>, a <strong>B-tree Index</strong>, and a 
          <strong>Data Store</strong>. This architecture, inspired by modern database systems like RocksDB and PostgreSQL, separates concerns 
          between fast sequential writes (WAL), efficient indexed lookups (B-tree), and persistent storage (Data Store). All three components 
          are built on top of a unified abstraction layer called <strong>Persistent Random Access Memory (PRAM)</strong>, which provides 
          memory-mapped file access with allocation management.
        </p>

        <div class="diagram-container">
          <ScalableSvg :src="NoredbComponents" alt="NoReDB Components Diagram" />
          <p class="diagram-caption">Figure 7: NoReDB instance internal architecture showing WAL, B-tree index, data store, and PRAM abstraction</p>
        </div>

        <p>
          The <strong>Write-Ahead Log (WAL)</strong> is a circular buffer that records all incoming pixel updates in append-only fashion. 
          Its purpose is twofold: first, it provides crash recovery—if the instance fails before persisting data to the index and store, 
          the WAL can be replayed on restart to restore the database to a consistent state. Second, it decouples write acknowledgment from 
          expensive indexing operations. When a pixel update arrives, it's immediately appended to the WAL and flushed to disk in batches 
          every 100ms, allowing the server to acknowledge the write quickly without blocking on tree updates or memory allocation.
        </p>

        <pre><code>// Write-Ahead Log structure
  pub struct WriteAheadLog&lt;T&gt; where T: Sized {
      pram: Arc&lt;PersistentRandomAccessMemory&gt;,
      size: usize,                      // Maximum number of entries
      length: Arc&lt;AtomicU64&gt;,          // Current number of entries
      head: Arc&lt;RwLock&lt;Pointer&lt;u64&gt;&gt;&gt;, // Write position
      tail: Arc&lt;RwLock&lt;Pointer&lt;u64&gt;&gt;&gt;, // Read position
      data: Arc&lt;Pointer&lt;T&gt;&gt;,           // Circular buffer data
  }</code></pre>

        <p>
          The <strong>B-tree Index</strong> provides O(log n) lookup time for pixel keys, maintaining a mapping from pixel key 
          (a 32-bit hash of x,y coordinates) to memory address in the data store. The B-tree has an order of 255, chosen to fit within 
          4KB memory pages, minimizing page faults during tree traversals. Unlike traditional B-trees that store values inline, this index 
          stores only 64-bit pointers to pixel entries in the data store, keeping the tree compact and cache-friendly. Insertions and updates 
          occur asynchronously in background worker threads, processing batched entries from the WAL without blocking incoming writes.
        </p>

        <pre><code>// B-tree node structure (order 255)
  #[repr(C)]
  struct BTreeNode {
      keys: [u64; 254],        // 254 keys per node
      values: [u64; 255],      // 255 pointers to data store
      length: usize,           // Number of keys in this node
      is_leaf: bool,           // True if leaf node
  }

  const BTREE_ORDER: usize = 255;  // Fits in 4KB page
  const ROOT_NODE_ADDRESS: u64 = 0;</code></pre>

        <p>
          The <strong>Data Store</strong> is a heap-managed memory region where actual pixel entries are stored. Each entry contains the 
          pixel's key (derived from coordinates), RGB color, and a 16-byte timestamp for conflict resolution. The data store supports allocation 
          (<code>malloc</code>) and deallocation (<code>free</code>) of variable-sized blocks, using a free-list allocator to manage fragmentation. 
          When new pixels are inserted, space is allocated in the data store, and the pointer is recorded in the B-tree index. Updates to existing 
          pixels are performed in-place by comparing timestamps and overwriting the entry if the new timestamp is newer.
        </p>

        <pre><code>#[repr(C)]
  pub struct PixelEntry {
      pub pixel: Pixel,           // Key (u32) + color ([u8; 3])
      pub timestamp: TimeStamp,   // 16-byte hybrid logical clock
  }

  impl CanvasDB {
      pub fn new(width: usize, height: usize, path: &str, wal_size: usize) -> Self {
          // WAL: wal_size * sizeof(PixelEntry) + 1024 bytes overhead
          let wal_pram = PersistentRandomAccessMemory::new(
              wal_size * std::mem::size_of::&lt;PixelEntry&gt;() + 1024,
              &format!("{}.wal", path)
          );
          
          // B-tree: (width * height * 32 * 2) + 1024 bytes
          // *32 for each entry (8 bytes key + 8 bytes value + overhead)
          // *2 for internal nodes
          let btree_pram = PersistentRandomAccessMemory::new(
              (width * height * 32 * 2) + 1024,
              &format!("{}.index", path)
          );
          
          // Data store: (width * height * sizeof(PixelEntry) * 2) + 1024 bytes
          // *2 for fragmentation and alignment
          let data_store = PersistentRandomAccessMemory::new(
              (width * height * std::mem::size_of::&lt;PixelEntry&gt;() * 2) + 1024,
              &format!("{}.store", path)
          );
      }
  }</code></pre>

        <h3>Persistent Random Access Memory (PRAM): Unified Abstraction</h3>
        <p>
          All three storage components—WAL, B-tree, and Data Store—are built on top of <strong>PRAM (Persistent Random Access Memory)</strong>, 
          a foundational abstraction that provides memory-mapped file I/O with allocation management. PRAM uses <code>mmap</code> to map files 
          directly into the process's address space, allowing reads and writes to persistent storage via normal memory operations (pointer 
          dereferences) rather than explicit I/O syscalls. This approach, called memory-mapped I/O, leverages the operating system's page cache 
          and virtual memory system for efficient disk access.
        </p>

        <p>
          The PRAM implementation provides three allocation primitives: <code>malloc</code> allocates a new block of memory using a free-list 
          allocator, <code>free</code> returns a block to the free list, and <code>smalloc</code> (static allocation) claims a fixed address 
          for metadata like the WAL's head/tail pointers. The abstraction exposes typed <code>Pointer&lt;T&gt;</code> objects that encapsulate 
          both the memory address and a weak reference to the PRAM manager, preventing use-after-free bugs while allowing efficient pointer 
          arithmetic and dereferencing.
        </p>

        <pre><code>pub struct PersistentRandomAccessMemory {
      mmap: Arc&lt;RwLock&lt;MmapMut&gt;&gt;,      // Memory-mapped file
      free_list: Arc&lt;Mutex&lt;BTreeSet&lt;(usize, u64)&gt;&gt;&gt;, // Available blocks (size, address)
      allocated: Arc&lt;Mutex&lt;BTreeSet&lt;(u64, usize)&gt;&gt;&gt;, // Used blocks (address, size)
  }

  impl PersistentRandomAccessMemory {
      // Dynamic allocation using free-list
      pub fn malloc&lt;T&gt;(&self, size: usize) -> Result&lt;Pointer&lt;T&gt;, Error&gt; {
          let mut free_list = self.free_list.lock();
          // Find first block large enough (first-fit allocation)
          let block = free_list.range((size, 0)..).next().copied();
          // ... split block, update free list, return pointer
      }
      
      // Static allocation at fixed address for metadata
      pub fn smalloc&lt;T&gt;(&self, address: u64, size: usize) -> Result&lt;Pointer&lt;T&gt;, Error&gt; {
          // Mark specific address range as allocated
          // Used for WAL head/tail pointers, B-tree root, etc.
      }
  }</code></pre>

        <p>
          The <strong>Write-Ahead Log</strong> uses PRAM's <code>smalloc</code> to allocate fixed positions for its metadata: head and tail 
          pointers are stored at offsets 0 and 8, with the circular buffer data starting at offset 16. On initialization, the WAL reads these 
          pointers from disk (if they exist from a previous run) to determine where the log left off, enabling crash recovery. Appending to 
          the WAL simply writes the entry to <code>data[head % size]</code> and increments the head pointer, with periodic <code>persist()</code> 
          calls flushing the memory-mapped region to disk via <code>msync</code>.
        </p>

        <pre><code>impl&lt;T&gt; WriteAheadLog&lt;T&gt; where T: Sized {
      pub fn new(pram: Arc&lt;PersistentRandomAccessMemory&gt;, size: usize) -> Self {
          // Allocate head/tail pointers at fixed addresses
          let head: Pointer&lt;u64&gt; = pram.smalloc::&lt;u64&gt;(0, 8).unwrap();
          let tail: Pointer&lt;u64&gt; = pram.smalloc::&lt;u64&gt;(8, 8).unwrap();
          
          // Read existing values (for crash recovery)
          let head_val: u64 = head.deref().unwrap_or(0);
          let tail_val: u64 = tail.deref().unwrap_or(0);
          
          // Calculate current length
          let length = if head_val &gt;= tail_val {
              head_val - tail_val
          } else {
              size as u64 - tail_val + head_val
          };
          
          // Allocate circular buffer data region
          let data = pram.smalloc::&lt;T&gt;(16, size * std::mem::size_of::&lt;T&gt;()).unwrap();
          
          WriteAheadLog { pram, size, length, head, tail, data, ... }
      }
      
      pub fn append(&self, entry: &T) -> Result&lt;(), Error&gt; {
          let head_lock = self.head.write();
          let head_val = self.head_cache.load(Ordering::Acquire);
          
          // Write to circular buffer
          let index = head_val % self.size as u64;
          self.data.at(index as usize).set(entry)?;
          
          // Update head pointer
          let new_head = head_val + 1;
          head_lock.set(&new_head)?;
          self.head_cache.store(new_head, Ordering::Release);
          
          Ok(())
      }
  }</code></pre>

        <p>
          Similarly, the <strong>B-tree Index</strong> uses PRAM to allocate tree nodes on demand. When a node splits during insertion, the 
          index calls <code>pram.malloc(sizeof(BTreeNode))</code> to allocate space for the new node, storing its address in the parent node's 
          values array. Node pointers remain stable across restarts because PRAM preserves the memory-mapped file, allowing the tree structure 
          to be recovered by simply loading the root node from address 0 and traversing the child pointers. The B-tree's <code>persist()</code> 
          method calls <code>pram.persist()</code> to flush all dirty pages to disk, ensuring durability.
        </p>

        <h3>Write Path: Asynchronous Persistence Pipeline</h3>
        <p>
          Understanding NoReDB's write path reveals why it achieves both high throughput and durability. When a <code>set_pixel</code> request 
          arrives via gRPC, the CanvasDB immediately appends the pixel entry to the WAL, a fast O(1) operation requiring only a pointer 
          increment and memory write. The write is <em>not</em> immediately flushed to disk—instead, a background persistence thread wakes up 
          every 100ms, calls <code>wal.commit()</code> to flush buffered entries via <code>msync</code>, and then invokes all registered listener 
          callbacks to acknowledge the writes. This batching amortizes the cost of disk synchronization across many writes, achieving throughput 
          of hundreds of thousands of operations per second.
        </p>

        <pre><code>impl CanvasDBTrait for CanvasDB {
      fn set_pixel(&self, pixel: PixelEntry, listener: Option&lt;Box&lt;dyn FnOnce() + Send&gt;&gt;) {
          // Step 1: Append to WAL (fast, in-memory)
          self.write_ahead_log.append(&pixel).expect("WAL append failed");
          
          // Step 2: Register listener for acknowledgment after persistence
          if let Some(callback) = listener {
              let mut listeners = self.listeners.lock().unwrap();
              listeners.push(callback);
          }
          
          // Background threads handle steps 3-6 asynchronously
      }
  }</code></pre>

        <p>
          While the persistence thread handles WAL flushing, separate worker threads (typically 4-16 threads) continuously drain entries from 
          the WAL and apply them to the B-tree and Data Store. The persistence coordinator thread runs this loop every 100ms:
        </p>

        <pre><code>// Persistence coordinator thread (simplified)
  loop {
      // Step 1: Peek entries from WAL without removing them
      let entries = wal.peek_many(PERSIST_BATCH_SIZE); // 8192 entries
      
      // Step 2: Send entries to worker threads for processing
      for entry in entries {
          tx_work.send(entry).unwrap();
      }
      
      // Step 3: Wait for all workers to finish processing
      for _ in 0..entries.len() {
          rx_res.recv().unwrap(); // Worker signals completion
      }
      
      // Step 4: Persist B-tree and data store to disk
      data_store.persist()?;
      btree_index.persist()?;
      
      // Step 5: Remove processed entries from WAL
      wal.pop_many(entries.len())?;
      
      // Step 6: Commit WAL (advance tail pointer)
      wal.commit()?;
  }</code></pre>

        <p>
          Each worker thread processes entries by first checking if the pixel key exists in the B-tree index. If found, the worker loads the 
          pointer to the existing entry in the data store and performs an atomic compare-and-swap on the timestamp: if the new entry has a 
          newer timestamp, it overwrites the old data; otherwise, it discards the update. If the key doesn't exist, the worker allocates space 
          in the data store via <code>malloc</code>, inserts the pointer into the B-tree, and writes the pixel data. This ensures that 
          out-of-order delivery (e.g., an old update arriving after a new one due to network delays) doesn't corrupt the database.
        </p>

        <pre><code>// Worker thread processing (simplified)
  let pixel = rx.recv().unwrap(); // Receive entry from coordinator

  // Check if pixel exists in B-tree
  let pointer_address = btree_index.get(pixel.pixel.key as u64);

  if let Ok(addr) = pointer_address {
      // Update existing pixel
      let pointer = Pointer::&lt;PixelEntry&gt;::from_address(addr, data_store.clone());
      let entry = pointer.deref()?;
      
      // Compare timestamps and update if newer
      if pixel.timestamp &gt; entry.timestamp {
          pointer.set(&pixel)?;
      }
  } else {
      // Insert new pixel
      let new_pointer = data_store.malloc::&lt;PixelEntry&gt;(std::mem::size_of::&lt;PixelEntry&gt;())?;
      new_pointer.set(&pixel)?;
      btree_index.set(pixel.pixel.key as u64, new_pointer.address)?;
  }

  tx_res.send(()).unwrap(); // Signal completion to coordinator</code></pre>

        <p>
          This multi-stage pipeline achieves durability without sacrificing performance. The WAL acts as a buffer, absorbing write bursts and 
          providing immediate acknowledgment to clients. The worker threads asynchronously apply updates to the indexed data structures, which 
          are optimized for read performance. Only after both the B-tree and data store are successfully persisted does the coordinator remove 
          entries from the WAL, ensuring that no committed write is lost even if the server crashes mid-process.
        </p>

        <h3>Read Path: Multi-Layered Lookup with Timestamp Arbitration</h3>
        <p>
          The read path demonstrates the importance of the WAL in maintaining consistency. When a <code>get_pixel</code> request arrives, 
          CanvasDB must check <em>both</em> the WAL and the indexed data store, returning whichever entry has the newer timestamp. This is 
          necessary because recent writes may still be in the WAL awaiting batch processing, not yet reflected in the B-tree/data store. The 
          implementation first scans the WAL (in batches of 8192 entries to exploit sequential memory access), filtering for the requested 
          pixel key and tracking the entry with the maximum timestamp.
        </p>

        <pre><code>impl CanvasDBTrait for CanvasDB {
      fn get_pixel(&self, key: u32) -> Option&lt;(Pixel, TimeStamp)&gt; {
          // Step 1: Search WAL for most recent entry
          let most_current_wal = self.write_ahead_log
              .iter(GET_BATCH_SIZE) // 8192 entries per batch
              .flatten()
              .filter(|e| e.pixel.key == key)
              .max_by(|a, b| a.timestamp.cmp(&b.timestamp));
          
          // Step 2: Lookup in B-tree index
          let pointer_address = self.btree_index.get(key as u64);
          
          if let Ok(addr) = pointer_address {
              let pointer = Pointer::&lt;PixelEntry&gt;::from_address(addr, self.data_store.clone());
              let entry = pointer.deref().ok()?;
              
              // Step 3: Compare timestamps and return newest
              if let Some(wal_entry) = most_current_wal {
                  if wal_entry.timestamp &gt; entry.timestamp {
                      return Some((wal_entry.pixel, wal_entry.timestamp));
                  }
              }
              return Some((entry.pixel, entry.timestamp));
          } else {
              // Not in index, return WAL entry if found
              most_current_wal.map(|e| (e.pixel, e.timestamp))
          }
      }
  }</code></pre>

        <p>
          This dual-lookup strategy ensures <strong>read-after-write consistency</strong>: a client that writes a pixel and immediately reads 
          it back will always see the value it wrote, even if the write hasn't yet been indexed. The timestamp comparison handles race conditions 
          where a worker thread is concurrently updating the indexed entry—the read path always returns the globally newest value. The tradeoff 
          is that reads must scan the WAL, which adds latency proportional to the current WAL occupancy. To minimize this cost, the WAL is sized 
          to hold only a few seconds of writes (typically 1024-2048 entries), and the batch processing keeps it drained rapidly.
        </p>

        <div class="diagram-container">
          <ScalableSvg :src="NoredbSequence" alt="NoReDB Read/Write Sequence Diagram" />
          <p class="diagram-caption">Figure 8: NoReDB read and write operation sequences showing WAL, B-tree index, and data store interactions</p>
        </div>

        <h3>Crash Recovery: Replaying the Write-Ahead Log</h3>
        <p>
          When a NoReDB instance restarts after a crash, the initialization sequence checks the WAL for uncommitted entries by comparing the 
          head and tail pointers read from PRAM. If the WAL contains entries (head != tail), the system enters recovery mode, replaying each 
          entry by re-executing the worker thread logic: check the B-tree, compare timestamps, update or insert as needed. Because the WAL 
          records operations with idempotent semantics (timestamp-based conflict resolution), replaying entries multiple times is safe—applying 
          the same update twice produces the same result as applying it once.
        </p>

        <p>
          This recovery mechanism guarantees that <strong>all acknowledged writes survive crashes</strong>. When the persistence thread calls 
          <code>wal.commit()</code> and then invokes listener callbacks, clients receive acknowledgment only after the WAL has been fsynced 
          to disk. Even if the instance crashes immediately after acknowledgment—before the B-tree and data store are persisted—the write is 
          preserved in the WAL and will be replayed on restart. The WAL acts as a transactional log, similar to PostgreSQL's WAL or MySQL's 
          redo log, providing atomicity and durability guarantees.
        </p>

        <h3>Performance Characteristics and Benchmark Results</h3>
        <p>
          To validate the design, comprehensive benchmarks were conducted using Criterion.rs, measuring throughput and latency across various 
          workloads. The tests ran on a system with an NVMe SSD and 16 CPU cores, simulating realistic canvas operations. Three key benchmark 
          suites were executed: <strong>CanvasDB</strong> (end-to-end pixel operations), <strong>B-tree Index</strong> (isolated tree performance), 
          and <strong>PRAM</strong> (memory allocator overhead).
        </p>

        <h4>CanvasDB Concurrent Writes</h4>
        <p>
          The <code>canvasdb_set_concurrent</code> benchmark spawns 8 threads, each writing a disjoint subset of pixel keys to avoid contention. 
          The system was tested with varying canvas sizes (32,768, 65,536, and 131,072 pixels), measuring total throughput:
        </p>

        <pre><code class="benchmark-results">canvasdb_set_concurrent/32768    time: [186.02 ms  207.60 ms  225.47 ms]
                                        thrpt: [145.33 Kelem/s  157.84 Kelem/s  176.15 Kelem/s]

  canvasdb_set_concurrent/65536     time: [226.81 ms  235.65 ms  242.24 ms]
                                        thrpt: [270.54 Kelem/s  278.10 Kelem/s  288.94 Kelem/s]

  canvasdb_set_concurrent/131072    time: [240.08 ms  241.38 ms  242.64 ms]
                                        thrpt: [540.19 Kelem/s  543.02 Kelem/s  545.94 Kelem/s]</code></pre>

        <p>
          These results demonstrate excellent scalability: with 131,072 writes, the system sustains <strong>543,000 writes per second</strong> 
          (543 Kelem/s), translating to approximately 1.8 microseconds per write operation. The near-linear scaling from 32K to 131K elements 
          indicates that the WAL circular buffer and batching strategy effectively amortize synchronization costs. The variance in throughput 
          (±10% between runs) stems from OS page cache behavior and background persistence threads competing for disk bandwidth.
        </p>

        <h4>CanvasDB Concurrent Reads</h4>
        <p>
          Read performance was measured by pre-populating the database with pixels and then issuing concurrent lookups across 8 threads:
        </p>

        <pre><code class="benchmark-results">canvasdb_get_concurrent/32768     time: [198.81 ms  208.99 ms  218.58 ms]
                                        thrpt: [149.91 Kelem/s  156.79 Kelem/s  164.82 Kelem/s]

  canvasdb_get_concurrent/65536      time: [219.68 ms  233.84 ms  246.69 ms]
                                        thrpt: [265.66 Kelem/s  280.27 Kelem/s  298.32 Kelem/s]

  canvasdb_get_concurrent/131072     time: [233.88 ms  244.72 ms  256.32 ms]
                                        thrpt: [511.37 Kelem/s  535.59 Kelem/s  560.43 Kelem/s]</code></pre>

        <p>
          Read throughput reaches <strong>535,000 reads per second</strong> for the 131K dataset, slightly slower than writes due to the 
          dual-lookup overhead (WAL scan + B-tree traversal). The O(log n) B-tree complexity is evident in the modest increase in latency 
          as dataset size grows: reads scale from 157 Kelem/s (32K) to 535 Kelem/s (131K), roughly 3.4× throughput improvement for 4× data size. 
          This sub-linear scaling confirms that tree depth increases logarithmically, with the 131K dataset requiring one additional tree level 
          compared to 32K.
        </p>

        <h4>CanvasDB Iterator Performance</h4>
        <p>
          The <code>canvasdb_iterate_pixels</code> benchmark measures full dataset scans, which traverse the B-tree in sorted order and 
          dereference each pointer to load pixel data from the data store:
        </p>

        <pre><code class="benchmark-results">canvasdb_iterate_pixels/65536     time: [155.91 ms  170.50 ms  185.22 ms]
                                        thrpt: [353.84 Kelem/s  384.37 Kelem/s  420.34 Kelem/s]

  canvasdb_iterate_pixels/131072     time: [189.61 ms  200.75 ms  209.44 ms]
                                        thrpt: [625.82 Kelem/s  652.91 Kelem/s  691.27 Kelem/s]

  canvasdb_iterate_pixels/262144     time: [209.81 ms  216.04 ms  221.68 ms]
                                        thrpt: [1.1825 Melem/s  1.2134 Melem/s  1.2494 Melem/s]</code></pre>

        <p>
          Sequential iteration achieves <strong>1.2 million elements per second</strong> (262K dataset), significantly faster than random 
          access due to spatial locality. Traversing the B-tree in order results in mostly sequential memory access patterns, benefiting from 
          CPU cache prefetching and reduced TLB misses. This performance is critical for bulk operations like snapshotting the canvas or 
          migrating partitions during rebalancing.
        </p>

        <h2>Project Reflections: Successes and Challenges</h2>
        <p>
          Building WebCanvas was an ambitious undertaking that pushed the boundaries of what could be accomplished within the project timeline. 
          Looking back, the journey revealed both successes and areas where initial assumptions proved overly optimistic.
        </p>

        <h4>Scope and Learning Curve</h4>
        <p>
          The project scope was significantly underestimated at the outset. Building three distributed systems components from scratch—Veritas 
          (consensus), NoReDB (database), and the Partitioning Controller—while learning <strong>two entirely new programming languages</strong> 
          (Rust and Go, or arguably three if Kubernetes YAML is counted) proved far more time-consuming than anticipated. Both Rust and Go were 
          completely new technologies, requiring simultaneous learning of language syntax, idioms, concurrency models, and ecosystem tooling 
          while architecting distributed systems. Despite this challenge, the agile project management approach allowed for continuous adaptation, 
          prioritizing core functionality and deferring non-essential features when timelines became tight.
        </p>

        <h4>Technical Evolution and Discovery</h4>
        <p>
          A significant amount of effort was initially invested in building a <strong>custom memory management system</strong> for persistent 
          storage, complete with manual caching, paging algorithms, and file block management. Only after substantial implementation work did 
          the realization occur that <strong>memory-mapped files</strong> (<code>mmap</code>) provided exactly this functionality at the OS 
          level, rendering the custom solution obsolete. The decision to pivot to memory-mapped I/O, while costly in terms of discarded code, 
          ultimately improved both performance and reliability by leveraging battle-tested kernel implementations rather than reinventing them.
        </p>

        <h4>LLM-Assisted Development: Effectiveness and Limitations</h4>
        <p>
          Large Language Models (Claude, ChatGPT, Gemini) proved invaluable for certain tasks but fell short in others. They excelled at 
          generating <strong>documentation, boilerplate code, and frontend Vue.js components</strong>, significantly accelerating development 
          in these areas. However, they struggled dramatically with the <strong>core Rust database implementation and Go partitioning logic</strong>. 
          LLM-generated Rust code frequently exhibited anti-patterns like collecting entire iterators into memory before filtering (materializing 
          the entire B-tree index), incorrect lifetime annotations, and unsafe pointer usage that violated Rust's guarantees. The generated Go 
          code similarly showed poor understanding of context propagation, goroutine lifecycle management, and channel patterns. These components 
          ultimately required manual authorship from first principles, with LLMs relegated to syntax lookup rather than code generation.
        </p>

        <h4>Veritas Complexity: Custom Protocol Challenges</h4>
        <p>
          The <strong>Veritas consensus implementation</strong> was substantially more complex than initially estimated. Designing a custom 
          leader election protocol with loyalty mechanisms, handling split-brain scenarios, and ensuring linearizable atomic operations required 
          deep understanding of distributed systems theory. The decision to build a custom protocol rather than adopting Raft or Paxos was driven 
          by educational curiosity, but the complexity cost was high. Debugging timing-dependent race conditions in leader transitions and vote 
          counting consumed significant development time, though the learning experience was invaluable.
        </p>

        <h4>Testing Philosophy and Infrastructure</h4>
        <p>
          Testing strategy varied significantly across components. The <strong>stateful backend components</strong> (NoReDB and Veritas) received 
          extensive unit and integration testing, particularly at the PRAM, WAL, and B-tree levels. However, abstracting implementation details 
          for testability in Rust proved extremely difficult—creating mock <code>PersistentRandomAccessMemory</code> instances or simulating 
          network partitions for Veritas leader election required fighting Rust's ownership system and trait bounds. The tests that were written 
          provided high confidence in correctness, but achieving comprehensive coverage would have required an architectural redesign prioritizing 
          testability from the start.
        </p>

        <p>
          In contrast, the <strong>stateless backend</strong> (ASP.NET SignalR services) and <strong>frontend</strong> received minimal automated 
          testing. The frontend logic was simple enough—primarily UI state management and WebSocket event handling—that manual testing sufficed. 
          The backend's complexity lay in coordination rather than algorithmic logic, making integration tests against the full cluster more 
          valuable than isolated unit tests. Time constraints necessitated prioritizing functional testing over achieving high test coverage metrics.
        </p>

        <h4>Rust: Pain and Payoff</h4>
        <p>
          Working with Rust was a love-hate relationship. The language's <strong>type system and explicit error handling</strong> forced thinking 
          through both success and failure paths at every step—no <code>null</code> pointer exceptions, no unchecked errors, explicit <code>panic!</code> 
          for truly unrecoverable situations. This rigor caught countless bugs at compile time that would have been runtime crashes in C++ or Go. 
          The borrow checker, while frustrating during development, prevented entire classes of concurrency bugs and memory leaks that would have 
          been nightmares to debug in a distributed system.
        </p>

        <p>
          However, Rust's learning curve was steep. Fighting the compiler to express patterns that were trivial in garbage-collected languages—such 
          as self-referential data structures or dynamic dispatch with shared ownership—consumed hours that could have been spent on features. 
          With the benefit of hindsight and the experience gained, <strong>many Rust components would be rewritten differently</strong> if starting 
          over: more liberal use of <code>Arc&lt;Mutex&lt;T&gt;&gt;</code> for simplicity rather than complex lifetime annotations, trait-based 
          dependency injection for testability, and clearer separation between hot-path performance code and cold-path configuration logic.
        </p>

        <h4>Would Do It Again</h4>
        <p>
          Despite the challenges, undertaking this project was absolutely worthwhile. The hands-on experience of building distributed systems 
          components from scratch—wrestling with consensus algorithms, crash recovery, concurrent data structures, and network partition handling—provided 
          insights that no amount of reading could replicate. Pairing this practical work with <em>Designing Data-Intensive Applications</em> by 
          Martin Kleppmann created a powerful learning loop: the book explained the theory and tradeoffs, the project revealed why those tradeoffs 
          matter in practice. The knowledge gained about distributed systems, Rust, Go, and Kubernetes far exceeded the project's initial educational 
          goals, making the significant time investment worthwhile.
        </p>

        <h3>Future Improvements and Production Considerations</h3>
        <p>
          WebCanvas was designed as an <strong>educational reference implementation</strong> prioritizing clarity and modularity over maximum 
          performance. While the system successfully handles hundreds of thousands of operations per second, transitioning to a production deployment 
          would require several architectural and feature enhancements:
        </p>

        <h4>Performance Optimizations</h4>
        <ul>
          <li><strong>Backend Image Caching and Delta Streaming:</strong> Currently, the ASP.NET backend forwards every pixel update individually 
          over SignalR. A production system would maintain a <strong>server-side cached render of the full canvas image</strong>, serving initial 
          page loads with the complete image, then streaming only incremental changes (deltas) over the WebSocket connection. This would reduce 
          bandwidth consumption by 10-100× for clients joining an active canvas and eliminate the thundering herd problem when many users connect 
          simultaneously.</li>

          <li><strong>Vector Clocks for Cache Invalidation:</strong> The current system uses hybrid logical clocks (global + local) managed by 
          the Partitioning Controller, requiring periodic <code>get_add</code> calls to Veritas. An alternative would be <strong>vector clocks 
          determined by the ASP.NET backend instances</strong> themselves, piggybacking version vectors on the cache invalidation messages already 
          being broadcast between backend pods. <em>Advantages:</em> Eliminates Veritas dependency for timestamp generation, reduces latency by 
          one network round-trip per write. <em>Disadvantages:</em> Violates separation of concerns by coupling cache invalidation logic to database 
          consistency model, makes backend stateful (must maintain vector clock state), increases implementation complexity. This exemplifies the 
          performance-vs-modularity tradeoff discussed throughout the architecture.</li>

          <li><strong>Specialized Pixel Storage:</strong> The current NoReDB uses a general-purpose B-tree index mapping arbitrary keys to memory 
          addresses. For a canvas with known dimensions (e.g., 3840×2160), pixels could be stored in a <strong>simple indexed array</strong>: 
          pixel 1 at offset 0, pixel 2 at offset 8, pixel 3 at offset 16, and so forth. This would eliminate the B-tree entirely, replacing $O(\log n)$ 
          lookups with $O(1)$ array indexing. <em>Advantages:</em> 5-10× faster reads/writes, simpler codebase, reduced memory overhead. 
          <em>Disadvantages:</em> Loses generality—cannot handle sparse canvases with few set pixels, fixed canvas size at compile time, WAL still 
          needed for durability so only partial simplification. This specialization-vs-generality tradeoff is central to production database design.</li>

          <li><strong>Lock-Free Data Structures:</strong> The B-tree uses read-write locks (<code>RwLock</code>) for concurrency control. Implementing 
          lock-free structures using atomic operations (e.g., Bw-tree, skip lists) could eliminate contention on hot paths, improving scalability on 
          high core-count systems. However, lock-free algorithms are notoriously difficult to implement correctly and debug, requiring expert-level 
          understanding of memory ordering and ABA problems.</li>

          <li><strong>Write Combining and Batching:</strong> Currently, each pixel update generates a separate WAL entry. Batching multiple updates 
          into a single entry would reduce metadata overhead and improve cache locality. The Partitioning Controller could accumulate writes for 
          10-100ms before flushing to NoReDB, trading slight latency for higher throughput.</li>
        </ul>

        <h4>Feature Enhancements</h4>
        <ul>
          <li><strong>User Authentication and Authorization:</strong> Production deployment requires <strong>user accounts</strong> with authentication 
          (OAuth2, JWT tokens), per-user rate limiting to prevent abuse (e.g., max 10 pixels per second), and optional access control (public vs. 
          private canvases, moderator roles).</li>

          <li><strong>Multiple Canvases and Persistence:</strong> The current system supports a single global canvas. Scaling to thousands of independent 
          canvases requires routing logic (hash canvas ID to partition), per-canvas access control, and lifecycle management (archive inactive canvases 
          to cold storage after N days).</li>

          <li><strong>Canvas History and Rollback:</strong> Store not just current state but full history of changes, enabling time-travel queries 
          ("show me the canvas as it looked 1 hour ago") and rollback functionality for moderation.</li>

          <li><strong>Draw Tools Beyond Pixels:</strong> Lines, rectangles, fill tools, erasers, undo/redo, layers—all require moving beyond single-pixel 
          operations to higher-level drawing primitives with transaction semantics.</li>
        </ul>

        <h4>Infrastructure and Operations</h4>
        <ul>
          <li><strong>Established Coordination Service:</strong> Replace Veritas with <strong>etcd, Consul, or ZooKeeper</strong>. While building 
          a custom consensus protocol was educationally valuable, production systems should use battle-tested implementations with years of bug fixes 
          and operational tooling. The custom protocol lacks monitoring, debugging tools, and the expertise of a large community.</li>

          <li><strong>Standard Protocols:</strong> The current system uses custom gRPC message formats and coordination protocols designed for learning 
          purposes. Production deployment should adopt <strong>established protocols</strong> where possible: Raft for consensus, Prometheus for metrics, 
          OpenTelemetry for distributed tracing. Custom protocols incur ongoing maintenance burden and make it difficult for new team members to understand 
          the system.</li>

          <li><strong>Observability and Debugging:</strong> Add comprehensive metrics (latency percentiles, throughput, error rates), distributed tracing 
          to follow requests across services, structured logging with correlation IDs, and health check endpoints for each component. The current system 
          has basic logging but lacks production-grade observability.</li>

          <li><strong>Improved Testing Infrastructure:</strong> Addressing the Rust testability challenges would require <strong>better abstractions</strong>: 
          trait-based dependency injection for PRAM, B-tree, and WAL to allow mock implementations; async test harnesses for simulating network partitions 
          and clock skew; property-based testing (e.g., with <code>proptest</code>) to verify invariants under random operation sequences. The difficulty 
          of testing the current implementation suggests it was not designed with testing as a first-class concern.</li>
        </ul>

        <p>
          These improvements collectively represent the gap between an educational prototype and a production system. The current implementation 
          successfully demonstrates distributed systems principles and validates the core architecture, achieving performance sufficient for the 
          WebCanvas use case. However, the modularity and clarity that make it valuable for learning would need to be balanced with specialization 
          and optimization for production deployment. This tension—between understandable general-purpose code and high-performance specialized 
          systems—is a fundamental tradeoff in software engineering, and one that this project illuminated through direct experience.
        </p>
      </section>
    </div>
  </div>
</template>

<script setup lang="ts">
import InteractiveDiagram from '@/components/InteractiveDiagram.vue'
import ScalableSvg from '@/components/ScalableSvg.vue'
import AspNetBackendSequenceDiagram from '@/assets/AspNetBackendSequenceDiagram.drawio.svg'
import AspNetBackendComponents from '@/assets/AspNetBackendComponents.drawio.svg'
import GoPartitioningControllerComponents from '@/assets/GoPartitioningControllerComponentsDark.drawio.svg'
import VeritasComponents from '@/assets/VeritasComponentsDark.drawio.svg'
import VeritasGetSetSequence from '@/assets/VeritasGetSetSequenceDiagramDark.drawio.svg'
import VeritasLeaderElectionSequence from '@/assets/VeritasLeaderElectionSequenceDark.drawio.svg'
import NoredbComponents from '@/assets/NoredbComponentsDark.drawio.svg'
import NoredbSequence from '@/assets/NoredbSequenceDark.drawio.svg'


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
    targetSection: 'veritas-consensus',
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
    targetSection: 'veritas-consensus',
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
.page {
  overflow-y: scroll;
  height: calc(100dvh - 72px);
}

.about {
  margin: 1rem auto;
  padding: 0 2rem;
  max-width: 1280px;
  color: #ffffff;
  overflow: visible;
  height: fit-content;
  padding-bottom: 5em;
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

code {
  background: rgba(255, 255, 255, 0.1);
  padding: 0.2rem 0.4rem;
  border-radius: 3px;
  font-family: 'Courier New', Courier, monospace;
  font-size: 0.9em;
  color: #a8dadc;
}

pre {
  background: rgba(0, 0, 0, 0.3);
  padding: 1rem;
  border-radius: 6px;
  overflow-x: auto;
  margin: 1rem 0;
  border: 1px solid rgba(255, 255, 255, 0.1);
}

pre code {
  background: none;
  padding: 0;
  font-size: 0.85rem;
  line-height: 1.5;
  color: #e0e0e0;
  display: block;
  white-space: pre;
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
  
  pre {
    padding: 0.75rem;
    font-size: 0.8rem;
  }
  
  pre code {
    font-size: 0.75rem;
  }
}

</style>
