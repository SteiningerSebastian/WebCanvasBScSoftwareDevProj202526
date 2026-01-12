package partitioningcontroller

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	pixelhasher "general/pixel_hasher"
	"log/slog"
	noredbclient "partitioning-controller/noredb-client"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	veritasclient "veritas-client"
)

const VERITAS_PARTITIONING_SERVICE_KEY = "service-partitioning-controller.partition.config"
const VERITAS_TIME_SERVICE_KEY = "service-partitioning-controller.time"

// The service registration key for the NoReDB service (the databases currently running)
const NOREDB_SERVICE_REGISTRATION_KEY = "service-noredb"

// After this timeout, a NoReDB service endpoint is considered inactive and it is removed from partition assignments.
// (If it's not back in time, the leader will reassign its partitions to other active NoReDB services. This is to ensure availability.)
const NOREDB_SERVICE_REGISTRATION_TIMEOUT_PARTITIONING = 5 * time.Minute

const SERVICE_REGISTRATION_TIMEOUT = 30 * time.Second
const LEADER_ELECTION_INTERVAL = 5 * time.Second

const ROLE_LEADER uint8 = 1
const ROLE_FOLLOWER uint8 = 2

const RETRY_COUNT = 5
const RETRY_INTERVAL = 200 * time.Millisecond
const RETRY_BACKOFF_FACTOR = 2

const PARTITIONING_INTERVAL = 10 * time.Second

type PartitioningControllerConfig struct {
	nPartitions       uint32              // Total number of partitions - must not change else data consistency breaks
	nReplicas         uint32              // Number of replicas for each partition (This is for quorum read/writes, failover is implemented but takes longer and loosing quorum while reconstructing can lead to data loss)
	nReads            uint32              // Number of replicas to read from for quorum reads
	nWrites           uint32              // Number of replicas to write to for quorum writes
	mServicePartition map[uint32][]string // Map partition ID to list of NoReDB service IDs responsible for that partition
}

type PartitioningController struct {
	mu                     sync.RWMutex
	config                 *PartitioningControllerConfig
	vClient                *veritasclient.VeritasClient
	service_handler        *veritasclient.ServiceRegistrationHandler
	noredb_service_handler *veritasclient.ServiceRegistrationHandler
	role                   uint8
	id                     string // Unique identifier for this controller instance (e.g., hostname)
	cancel_duties          context.CancelFunc
	noredb_clients         map[string]noredbclient.NoreDBClient // Map of NoReDB service ID to client
	op_index               atomic.Uint32                        // Monotonic operation index for requests
	local_time             atomic.Uint64                        // Local logical clock
	global_time            atomic.Uint64                        // Global logical clock
}

// getActiveNoReDBServices retrieves the list of currently active NoReDB services based on their id.
func (pc *PartitioningController) getActiveNoReDBServices(ctx context.Context) ([]string, error) {
	service_reg, err := pc.noredb_service_handler.ResolveService(ctx)
	if err != nil {
		slog.Error("Error resolving NoReDB service.", "error", err)
		return nil, err
	}

	active_services := make([]string, 0)
	for _, endpoint := range service_reg.Endpoints {
		if time.Since(endpoint.Timestamp) < SERVICE_REGISTRATION_TIMEOUT {
			active_services = append(active_services, endpoint.ID)
		}
	}

	return active_services, nil
}

// dropLeadershipDuties removes leadership duties from this instance, if it is currently the leader.
// This involves updating the service registration to remove this instance as the leader.
// If any errors occur during this process, they are logged and the function panics to force a restart and re-election.
// This is to ensure that the system remains in a consistent state and that leadership can be properly reassigned.
func (pc *PartitioningController) dropLeadershipDuties() {
	// In this critical phase, we lock the entire controller to prevent any concurrent modifications.
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Get current service registration
	ctx := context.Background()
	service_reg, err := pc.service_handler.ResolveService(ctx)
	if err != nil {
		slog.Error("Error resolving service during dropLeadershipDuties.", "error", err)
		// At this point we can not continue, panic to force a restart and re-election
		// It would not be safe to continue as we can not performe leadership duties or drop them properly
		panic("Error resolving service during dropLeadershipDuties.")
	}

	if service_reg.Meta["leader_id"] == pc.id {
		// Remove self as leader
		service_reg.Meta["leader_id"] = ""
		err := pc.service_handler.RegisterOrUpdateService(ctx, service_reg, nil)
		if err != nil {
			slog.Error("Error dropping leadership during dropLeadershipDuties.", "error", err)
			// At this point we can not continue, panic to force a restart and re-election
			// It would not be safe to continue as we can not performe leadership duties or drop them properly
			panic("Error dropping leadership during dropLeadershipDuties.")
		}
	}

	pc.role = ROLE_FOLLOWER
	if pc.cancel_duties != nil {
		pc.cancel_duties()
	}
}

func (pc *PartitioningController) startLeadershipDuties(ctx context.Context) error {
	var current_config PartitioningControllerConfig
	i := 0
	time_retry := RETRY_INTERVAL
	for ; i < RETRY_COUNT; i++ {
		// Get the current configuration
		config, err := pc.vClient.GetVariable(ctx, VERITAS_PARTITIONING_SERVICE_KEY)
		if err != nil {
			slog.Error("PartitioningController leader failed to get current config.", "error", err)
			time.Sleep(time_retry) // Wait before retrying
			time_retry *= RETRY_BACKOFF_FACTOR
			continue
		}

		// Make sure we have the latest config at start of leadership duties
		slog.Info("PartitioningController starting leadership duties with current config.")
		if err := json.Unmarshal([]byte(config), &current_config); err == nil {
			slog.Info("PartitioningController lead working config updated.")
			break // Exit loop on success
		} else {
			slog.Error("PartitioningController leader failed to unmarshal current config.", "error", err)
			continue
		}
	}

	if i == RETRY_COUNT {
		slog.Error("PartitioningController leader failed to get current config after multiple attempts.")
		pc.dropLeadershipDuties()
		return nil // Exit leadership duties
	}

	// Main leadership loop
	for {
		select {
		case <-ctx.Done():
			slog.Info("PartitioningController leader duties context done, exiting.")
			return nil
		case <-time.After(PARTITIONING_INTERVAL): // Periodic partitioning check
			updated_config := false

			// Find all currently registered and active NoReDB services
			active_services, err := pc.getActiveNoReDBServices(ctx)
			if err != nil {
				slog.Error("PartitioningController leader failed to get active NoReDB services.", "error", err)
				continue
			}

			slog.Debug("Found active services: " + strings.Join(active_services, ", "))

			i := 0
			// Iterate over all partitions and assign them to active NoReDB services
			for partitionID := uint32(0); partitionID < pc.config.nPartitions; partitionID++ {
				// Simple round-robin assignment if no existing assignments
				val, ok := current_config.mServicePartition[partitionID]

				// If no existing assignment
				if !ok || len(val) == 0 {
					updated_config = true
					// Assign partition
					assigned_services := make([]string, 0)
					for r := uint32(0); r < pc.config.nReplicas; r++ {
						assigned_services = append(assigned_services, active_services[i%len(active_services)])
						i++
					}
				} else {
					// Check existing assignments and reassign if any assigned service is no longer active
					for i, serviceID := range val {
						found := false
						// Check if assigned service is still active
						for _, activeServiceID := range active_services {
							if serviceID == activeServiceID {
								found = true
								break
							}
						}

						// If the assigned service is not found among active services - reassign a new service endpoint to the partition
						if !found {
							updated_config = true // Updated config as we had to reassign some partitions
							slog.Info("Reassigning partition to a new active service.")

							// Assign a new active service
							val[i] = active_services[i%len(active_services)]
							i++

							break
						}
					}
				}
			}

			// If configuration was updated, write it back to Veritas
			if updated_config {
				i := 0
				time_retry := RETRY_INTERVAL
				for ; i < RETRY_COUNT; i++ { // Retry loop for updating config
					slog.Info("PartitioningController leader updating partitioning config in Veritas.")
					configBytes, err := json.Marshal(current_config)
					if err != nil {
						slog.Error("PartitioningController leader failed to marshal updated config.", "error", err)
						break
					}
					ok, err := pc.vClient.SetVariable(ctx, VERITAS_PARTITIONING_SERVICE_KEY, string(configBytes))
					if err != nil || !ok {
						slog.Error("PartitioningController leader failed to update config in Veritas.", "error", err)
						time.Sleep(time_retry) // Wait before retrying
						time_retry *= RETRY_BACKOFF_FACTOR
						continue
					}
					slog.Info("PartitioningController leader successfully updated partitioning config in Veritas.")
					break
				}

				if i == 5 {
					slog.Error("PartitioningController leader failed to update config in Veritas after multiple attempts.")

					pc.dropLeadershipDuties()
				}
			}

		}
	}
}

func (pc *PartitioningController) startFollowerDuties(ctx context.Context) error {
	// Placeholder for follower-specific duties, such as syncing with the leader
	return nil
}

// GetPartition retrieves the partition ID for given pixel coordinates.
func (pc *PartitioningController) GetPartition(x uint16, y uint16) (uint32, error) {
	key := pixelhasher.PixelToKey(x, y)

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	// Simple modulo-based partitioning (this is safe because the number of partitions is fixed in the config and can not change)
	// What changes is the mapping of partitions to actual database instances, which is handled elsewhere.
	partitionID := key % pc.config.nPartitions
	return partitionID, nil
}

// GetPartitionReplicas retrieves the list of replicas for a given partition ID.
func (pc *PartitioningController) GetPartitionReplicas(partitionID uint32) []string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	replicas, ok := pc.config.mServicePartition[partitionID]
	if !ok {
		return nil
	}
	return replicas
}

type ServiceNotFound struct{}

func (e *ServiceNotFound) Error() string {
	return "Service-Endpoint not found for partition."
}

type NoReplicaAvailable struct{}

func (e *NoReplicaAvailable) Error() string {
	return "No replica available for partition."
}

func (pc *PartitioningController) GetClientsForPartition(ctx context.Context, partitionID uint32) ([]noredbclient.NoreDBClient, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	replicaIDs, ok := pc.config.mServicePartition[partitionID]
	if !ok {
		return nil, &NoReplicaAvailable{}
	}

	clients := make([]noredbclient.NoreDBClient, 0)
	for _, replicaID := range replicaIDs {
		client, ok := pc.noredb_clients[replicaID]
		if ok {
			clients = append(clients, client)
		} else {
			slog.Debug("No NoreDB client found for replica ID: " + replicaID)

			// Creating a new client for this replica
			pc.mu.RUnlock() // Unlock read lock before acquiring write lock
			pc.mu.Lock()
			// Check again to avoid race condition
			client, ok = pc.noredb_clients[replicaID]
			if ok {
				clients = append(clients, client)
				pc.mu.Unlock()
				pc.mu.RLock() // Reacquire read lock
			} else {

				// Get service endpoint for this replica
				nordb_services, err := pc.noredb_service_handler.ResolveService(ctx)
				if err != nil {
					slog.Error("Failed to resolve NoreDB service for replica ID: "+replicaID, "error", err)
					pc.mu.Unlock()
					return nil, err
				}

				var found_endpoint *veritasclient.ServiceEndpoint
				for _, endpoint := range nordb_services.Endpoints {
					if endpoint.ID == replicaID {
						found_endpoint = &endpoint
						break
					}
				}

				if found_endpoint == nil {
					slog.Error("No endpoint found for NoreDB replica ID: " + replicaID)
					return nil, &ServiceNotFound{}
				}

				new_client := noredbclient.NewNoreDBClient(fmt.Sprintf("%s:%d", found_endpoint.Address, found_endpoint.Port))
				if new_client != nil {
					pc.noredb_clients[replicaID] = *new_client
					clients = append(clients, *new_client)
					pc.mu.Unlock()
					pc.mu.RLock() // Reacquire read lock
				}
			}
		}
	}

	return clients, nil
}

func (pc *PartitioningController) switchRoles(newRole uint8, ctx context.Context) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.role != newRole {
		// Cancel current duties
		if pc.cancel_duties != nil {
			pc.cancel_duties()
		}
		// Create a new context for the new role's duties
		ctx_duties, cancel_duties := context.WithCancel(ctx)
		pc.cancel_duties = cancel_duties
		pc.role = newRole
		if newRole == ROLE_LEADER {
			go pc.startLeadershipDuties(ctx_duties)
		} else {
			go pc.startFollowerDuties(ctx_duties)
		}
	}
	return nil
}

func (pc *PartitioningController) startLeaderElection(ctx context.Context) error {
	go func() {
		select {
		case <-ctx.Done():
			slog.Info("PartitioningController leader election context done, exiting.")
			return
		default:
			// Wait before next election attempt
			time.Sleep(LEADER_ELECTION_INTERVAL)
			retry := true
			for retry {
				retry = false // Assume no retry unless needed
				// If I am the leader, try to revalidate leadership
				if pc.role == ROLE_LEADER {
					service_reg, err := pc.service_handler.ResolveService(ctx)
					if err != nil {
						slog.Error("Error resolving service during leader election.", "error", err)
						pc.switchRoles(ROLE_FOLLOWER, ctx)
					} else {
						// Check if I am still the leader
						if service_reg.Meta["leader_id"] != pc.id && pc.role == ROLE_LEADER {
							slog.Info("Lost leadership, switching to follower role.")

							// Switch to follower role
							pc.switchRoles(ROLE_FOLLOWER, ctx)
						} else {
							slog.Debug("Retained leadership.")
						}
					}
				} else {
					service_reg, err := pc.service_handler.ResolveService(ctx)
					current_service_reg := (*service_reg).Clone()

					if err != nil {
						slog.Error("Error resolving service during leader election.", "error", err)
					} else {
						// Check if I am the leader
						if service_reg.Meta["leader_id"] == pc.id {
							slog.Info("Gained leadership, switching to leader role.")
							pc.switchRoles(ROLE_LEADER, ctx)
							// Sleep breifly to avoid concurrent leaders (In this case no leader is acceptable for even a few seconds as leadership is only
							// responsible for config updates and normal operation can continue without it).
							time.Sleep(LEADER_ELECTION_INTERVAL)
						} else {
							slog.Debug("Remaining follower.")

							// Check if current leader is alive
							leader := service_reg.Meta["leader_id"]
							leader_alive := false
							for _, endpoint := range service_reg.Endpoints {
								if endpoint.ID == leader && time.Since(endpoint.Timestamp) < SERVICE_REGISTRATION_TIMEOUT {
									leader_alive = true
									break
								}
							}

							// If leader is not alive, attempt to become leader
							if !leader_alive {
								slog.Info("Current leader appears down, attempting to become leader.")
								service_reg.Meta["leader_id"] = pc.id
								err := pc.service_handler.RegisterOrUpdateService(ctx, service_reg, &current_service_reg)

								// The service registration has changed between our read and write, someone else has likely become leader
								// in the meantime. But to be sure, we will check and retry if needed.
								if err == veritasclient.ErrServiceRegistrationChanged {
									slog.Info("Failed to become leader, another instance may have become leader first, checking...")

									updated_service_reg, err := pc.service_handler.ResolveService(ctx)
									if err != nil {
										slog.Error("Error resolving service during leader election.", "error", err)
									}

									// Check if I am still not the leader
									if updated_service_reg.Meta["leader_id"] != pc.id {
										leader_id := updated_service_reg.Meta["leader_id"]
										// Check if leader is alive
										leader_alive := false
										for _, endpoint := range updated_service_reg.Endpoints {
											if endpoint.ID == leader_id && time.Since(endpoint.Timestamp) < SERVICE_REGISTRATION_TIMEOUT {
												leader_alive = true
												break
											}
										}

										// If leader is not alive, retry to become leader without waiting for next interval
										if !leader_alive {
											slog.Info("Another instance became leader first, but it appears down, will retry to become leader soon: " + leader_id)
											retry = true
										} else {
											slog.Info("Another instance became leader first: " + leader_id)
										}
									}
								} else if err != nil {
									slog.Error("Error attempting to become leader.", "error", err)
								}
							} else {
								slog.Debug("Accepting leadership from " + leader)
							}
						}
					}
				}
			}

		}
	}()

	return nil
}

func startListeningForConfigChanges(ctx context.Context, vClient *veritasclient.VeritasClient, pc *PartitioningController) (<-chan error, error) {
	error_chan := make(chan error)
	config_changed_chan, err_chan := vClient.WatchVariablesAutoReconnect(ctx, []string{VERITAS_PARTITIONING_SERVICE_KEY})

	// Listen for config changes
	go func() {
		defer close(error_chan)
		for {
			select {
			case value := <-config_changed_chan:
				var newConfig PartitioningControllerConfig
				if err := json.Unmarshal([]byte(value.NewValue), &newConfig); err == nil {
					slog.Info("PartitioningController config updated via Veritas variable watch.")
					// Update the actual controller's config pointer
					pc.mu.Lock()
					pc.config = &newConfig
					pc.mu.Unlock()
				} else {
					slog.Error("Failed to unmarshal updated PartitioningController config.", "error", err)
				}
			case err := <-err_chan:
				error_chan <- err
				slog.Error("Received an error while watching variable.")
			}
		}
	}()

	return error_chan, nil
}

// startUpdateGlobalTime periodically updates the global time from Veritas.
func (pc *PartitioningController) startUpdateGlobalTime(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("PartitioningController global time update context done, exiting.")
			return
		default:
			// Get current global time from Veritas
			value, err := pc.vClient.GetAndAddVariable(ctx, VERITAS_TIME_SERVICE_KEY)
			if err != nil {
				slog.Error("PartitioningController failed to get global time from Veritas.", "error", err)
			} else {
				var global_time uint64
				global_time = uint64(value)
				pc.global_time.Store(global_time)
				slog.Debug(fmt.Sprintf("PartitioningController updated global time to %d.", global_time))
			}
		}
	}
}

func NewPartitioningController(id string, vClient *veritasclient.VeritasClient, service_registration_handler *veritasclient.ServiceRegistrationHandler) (*PartitioningController, <-chan error, error) {
	ctx := context.Background()

	value, err := vClient.GetVariable(ctx, VERITAS_PARTITIONING_SERVICE_KEY)
	if err != nil {
		return nil, nil, err
	}

	// Convert value to configuration
	var config PartitioningControllerConfig
	if err := json.Unmarshal([]byte(value), &config); err != nil {
		return nil, nil, err
	}

	noredb_service_handler, err := veritasclient.NewServiceRegistrationHandler(ctx, vClient, NOREDB_SERVICE_REGISTRATION_KEY)
	if err != nil {
		return nil, nil, err
	}

	pc := &PartitioningController{
		id:                     id,
		config:                 &config,
		vClient:                vClient,
		service_handler:        service_registration_handler,
		role:                   ROLE_FOLLOWER,
		noredb_service_handler: noredb_service_handler,
		noredb_clients:         make(map[string]noredbclient.NoreDBClient),
	}

	// Create a context for duties that can be cancelled on role change
	duties_ctx, cancel_duties := context.WithCancel(ctx)
	pc.cancel_duties = cancel_duties
	pc.startFollowerDuties(duties_ctx)

	// Start listening for config changes
	error_chan, err := startListeningForConfigChanges(ctx, vClient, pc)
	if err != nil {
		return nil, nil, err
	}

	// Start leader election (making sure that a leader is responsible for config updates)
	err = pc.startLeaderElection(ctx)
	if err != nil {
		return nil, nil, err
	}

	go pc.startUpdateGlobalTime(ctx)

	return pc, error_chan, nil
}

func (pc *PartitioningController) GetConfig() *PartitioningControllerConfig {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.config
}

type Color struct {
	R uint8
	G uint8
	B uint8
}

func (c Color) ToBytes() []byte {
	return []byte{c.R, c.G, c.B}
}

func ColorFromBytes(data []byte) Color {
	if len(data) != 3 {
		return Color{0, 0, 0}
	}
	return Color{data[0], data[1], data[2]}
}

type Data struct {
	x   uint16
	y   uint16
	val Color
}

type QuorumNotReached struct{}

func (e *QuorumNotReached) Error() string {
	return "Quorum not reached for operation."
}

type Timestamp struct {
	LocalClock  uint64
	GlobalClock uint64
}

// GetTimestampBytes converts a Timestamp struct into a 16-byte big-endian representation.
func (ts *Timestamp) GetTimestampBytes() []byte {
	uint128_bytes := make([]byte, 16)
	binary.BigEndian.PutUint64(uint128_bytes[0:8], ts.GlobalClock)
	binary.BigEndian.PutUint64(uint128_bytes[8:16], ts.LocalClock)

	return uint128_bytes
}

func TimestampFromBytes(data []byte) Timestamp {
	if len(data) != 16 {
		return Timestamp{0, 0}
	}
	return Timestamp{
		GlobalClock: binary.BigEndian.Uint64(data[0:8]),
		LocalClock:  binary.BigEndian.Uint64(data[8:16]),
	}
}

// Cmp compares two Timestamps.
func (ts *Timestamp) Cmp(other *Timestamp) int {
	if ts.GlobalClock < other.GlobalClock {
		return -1
	}
	if ts.GlobalClock > other.GlobalClock {
		return 1
	}
	if ts.LocalClock < other.LocalClock {
		return -1
	}
	if ts.LocalClock > other.LocalClock {
		return 1
	}
	return 0
}

func (pc *PartitioningController) getCurrentTimestamp() Timestamp {
	return Timestamp{
		LocalClock:  pc.local_time.Add(1),  // Increment local clock
		GlobalClock: pc.global_time.Load(), // Use current global clock
	}
}

// Get retrieves data based on partitioning logic.
func (pc *PartitioningController) Get(ctx context.Context, x uint16, y uint16) (*Color, error) {
	index := pc.op_index.Add(1) // Increment operation index
	slog.Debug(fmt.Sprintf("Starting Get operation %d for pixel (%d, %d).", index, x, y))

	// Get clients for the partition
	partitionID, err := pc.GetPartition(x, y)
	if err != nil {
		return nil, err
	}
	clients, err := pc.GetClientsForPartition(context.Background(), partitionID)
	if err != nil {
		return nil, err
	}

	var completed_reads atomic.Uint32
	var waiting atomic.Uint32

	var waitGroup sync.WaitGroup
	waitGroup.Add(int(pc.config.nReads))
	waiting.Store(pc.config.nReads) // Number of reads we wait for
	var mu sync.Mutex
	readValues := make([]noredbclient.PixelResponse, 0)
	clientRead := make([]int, len(clients))

	for i, client := range clients {
		go func(i int, client noredbclient.NoreDBClient) {
			value_bytes, err := client.Get(ctx, index, pixelhasher.PixelToKey(x, y))
			if err != nil {
				slog.Error("Failed to get data from NoReDB client.", "error", err)

				// Remove client
				pc.mu.Lock()
				client.Close()
				// Find and delete the client from the map
				for replicaID, c := range pc.noredb_clients {
					if c == client {
						delete(pc.noredb_clients, replicaID)
						break
					}
				}
				pc.mu.Unlock()

				// If error, do not count this as a completed read
				if waiting.Add(1) < uint32(len(clients)) {
					waitGroup.Add(1)
				}
			} else {
				completed_reads.Add(1)

				mu.Lock()
				readValues = append(readValues, *value_bytes)
				clientRead[len(clientRead)-1] = i // Mark which client responded
				mu.Unlock()
			}
		}(i, client)
	}

	waitGroup.Wait() // Wait for all necessary reads to complete

	// Check if quorum is reached and return error if not
	if uint32(completed_reads.Load()) < pc.config.nReads {
		return nil, &QuorumNotReached{}
	}

	// Find the value with the latest timestamp
	var latestValue *noredbclient.PixelResponse
	consensus := true // Assume consensus until proven otherwise (all read values are the same)

	for _, val := range readValues {
		if latestValue == nil {
			latestValue = &val
		} else {
			ts_current := TimestampFromBytes(val.Timestamp)
			ts_latest := TimestampFromBytes(latestValue.Timestamp)

			// Check for consensus
			if ts_current.Cmp(&ts_latest) != 0 {
				consensus = false
			}

			if ts_current.Cmp(&ts_latest) > 0 {
				latestValue = &val
			}
		}
	}

	if latestValue == nil {
		return nil, nil // No value found
	}

	// If consensus is reached, return the value directly
	if consensus {
		slog.Debug(fmt.Sprintf("Quorum read reached consensus for pixel (%d, %d).", x, y))
		color := ColorFromBytes(latestValue.PixelData)
		return &color, nil // All values are the same, no need for read-repair
	}

	clientsToWriteBack := make([]int, 0)
	for i, val := range readValues {
		ts_current := TimestampFromBytes(val.Timestamp)
		ts_latest := TimestampFromBytes(latestValue.Timestamp)
		if ts_current.Cmp(&ts_latest) != 0 {
			clientsToWriteBack = append(clientsToWriteBack, i)
		}
	}

	// Perform read-repair to update stale replicas
	slog.Info(fmt.Sprintf("Quorum read did not reach consensus for pixel (%d, %d), performing read-repair.", x, y))

	var writeComplete atomic.Uint32
	writeComplete.Store(pc.config.nReplicas - uint32(len(clientsToWriteBack))) // Count already up-to-date replicas

	var waitGroupWrite sync.WaitGroup
	waitGroupWrite.Add(int(pc.config.nWrites - writeComplete.Load())) // Number of writes we wait for (needed to reach quorum)

	ops_completed := atomic.Uint32{}
	ops_completed.Store(0)

	// Write back the latest value to all replicas to ensure consistency (read-repair)
	for _, clientIndex := range clientsToWriteBack {
		client := clients[clientIndex] // Get the corresponding client that needs to be updated
		go func() {
			err := client.Set(ctx, index, pixelhasher.PixelToKey(x, y), latestValue.PixelData, latestValue.Timestamp)
			if err != nil {
				slog.Error("Failed to write back data to NoReDB client during read-repair.", "error", err)

				// If error, do not count this as a completed write
				if ops_completed.Add(1) < uint32(len(clientsToWriteBack)) {
					waitGroupWrite.Add(1)
				}
			} else {
				writeComplete.Add(1)
				ops_completed.Add(1)
			}
			waitGroupWrite.Done()
		}()
	}
	waitGroupWrite.Wait() // Wait for all write-backs to complete

	if writeComplete.Load() < pc.config.nWrites {
		slog.Warn(fmt.Sprintf("Read-repair quorum not reached for pixel (%d, %d), writes successful: %d", x, y, writeComplete.Load()))
		return nil, &QuorumNotReached{} // Indicate that read-repair quorum was not reached
	} else {
		slog.Info(fmt.Sprintf("Read-repair quorum reached for pixel (%d, %d), writes successful: %d", x, y, writeComplete.Load()))
	}
	slog.Debug(fmt.Sprintf("Read-repair completed for pixel (%d, %d), writes successful: %d", x, y, writeComplete.Load()))

	color := ColorFromBytes(latestValue.PixelData)
	return &color, nil
}

// Set sets data based on partitioning logic.
func (pc *PartitioningController) Set(ctx context.Context, x uint16, y uint16, value Color) error {
	index := pc.op_index.Add(1) // Get a new operation index

	// Get clients for the partition
	partitionID, err := pc.GetPartition(x, y)
	if err != nil {
		return err
	}
	clients, err := pc.GetClientsForPartition(context.Background(), partitionID)
	if err != nil {
		return err
	}

	var completed_reads atomic.Uint32
	var waiting atomic.Uint32

	var waitGroup sync.WaitGroup
	waitGroup.Add(int(pc.config.nWrites))
	waiting.Store(pc.config.nWrites) // Number of writes we wait for

	time := pc.getCurrentTimestamp()

	for _, client := range clients {
		go func() {
			err := client.Set(ctx, index, pixelhasher.PixelToKey(x, y), value.ToBytes(), time.GetTimestampBytes())
			if err != nil {
				slog.Error("Failed to set data on NoReDB client.", "error", err)

				// Remove client
				pc.mu.Lock()
				client.Close()

				// Find and delete the client from the map
				for replicaID, c := range pc.noredb_clients {
					if c == client {
						delete(pc.noredb_clients, replicaID)
						break
					}
				}

				pc.mu.Unlock()

				// If error, do not count this as a completed write
				if waiting.Add(1) < uint32(len(clients)) {
					waitGroup.Add(1)
				}
			} else {
				completed_reads.Add(1)
			}
			waitGroup.Done()
		}()
	}

	waitGroup.Wait() // Wait for all necessary writes to complete

	// Check if quorum is reached and return error if not
	if uint32(completed_reads.Load()) < pc.config.nWrites {
		return &QuorumNotReached{}
	}

	// Placeholder for setting data based on partitioning
	return nil
}

// GetAll retrieves all data across partitions.
func (pc *PartitioningController) GetAll() (<-chan Data, error) {
	// TODO Placeholder for getting all data across partitions
	dataChan := make(chan Data)
	go func() {
		close(dataChan)
	}()
	return dataChan, nil
}
