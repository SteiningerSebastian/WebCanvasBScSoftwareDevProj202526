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

// Threshold for partition imbalance between NoReDB services before rebalancing is triggered
const NOREDB_PARTITION_INBALANCE_THRESHOLD = 16 // Max allowed partition imbalance between NoReDB services before rebalancing is triggered

const SERVICE_REGISTRATION_TIMEOUT = 30 * time.Second
const LEADER_ELECTION_INTERVAL = 5 * time.Second

const ROLE_LEADER uint8 = 1
const ROLE_FOLLOWER uint8 = 2

const RETRY_COUNT = 5
const RETRY_INTERVAL = 200 * time.Millisecond
const RETRY_BACKOFF_FACTOR = 2

const PARTITIONING_INTERVAL = 10 * time.Second
const REPARTITIONING_INTERVAL = 100 * time.Millisecond // Time between repartitioning checks (shorter than partitioning interval to allow quick reaction to changes)

const MAX_CLOCK_TICK = 100 * time.Millisecond // Time between queries to update the global clock

const LEADERSHIP_COOLDOWN = 128 * time.Second // Time to wait before repartitioning etc. after gaining leadership (allow system to stabilize)

const READ_TIMEOUT = 5 * time.Second  // Timeout for read operations to NoReDB services
const WRITE_TIMEOUT = 5 * time.Second // Timeout for write operations to NoReDB services

type PartitioningControllerConfig struct {
	NPartitions       uint32              // Total number of partitions - must not change else data consistency breaks
	NReplicas         uint32              // Number of replicas for each partition (This is for quorum read/writes, failover is implemented but takes longer and loosing quorum while reconstructing can lead to data loss)
	NReads            uint32              // Number of replicas to read from for quorum reads
	NWrites           uint32              // Number of replicas to write to for quorum writes
	MServicePartition map[uint32][]string // Map partition ID to list of NoReDB service IDs responsible for that partition
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
func (pc *PartitioningController) getActiveNoReDBServices(ctx context.Context) ([]string, bool, error) {
	service_reg, err := pc.noredb_service_handler.ResolveService(ctx)
	if err != nil {
		slog.Error("Error resolving NoReDB service.", "error", err)
		return nil, false, err
	}

	active_services := make([]string, 0)
	for _, endpoint := range service_reg.Endpoints {
		if time.Since(endpoint.Timestamp) < NOREDB_SERVICE_REGISTRATION_TIMEOUT_PARTITIONING {
			active_services = append(active_services, endpoint.ID)
		} else {
			slog.Info("NoReDB service endpoint considered inactive for partitioning: " + endpoint.ID)
		}
	}

	return active_services, len(active_services) == len(service_reg.Endpoints), nil
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

// transferPartition transfers all data for a given partition from the old service to the new service.
func (pc *PartitioningController) transferPartition(ctx context.Context, partitionId uint32, old string, new string) error {
	if old == new {
		return nil
	}

	// Stream everything from old to new
	clients, err := pc.getClientsForPartition(context.Background(), partitionId)
	if err != nil {
		slog.Error("Error getting clients for partition during transfer.", "error", err)
		return err
	}

	replicaIDs, ok := pc.config.MServicePartition[partitionId]

	if !ok {
		slog.Error("No replicas found for partition during transfer.", "partitionId", partitionId)
		return &NoReplicaAvailable{}
	}

	var oldClient *noredbclient.NoreDBClient
	var newClient *noredbclient.NoreDBClient

	for i, replicaID := range replicaIDs {
		if replicaID == old {
			oldClient = &clients[i]
		}
		if replicaID == new {
			newClient = &clients[i]
		}
	}

	if oldClient == nil || newClient == nil {
		slog.Error("Old or new client not found for partition during transfer.", "partitionId", partitionId, "old", old, "new", new)
		return &ServiceNotFound{}
	}

	dataChan, errChan, err := oldClient.GetAll(ctx, 0)

	if err != nil {
		slog.Error("Error starting GetAll for partition transfer.", "error", err)
		return err
	}

	for {
		select {
		case data, ok := <-dataChan:
			if !ok {
				slog.Info("Completed data transfer for partition.", "partitionId", partitionId, "old", old, "new", new)
				return nil
			}

			// Check if key is in the partition
			partition := data.Key % pc.config.NPartitions

			// If not, skip
			if partition != partitionId {
				continue
			}

			// Write data to new client
			newClient.Set(ctx, 0, data.Key, data.PixelData, data.Timestamp)
		case err := <-errChan:
			if err != nil {
				slog.Error("Error during data transfer for partition.", "partitionId", partitionId, "old", old, "new", new, "error", err)
				return err
			}
		}
	}
}

func (pc *PartitioningController) startLeadershipDuties(ctx context.Context) error {
	// Make sure that everything is started up and working before starting leadership duties
	time.Sleep(LEADERSHIP_COOLDOWN)

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
			// Initialize map if nil
			if current_config.MServicePartition == nil {
				current_config.MServicePartition = make(map[uint32][]string)
			}

			// Copy nPartitions from controller config if not set
			if current_config.NPartitions == 0 {
				current_config.NPartitions = pc.config.NPartitions
				current_config.NReplicas = pc.config.NReplicas
				current_config.NReads = pc.config.NReads
				current_config.NWrites = pc.config.NWrites
			}
			slog.Info("PartitioningController leader initialized working config.", "nPartitions", current_config.NPartitions, "nReplicas", current_config.NReplicas, "nReads", current_config.NReads, "nWrites", current_config.NWrites)

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

	partitioningCheckTimeout := time.After(PARTITIONING_INTERVAL)
	// Main leadership loop
	for lc := 0; ; lc++ {
		select {
		case <-ctx.Done():
			slog.Info("PartitioningController leader duties context done, exiting.")
			return nil
		case <-partitioningCheckTimeout: // Periodic partitioning check
			partitioningCheckTimeout = time.After(PARTITIONING_INTERVAL) // Reset timer
			slog.Info(fmt.Sprintf("PartitioningController leader performing partitioning check. Loop count: %d", lc))
			updated_config := false

			// Find all currently registered and active NoReDB services
			active_services, _, err := pc.getActiveNoReDBServices(ctx)
			if err != nil {
				slog.Error("PartitioningController leader failed to get active NoReDB services.", "error", err)
				continue
			}

			if len(active_services) < int(current_config.NReplicas) {
				slog.Warn("PartitioningController leader found insufficient active NoReDB services for partitioning.", "active_services", len(active_services), "required_replicas", current_config.NReplicas)
				continue
			}

			slog.Debug("Found active services: " + strings.Join(active_services, ", "))

			// Count the number of partitions assigned to each active NoReDB service
			partition_count := make(map[string]int)
			for partitionID := uint32(0); partitionID < current_config.NPartitions; partitionID++ {
				val, ok := current_config.MServicePartition[partitionID]
				if ok {
					for _, serviceID := range val {
						partition_count[serviceID]++
					}
				}
			}

			// Check for partition imbalance
			rebalanceNeeded := false
			min := int(^uint(0) >> 1) // Max int
			minService := ""
			max := 0
			maxService := ""
			for _, serviceID := range active_services {
				count, ok := partition_count[serviceID]
				if !ok {
					count = 0
				}
				if count < min {
					min = count
					minService = serviceID
				}
				if count > max {
					max = count
					maxService = serviceID
				}
			}
			if max-min > NOREDB_PARTITION_INBALANCE_THRESHOLD {
				slog.Info(fmt.Sprintf("PartitioningController leader detected partition imbalance (max: %d, min: %d), rebalancing partitions.", max, min))
				updated_config = true
				rebalanceNeeded = true
			}

			i := 0
			// Iterate over all partitions and assign them to active NoReDB services
			for partitionID := uint32(0); partitionID < current_config.NPartitions; partitionID++ {
				// Simple round-robin assignment if no existing assignments
				val, ok := current_config.MServicePartition[partitionID]

				// If no existing assignment
				if !ok || len(val) == 0 {
					slog.Info(fmt.Sprintf("Assigning partition to active services. Partition ID: %d", partitionID))
					updated_config = true
					// Assign partition
					assigned_services := make([]string, 0)
					for r := uint32(0); r < current_config.NReplicas; r++ {
						assigned_services = append(assigned_services, active_services[i%len(active_services)])
						i++
					}

					current_config.MServicePartition[partitionID] = assigned_services
				} else {
					// Check existing assignments and reassign if any assigned service is no longer active
					for j, serviceID := range val {
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
							slog.Info(fmt.Sprintf("Reassigning partition from inactive service. Partition ID: %d, Inactive Service ID: %s", partitionID, serviceID))

							k := i
							for ; k < i+len(active_services); k++ {
								// Find the next active service that is not already assigned to this partition
								already_assigned := false
								for _, assignedServiceID := range val {
									if active_services[k%len(active_services)] == assignedServiceID {
										already_assigned = true
										break
									}
								}
								if !already_assigned {
									break
								}
							}

							// Assign a new active service
							val[j] = active_services[k%len(active_services)]
							i++
						}

						// Rebalance partitions if needed
						if rebalanceNeeded {
							// If the max service is assigned this partition, and the min service is not assigned, swap them
							if serviceID == maxService {
								already_assigned := false
								for _, assignedServiceID := range val {
									if assignedServiceID == minService {
										already_assigned = true
										break
									}
								}
								if !already_assigned {
									slog.Debug(fmt.Sprintf("Rebalancing partition assignment. Partition ID: %d, From Service ID: %s, To Service ID: %s", partitionID, maxService, minService))
									pc.transferPartition(ctx, partitionID, maxService, minService)

									// Only swap once every leader loop to avoid excessive data transfer and timeouts / keep system stable
									val[j] = minService
								}
							}

							// Set the timeout for next repartitioning check (shorter than partitioning interval to allow quick reaction to changes / new services)
							partitioningCheckTimeout = time.After(REPARTITIONING_INTERVAL)
						}
					}
				}
			}

			// If configuration was updated, write it back to Veritas
			if updated_config {
				slog.Info("PartitioningController leader detected configuration changes, updating Veritas.")
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

// getClients retrieves clients for all active NoReDB services.
func (pc *PartitioningController) getClients(ctx context.Context) ([]noredbclient.NoreDBClient, [][]int, error) {
	pc.mu.RLock()

	replicaIDs, _, err := pc.getActiveNoReDBServices(ctx)
	if err != nil {
		slog.Error("Error resolving NoReDB service.", "error", err)
		pc.mu.RUnlock()
		return nil, nil, &NoReplicaAvailable{}
	}

	clients := make([]noredbclient.NoreDBClient, 0)
	partitions := make([][]int, 0)

	for _, replicaID := range replicaIDs {
		clientPartitions := make([]int, 0)
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
			} else {

				// Get service endpoint for this replica
				nordb_services, err := pc.noredb_service_handler.ResolveService(ctx)
				if err != nil {
					slog.Error("Failed to resolve NoreDB service for replica ID: "+replicaID, "error", err)
					pc.mu.Unlock()
					return nil, nil, err
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
					pc.mu.Unlock()
					return nil, nil, &ServiceNotFound{}
				}

				new_client := noredbclient.NewNoreDBClient(fmt.Sprintf("%s:%d", found_endpoint.Address, found_endpoint.Port))
				if new_client != nil {
					pc.noredb_clients[replicaID] = *new_client
					clients = append(clients, *new_client)
				}
				pc.mu.Unlock()
			}

			pc.mu.RLock() // Reacquire read lock
		}

		// Find partitions for this replica
		for partitionID, serviceIDs := range pc.config.MServicePartition {
			// Check if this replica is assigned to the partition
			for _, serviceID := range serviceIDs {
				if serviceID == replicaID {
					clientPartitions = append(clientPartitions, int(partitionID))
					break
				}
			}
		}

		partitions = append(partitions, clientPartitions)
	}

	pc.mu.RUnlock()

	return clients, partitions, nil
}

// getPartition retrieves the partition ID for given pixel coordinates.
func (pc *PartitioningController) getPartition(x uint16, y uint16) (uint32, error) {
	key := pixelhasher.PixelToKey(x, y)

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	// Ensure configuration is initialized
	if pc.config.NPartitions == 0 {
		slog.Error("nPartitions is 0, configuration not initialized properly")
		return 0, fmt.Errorf("nPartitions is 0, configuration not initialized")
	}

	// Simple modulo-based partitioning (this is safe because the number of partitions is fixed in the config and can not change)
	// What changes is the mapping of partitions to actual database instances, which is handled elsewhere.
	partitionID := key % pc.config.NPartitions
	return partitionID, nil
}

// getPartitionReplicas retrieves the list of replicas for a given partition ID.
func (pc *PartitioningController) getPartitionReplicas(partitionID uint32) []string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	replicas, ok := pc.config.MServicePartition[partitionID]
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

func (pc *PartitioningController) getClientsForPartition(ctx context.Context, partitionID uint32) ([]noredbclient.NoreDBClient, error) {
	pc.mu.RLock()

	replicaIDs, ok := pc.config.MServicePartition[partitionID]
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
					pc.mu.Unlock()
					return nil, &ServiceNotFound{}
				}

				new_client := noredbclient.NewNoreDBClient(fmt.Sprintf("%s:%d", found_endpoint.Address, found_endpoint.Port))
				if new_client != nil {
					pc.noredb_clients[replicaID] = *new_client
					clients = append(clients, *new_client)
					pc.mu.Unlock()
				}
			}
			pc.mu.RLock() // Reacquire read lock
		}
	}

	pc.mu.RUnlock()
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
			slog.Info("Taking over leadership duties.")
			go pc.startLeadershipDuties(ctx_duties)
		} else {
			slog.Info("Starting follower duties.")
			go pc.startFollowerDuties(ctx_duties)
		}
	}
	return nil
}

func (pc *PartitioningController) startLeaderElection(ctx context.Context) error {
	go func() {
		for {
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
									} else {
										slog.Debug("Successfully became leader, switching to leader role.")
									}
								} else {
									slog.Debug("Accepting leadership from " + leader)
								}
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
				slog.Error("Received an error while watching variable.", "error", err)
			}
		}
	}()

	return error_chan, nil
}

// startUpdateGlobalTime periodically updates the global time from Veritas.
func (pc *PartitioningController) startUpdateGlobalTime(ctx context.Context) {
	ticker := time.NewTicker(MAX_CLOCK_TICK)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("PartitioningController global time update context done, exiting.")
			return
		case <-ticker.C:
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

	slog.Info("PartitioningController initializing with config from Veritas: " + value)

	// Convert value to configuration
	var config PartitioningControllerConfig
	if value == "" || value == "{}" || len(value) < 10 { // Basic check for empty or uninitialized config
		slog.Info("No existing PartitioningController config found in Veritas, initializing default config.")

		// Initialize default config if none exists
		config = PartitioningControllerConfig{
			NPartitions:       1024,
			NReplicas:         3,
			NReads:            2,
			NWrites:           2,
			MServicePartition: make(map[uint32][]string),
		}

		// Save default config to Veritas
		configBytes, err := json.Marshal(config)
		if err != nil {
			slog.Error("Failed to marshal default PartitioningController config.", "error", err)
			return nil, nil, err
		}

		slog.Info("Storing default PartitioningController config in Veritas.")
		slog.Debug(string(configBytes))

		_, err = vClient.SetVariable(ctx, VERITAS_PARTITIONING_SERVICE_KEY, string(configBytes))
		if err != nil {
			slog.Error("Failed to set default PartitioningController config in Veritas.", "error", err)
			return nil, nil, err
		}
	} else {
		slog.Info("Existing PartitioningController config found in Veritas, loading.")

		if err := json.Unmarshal([]byte(value), &config); err != nil {
			slog.Error("Failed to unmarshal existing PartitioningController config from Veritas.", "error", err)
			return nil, nil, err
		}
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
	X     uint16
	Y     uint16
	Value Color
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

// TODO revisit get and set logic and implementation!

// Get retrieves data based on partitioning logic.
func (pc *PartitioningController) Get(ctx context.Context, x uint16, y uint16) (*Color, error) {
	ctx, cancel := context.WithTimeout(ctx, READ_TIMEOUT)

	index := pc.op_index.Add(1) // Increment operation index
	slog.Debug(fmt.Sprintf("Starting Get operation %d for pixel (%d, %d).", index, x, y))

	// Get clients for the partition
	partitionID, err := pc.getPartition(x, y)
	if err != nil {
		cancel()
		return nil, err
	}

	slog.Debug(fmt.Sprintf("Pixel (%d, %d) maps to partition ID %d.", x, y, partitionID))

	clients, err := pc.getClientsForPartition(ctx, partitionID)
	if err != nil {
		cancel()
		return nil, err
	}

	slog.Debug(fmt.Sprintf("Retrieved clients for partition ID %d.", partitionID))

	var completed_reads atomic.Uint32
	var waiting atomic.Uint32
	var done atomic.Uint32 // Used to signal how many workers have finished
	done.Store(0)

	var waitGroup sync.WaitGroup
	waitGroup.Add(int(pc.config.NReads))
	waiting.Store(pc.config.NReads) // Number of reads we wait for
	var mu sync.Mutex
	readValues := make([]noredbclient.PixelResponse, 0)
	clientRead := make([]int, len(clients))

	slog.Debug(fmt.Sprintf("Initiating quorum read for pixel (%d, %d) with %d reads required.", x, y, pc.config.NReads))

	readctx, readCancel := context.WithCancel(ctx)

	// Perform reads in parallel
	for i, client := range clients {
		go func(i int, client noredbclient.NoreDBClient) {
			value_bytes, err := client.Get(readctx, index, pixelhasher.PixelToKey(x, y))
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

				mu.Lock()
				// If error, do not count this as a completed read
				if waiting.Add(1) < uint32(len(clients)) {
					waitGroup.Add(1)
				}
				mu.Unlock()
			} else {
				slog.Debug(fmt.Sprintf("Successfully got data from NoReDB client for pixel (%d, %d).", x, y))
				completed_reads.Add(1)

				mu.Lock()
				readValues = append(readValues, *value_bytes)
				clientRead[len(clientRead)-1] = i // Mark which client responded
				mu.Unlock()
			}

			mu.Lock() // TODO still fails on get
			// Signal that this read is done
			if done.Add(1) <= waiting.Load() {
				waitGroup.Done()
			}
			mu.Unlock()
		}(i, client)
	}

	slog.Debug("Waiting for a quarum to be achieved.")

	// Wait for either quorum to be achieved or context to be done
	select {
	case <-func() chan struct{} {
		done := make(chan struct{})
		go func() {
			waitGroup.Wait()
			close(done)
		}()
		return done
	}():
	case <-ctx.Done():
		slog.Warn("Get operation context done before quorum could be achieved.")
		cancel()
		readCancel()
		return nil, ctx.Err()
	}

	readCancel() // Cancel any remaining reads as quorum is achieved

	slog.Debug("Quorum read wait completed.")

	// Check if quorum is reached and return error if not
	if uint32(completed_reads.Load()) < pc.config.NReads {
		cancel()
		slog.Warn(fmt.Sprintf("Quorum not reached for Get operation %d for pixel (%d, %d). Completed reads: %d", index, x, y, completed_reads.Load()))
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
		slog.Warn(fmt.Sprintf("No valid value found in quorum read for pixel (%d, %d).", x, y))
		cancel()
		return nil, nil // No value found
	}

	// If consensus is reached, return the value directly
	if consensus {
		slog.Debug(fmt.Sprintf("Quorum read reached consensus for pixel (%d, %d).", x, y))
		color := ColorFromBytes(latestValue.PixelData)
		cancel()
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
	writeComplete.Store(pc.config.NReplicas - uint32(len(clientsToWriteBack))) // Count already up-to-date replicas

	var waitGroupWrite sync.WaitGroup
	waitGroupWrite.Add(int(pc.config.NWrites - writeComplete.Load())) // Number of writes we wait for (needed to reach quorum)

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

	if writeComplete.Load() < pc.config.NWrites {
		slog.Warn(fmt.Sprintf("Read-repair quorum not reached for pixel (%d, %d), writes successful: %d", x, y, writeComplete.Load()))
		cancel()
		return nil, &QuorumNotReached{} // Indicate that read-repair quorum was not reached
	} else {
		slog.Info(fmt.Sprintf("Read-repair quorum reached for pixel (%d, %d), writes successful: %d", x, y, writeComplete.Load()))
	}
	slog.Debug(fmt.Sprintf("Read-repair completed for pixel (%d, %d), writes successful: %d", x, y, writeComplete.Load()))

	color := ColorFromBytes(latestValue.PixelData)
	cancel()
	return &color, nil
}

// Set sets data based on partitioning logic.
func (pc *PartitioningController) Set(ctx context.Context, x uint16, y uint16, value Color) error {
	index := pc.op_index.Add(1) // Get a new operation index

	slog.Debug(fmt.Sprintf("Starting Set operation %d for pixel (%d, %d).", index, x, y))

	// Get clients for the partition
	partitionID, err := pc.getPartition(x, y)
	if err != nil {
		return err
	}
	clients, err := pc.getClientsForPartition(context.Background(), partitionID)
	if err != nil {
		return err
	}

	var mu sync.Mutex

	var completed_writes atomic.Uint32
	var waiting atomic.Uint32
	var done atomic.Uint32 // Used to signal how many workers have finished
	var waitGroup sync.WaitGroup
	waitGroup.Add(int(pc.config.NWrites))
	waiting.Store(pc.config.NWrites) // Number of writes we wait for

	time := pc.getCurrentTimestamp()
	slog.Debug(fmt.Sprintf("Using timestamp GlobalClock: %d, LocalClock: %d for Set operation %d for pixel (%d, %d).", time.GlobalClock, time.LocalClock, index, x, y))

	writectx, writeCancel := context.WithTimeout(ctx, WRITE_TIMEOUT)

	for _, client := range clients {
		go func() {
			err := client.Set(writectx, index, pixelhasher.PixelToKey(x, y), value.ToBytes(), time.GetTimestampBytes())
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

				mu.Lock()
				// If error, do not count this as a completed write
				if waiting.Add(1) < uint32(len(clients)) {
					waitGroup.Add(1)
				}
				mu.Unlock()
			} else {
				slog.Debug(fmt.Sprintf("Successfully set data on NoReDB client for pixel (%d, %d).", x, y))
				completed_writes.Add(1)
			}

			// Signal that this write is done
			mu.Lock()
			if done.Add(1) <= waiting.Load() {
				waitGroup.Done()
			}
			mu.Unlock()
		}()
	}

	waitGroup.Wait() // Wait for all necessary writes to complete

	writeCancel() // Cancel any remaining writes

	slog.Debug("Set operation write wait completed.")

	// Check if quorum is reached and return error if not
	if uint32(completed_writes.Load()) < pc.config.NWrites {
		return &QuorumNotReached{}
	}

	// Placeholder for setting data based on partitioning
	return nil
}

type errorWithIndex struct {
	err   error
	index int
}

// mergeDataChannels merges multiple data channels into a single channel.
func mergeErrorChannels(channels []<-chan error) <-chan errorWithIndex {
	merged := make(chan errorWithIndex)
	go func() {
		defer close(merged)
		var wg sync.WaitGroup
		wg.Add(len(channels))
		for i, ch := range channels {
			go func(c <-chan error, idx int) {
				defer wg.Done()
				for err := range c {
					merged <- errorWithIndex{err: err, index: idx}
				}
			}(ch, i)
		}
		wg.Wait()
	}()
	return merged
}

// mergeDataChannels merges multiple data channels into a single channel.
func mergeDataChannels(channels []<-chan *noredbclient.PixelResponse) <-chan *noredbclient.PixelResponse {
	merged := make(chan *noredbclient.PixelResponse)
	go func() {
		defer close(merged)
		var wg sync.WaitGroup
		wg.Add(len(channels))
		for _, ch := range channels {
			go func(c <-chan *noredbclient.PixelResponse) {
				defer wg.Done()
				for data := range c {
					merged <- data
				}
			}(ch)
		}
		wg.Wait()
	}()
	return merged
}

// GetAll retrieves all data across partitions. (Does not guarantee order or consistency)
func (pc *PartitioningController) GetAll(ctx context.Context) (<-chan Data, <-chan error, error) {
	dataChan := make(chan Data)
	errorChan := make(chan error)

	clients, paritionsAsignments, err := pc.getClients(ctx)

	if err != nil {
		return nil, nil, err
	}

	channels := make([]<-chan *noredbclient.PixelResponse, 0)
	errChannels := make([]<-chan error, 0)
	for _, client := range clients {
		dataChanClient, errChan, err := client.GetAll(context.Background(), pc.op_index.Add(1))
		if err != nil {
			continue
		}
		channels = append(channels, dataChanClient)
		errChannels = append(errChannels, errChan)
	}

	// Get read lock
	pc.mu.RLock()

	availableReplicasForPartition := make(map[uint32]int, 0)
	// Determine available replicas for each partition
	for partitionID := uint32(0); partitionID < pc.config.NPartitions; partitionID++ {
		replicas := pc.getPartitionReplicas(partitionID)
		availableReplicasForPartition[partitionID] = len(replicas)
	}

	// Release read lock
	pc.mu.RUnlock()

	responses := make(map[uint16][]*noredbclient.PixelResponse, 0)
	done := make(map[uint16]bool, 0)

	go func() {
		defer close(dataChan)

		// Merge data from all clients for this partition
		for {
			select {
			case errorWithIndex := <-mergeErrorChannels(errChannels):
				err := errorWithIndex.err
				idx := errorWithIndex.index

				slog.Error("Error received from NoReDB client during GetAll.", "error", err)

				// If an error occurs, we close and remove the client
				pc.mu.Lock()

				// Find and delete the client from the map
				for replicaID, c := range pc.noredb_clients {
					if c == clients[idx] {
						clients[idx].Close()
						delete(pc.noredb_clients, replicaID)
						break
					}
				}

				// Note that all partitions that this client was responsible for now have one less available replica
				for _, partitionID := range paritionsAsignments[idx] {
					availableReplicasForPartition[uint32(partitionID)] -= 1

					// If available replicas for this partition fall below read quorum, we can not continue
					if uint32(availableReplicasForPartition[uint32(partitionID)]) < pc.config.NReads {
						slog.Error("Not enough replicas available for partition during GetAll, aborting.", "partition_id", partitionID)
						errorChan <- &QuorumNotReached{}
						// Release lock
						pc.mu.Unlock()
						return
					}
				}

				// Release lock
				pc.mu.Unlock()
			case resp, ok := <-mergeDataChannels(channels):
				if !ok {
					// All channels are closed
					slog.Info("All NoReDB client data channels closed during GetAll.")
					return
				}

				// Process the received response if we haven't already completed it (multiple clients may return the same pixel)
				if !done[uint16(resp.Key)] {
					// Accumulate responses for each pixel
					responses[uint16(resp.Key)] = append(responses[uint16(resp.Key)], resp)

					// Check if we have enough responses for this pixel to reach read quorum
					if uint32(len(responses[uint16(resp.Key)])) >= pc.config.NReads {
						done[uint16(resp.Key)] = true

						// Find the value with the latest timestamp
						var latestValue *noredbclient.PixelResponse
						for _, val := range responses[uint16(resp.Key)] {
							if latestValue == nil {
								latestValue = val
							} else {
								ts_current := TimestampFromBytes(val.Timestamp)
								ts_latest := TimestampFromBytes(latestValue.Timestamp)
								if ts_current.Cmp(&ts_latest) > 0 {
									latestValue = val
								}
							}
						}
						if latestValue != nil {
							x, y := pixelhasher.KeyToPixel(latestValue.Key)
							dataChan <- Data{X: x, Y: y, Value: ColorFromBytes(latestValue.PixelData)}
						}

						// Remove accumulated responses for this pixel to free memory
						delete(responses, uint16(resp.Key))
					}
				}
			}
		}
	}()

	return dataChan, errorChan, nil
}
