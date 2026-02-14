// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Interface for service registration operations.
/// </summary>
public interface IServiceRegistrationHandler
{
    /// <summary>
    /// Registers a new service or updates an existing one with the service registry.
    /// </summary>
    /// <param name="service">The ServiceRegistration object containing service details</param>
    /// <param name="oldService">The previous ServiceRegistration object for updates (null for new registrations)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the async operation</returns>
    Task RegisterOrUpdateServiceAsync(
        ServiceRegistration service,
        ServiceRegistration? oldService = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers or updates a service endpoint.
    /// </summary>
    /// <param name="endpoint">The ServiceEndpoint to be registered</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the async operation</returns>
    Task RegisterOrUpdateEndpointAsync(
        ServiceEndpoint endpoint,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a listener for service registration updates.
    /// The callback will be called whenever there is a change in service registrations.
    /// </summary>
    /// <param name="callback">A function that takes a ServiceRegistration</param>
    void AddListener(Action<ServiceRegistration> callback);

    /// <summary>
    /// Resolves the current service registration information.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>ServiceRegistration if successful</returns>
    Task<ServiceRegistration> ResolveServiceAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a listener for endpoint updates.
    /// The callback will be called whenever there is a change in service endpoints.
    /// </summary>
    /// <param name="callback">A function that takes a ServiceEndpointsUpdate</param>
    void AddEndpointListener(Action<ServiceEndpointsUpdate> callback);

    /// <summary>
    /// Cleanup old endpoints that are no longer valid based on timeout.
    /// </summary>
    /// <param name="timeout">Timeout duration for endpoint validity</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the async operation</returns>
    Task TryCleanupOldEndpointsAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
}
