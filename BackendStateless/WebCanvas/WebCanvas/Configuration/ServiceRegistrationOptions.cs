namespace WebCanvas.Configuration;

/// <summary>
/// Configuration options for service registration with Veritas.
/// </summary>
public class ServiceRegistrationOptions
{
    /// <summary>
    /// The name of the service to register in Veritas (e.g., "service-webcanvas").
    /// </summary>
    public string ServiceName { get; set; } = "service-webcanvas";

    /// <summary>
    /// Unique identifier for this endpoint. If empty, will be generated automatically.
    /// </summary>
    public string EndpointId { get; set; } = string.Empty;

    /// <summary>
    /// Interval in seconds between endpoint keep-alive updates.
    /// </summary>
    public int UpdateIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// Whether to use the pod hostname for Kubernetes deployments (FQDN).
    /// When true, uses the HOSTNAME environment variable (e.g., pod-name.service.namespace.svc.cluster.local).
    /// When false, uses the local network address.
    /// </summary>
    public bool UsePodHostname { get; set; } = true;
}
