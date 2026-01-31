// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Represents changes in service endpoints.
/// </summary>
public class ServiceEndpointsUpdate
{
    public List<ServiceEndpoint> AddedEndpoints { get; set; } = new();
    public List<ServiceEndpoint> RemovedEndpoints { get; set; } = new();
}
