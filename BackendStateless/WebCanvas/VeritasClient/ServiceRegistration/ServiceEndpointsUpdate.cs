// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Represents changes in service endpoints.
/// </summary>
public class ServiceEndpointsUpdate
{
    public required List<ServiceEndpoint> AddedEndpoints { get; set; }
    public required List<ServiceEndpoint> RemovedEndpoints { get; set; }
    public required List<ServiceEndpoint> UpdatedEndpoints { get; set; }
}
