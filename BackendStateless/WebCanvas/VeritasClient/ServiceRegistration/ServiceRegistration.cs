// Translated from Go implementation using Claude Sonnet

using System.Text.Json.Serialization;

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Represents a service registration with endpoints and metadata.
/// </summary>
public class ServiceRegistration
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("endpoints")]
    public List<ServiceEndpoint> Endpoints { get; set; } = new();

    [JsonPropertyName("meta")]
    public Dictionary<string, string> Meta { get; set; } = new();

    /// <summary>
    /// Creates a deep copy of the ServiceRegistration.
    /// </summary>
    public ServiceRegistration Clone()
    {
        return new ServiceRegistration
        {
            Id = Id,
            Endpoints = Endpoints.Select(e => e.Clone()).ToList(),
            Meta = new Dictionary<string, string>(Meta)
        };
    }
}
