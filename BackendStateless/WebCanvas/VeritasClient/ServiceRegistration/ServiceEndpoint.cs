// Translated from Go implementation using Claude Sonnet

using System.Text.Json.Serialization;

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Represents a service endpoint with address, port, and metadata.
/// </summary>
public class ServiceEndpoint
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("address")]
    public string Address { get; set; } = string.Empty;

    [JsonPropertyName("port")]
    public ushort Port { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    public override string ToString()
    {
        return $"{Address}:{Port}";
    }

    /// <summary>
    /// Checks if two ServiceEndpoints are equal (by address and port).
    /// </summary>
    public bool Equals(ServiceEndpoint? other)
    {
        if (other == null) return false;
        return Address == other.Address && Port == other.Port;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as ServiceEndpoint);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Address, Port);
    }

    /// <summary>
    /// Creates a deep copy of this endpoint.
    /// </summary>
    public ServiceEndpoint Clone()
    {
        return new ServiceEndpoint
        {
            Id = Id,
            Address = Address,
            Port = Port,
            Timestamp = Timestamp
        };
    }
}
