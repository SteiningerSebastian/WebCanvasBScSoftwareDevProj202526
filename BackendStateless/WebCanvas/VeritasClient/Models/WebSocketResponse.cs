// Translated from Go implementation using Claude Sonnet

using System.Text.Json.Serialization;

namespace VeritasClient.Models;

public class WebSocketResponse
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = string.Empty;

    [JsonPropertyName("old_value")]
    public string OldValue { get; set; } = string.Empty;

    [JsonPropertyName("new_value")]
    public string NewValue { get; set; } = string.Empty;

    public UpdateNotification ToUpdateNotification()
    {
        return new UpdateNotification
        {
            Key = Key,
            OldValue = OldValue,
            NewValue = NewValue
        };
    }
}
