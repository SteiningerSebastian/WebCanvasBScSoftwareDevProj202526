// Translated from Go implementation using Claude Sonnet

using System.Text.Json.Serialization;

namespace VeritasClient.Models;

public class WebSocketCommand
{
    [JsonPropertyName("command")]
    public string Command { get; set; } = string.Empty;

    [JsonPropertyName("parameters")]
    public string[][] Parameters { get; set; } = Array.Empty<string[]>();

    public static WebSocketCommand FromWatchCommand(string key)
    {
        return new WebSocketCommand
        {
            Command = "WatchCommand",
            Parameters = new[] { new[] { "key", key } }
        };
    }
}
