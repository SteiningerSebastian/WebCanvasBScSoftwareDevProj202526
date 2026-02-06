// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.Models;

public record UpdateNotification : WebSocketCommand
{
    public string Key
    {
        get
        {
            return GetParameter("key");
        }
        set
        {
            SetParameter("key", value);
        }
    }
    public string? OldValue { get => GetParameter("old_value"); set => SetParameter("oldValue", value ?? string.Empty); }
    public string NewValue { get => GetParameter("new_value"); set => SetParameter("newValue", value); }

    public UpdateNotification(string key, string? oldValue, string newValue)
    {
        Command = "UpdateNotification";
        Parameters = new[] { new[] { "key", key }, new[] { "oldValue", oldValue ?? string.Empty }, new[] { "newValue", newValue } };
    }

    /// <summary>
    /// Initializes a new instance of the UpdateNotification class with default command and parameters.
    /// </summary>
    public UpdateNotification()
    {
        Command = "UpdateNotification";
        Parameters = Array.Empty<string[]>();
    }
}
