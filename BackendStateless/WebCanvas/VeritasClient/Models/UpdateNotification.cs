// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.Models;

public class UpdateNotification
{
    public string Key { get; set; } = string.Empty;
    public string OldValue { get; set; } = string.Empty;
    public string NewValue { get; set; } = string.Empty;
}
