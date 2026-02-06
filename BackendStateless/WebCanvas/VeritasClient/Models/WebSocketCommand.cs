// Translated from Go implementation using Claude Sonnet

using System.Text.Json.Serialization;

namespace VeritasClient.Models;

public record WebSocketCommand
{
    [JsonPropertyName("command")]
    public string Command { get; set; } = "noop"; // Default to "noop" for safety

    [JsonPropertyName("parameters")]
    public string[][] Parameters { get; set; } = Array.Empty<string[]>();

    protected string GetParameter(string name)
    {
        foreach (var param in Parameters)
        {
            if (param.Length == 2 && param[0] == name)
            {
                return param[1];
            }
        }
        return string.Empty; // Return empty string if parameter not found
    }

    protected void SetParameter(string name, string value)
    {
        for (int i = 0; i < Parameters.Length; i++)
        {
            if (Parameters[i].Length == 2 && Parameters[i][0] == name)
            {
                Parameters[i][1] = value;
                return;
            }
        }
        // If parameter not found, add it
        var newParam = new[] { name, value };
        var newParameters = new string[Parameters.Length + 1][];
        Array.Copy(Parameters, newParameters, Parameters.Length);
        newParameters[Parameters.Length] = newParam;
        Parameters = newParameters;
    }

    /// <summary>
    /// This method allows you to cast the current WebSocketCommand instance to a more specific type that inherits from WebSocketCommand.
    /// </summary>
    /// <typeparam name="T">The type to cast the command to.</typeparam>
    /// <returns>A new instance of that object is returned.</returns>
    public T CastDown<T>() where T : WebSocketCommand, new()
    {
        var result = new T
        {
            Command = this.Command,
            Parameters = this.Parameters
        };
        return result;
    }
}
