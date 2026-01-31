// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.Exceptions;

public class InvalidNameException : Exception
{
    public string VariableName { get; }

    public InvalidNameException(string name) 
        : base($"Variable name '{name}' is not allowed.")
    {
        VariableName = name;
    }
}
