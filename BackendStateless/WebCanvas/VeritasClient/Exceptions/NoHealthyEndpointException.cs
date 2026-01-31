// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.Exceptions;

public class NoHealthyEndpointException : Exception
{
    public NoHealthyEndpointException() : base("No healthy endpoint available")
    {
    }
}
