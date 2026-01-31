// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.Exceptions;

public class MaxRetriesReachedException : Exception
{
    public MaxRetriesReachedException() : base("Maximum retries reached")
    {
    }
}
