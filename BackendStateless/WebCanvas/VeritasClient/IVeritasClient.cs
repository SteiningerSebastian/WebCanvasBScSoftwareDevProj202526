// Translated from Go implementation using Claude Sonnet

using VeritasClient.Models;

namespace VeritasClient;

/// <summary>
/// Interface for interacting with the Veritas distributed key-value store.
/// </summary>
public interface IVeritasClient
{
    /// <summary>
    /// Get the value of a variable by name linearizably.
    /// </summary>
    Task<string> GetVariableAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get the value of a variable by name eventually (eventual consistency).
    /// </summary>
    Task<string> GetVariableEventualAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Peek at the value stored on the current node only (no consistency guarantees but way faster).
    /// </summary>
    Task<string> PeekVariableAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Set the value of a variable by name linearizably.
    /// </summary>
    Task<bool> SetVariableAsync(string name, string value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Append a value to a variable by name linearizably.
    /// </summary>
    Task<bool> AppendVariableAsync(string name, string value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replace oldValue with newValue for a variable by name linearizably.
    /// </summary>
    Task<bool> ReplaceVariableAsync(string name, string oldValue, string newValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Compare and set a variable by name linearizably.
    /// </summary>
    Task<bool> CompareAndSetVariableAsync(string name, string expectedValue, string newValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically add delta to the integer variable by name linearizably and return the old value.
    /// Each call will get a unique value.
    /// </summary>
    Task<long> GetAndAddVariableAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Watch for changes to variables by name. The callback is called linearizably whenever a variable changes.
    /// If a disconnection occurs, it can't automatically reconnect as updates between disconnection and reconnection are lost
    /// and the linearizability guarantee would be violated.
    /// </summary>
    Task<(IAsyncEnumerable<UpdateNotification> Updates, IAsyncEnumerable<Exception> Errors)> WatchVariablesAsync(
        string[] names, CancellationToken cancellationToken = default);

    /// <summary>
    /// Watch for changes to variables by name with automatic reconnection.
    /// NOTE: Updates that occur between disconnection and reconnection are lost. Therefore, the linearizability guarantee
    /// is only preserved for periods when the connection is active.
    /// </summary>
    (IAsyncEnumerable<UpdateNotification> Updates, IAsyncEnumerable<Exception> Errors) WatchVariablesAutoReconnect(
        string[] names, CancellationToken cancellationToken = default);
}
