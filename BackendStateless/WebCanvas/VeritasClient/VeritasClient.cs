// Translated from Go implementation using Claude Sonnet

using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using VeritasClient.CircuitBreaker;
using VeritasClient.Exceptions;
using VeritasClient.Models;
using Microsoft.Extensions.Logging;

namespace VeritasClient;

/// <summary>
/// Client for interacting with the Veritas distributed key-value store.
/// </summary>
public class VeritasClient : IVeritasClient
{
    private const int RetryBeforeFail = 8;
    private const int RetryDelayMs = 32;
    private const double ExponentialBackoffBase = 1.5;

    private readonly string[] _endpoints;
    private readonly EndpointSelector _endpointSelector;
    private readonly HttpClient _httpClient;
    private readonly TimeSpan _timeout;
    private readonly ILogger<VeritasClient> _logger;

    /// <summary>
    /// Creates a new VeritasClient with the given endpoints and timeout.
    /// </summary>
    /// <param name="endpoints">List of endpoint addresses</param>
    /// <param name="timeout">Request timeout</param>
    /// <param name="failureThreshold">Number of failures before opening circuit</param>
    /// <param name="successThreshold">Number of successes before closing circuit</param>
    /// <param name="openTimeout">Time to wait before attempting half-open state</param>
    /// <param name="httpClient">Optional HTTP client (for testing)</param>
    /// <param name="logger">Logger instance</param>
    public VeritasClient(
        string[] endpoints,
        TimeSpan timeout,
        ILogger<VeritasClient>? logger = null,
        int failureThreshold = 3,
        int successThreshold = 2,
        TimeSpan? openTimeout = null,
        HttpClient? httpClient = null)
    {
        if (endpoints == null || endpoints.Length == 0)
        {
            throw new ArgumentException("At least one endpoint must be provided", nameof(endpoints));
        }

        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<VeritasClient>.Instance;

        // Shuffle endpoints to distribute load
        _endpoints = ShuffleEndpoints(endpoints);
        _timeout = timeout;
        _httpClient = httpClient ?? new HttpClient();
        _endpointSelector = new EndpointSelector(
            _endpoints.Length,
            failureThreshold,
            successThreshold,
            openTimeout ?? TimeSpan.FromSeconds(30));

        _logger.LogInformation("VeritasClient initialized with {EndpointCount} endpoints: [{Endpoints}], timeout: {Timeout}s, failureThreshold: {FailureThreshold}, successThreshold: {SuccessThreshold}",
            _endpoints.Length, string.Join(", ", _endpoints), timeout.TotalSeconds, failureThreshold, successThreshold);
    }

    private static string[] ShuffleEndpoints(string[] endpoints)
    {
        var shuffled = new string[endpoints.Length];
        Array.Copy(endpoints, shuffled, endpoints.Length);
        var random = new Random();
        
        for (int i = shuffled.Length - 1; i > 0; i--)
        {
            int j = random.Next(i + 1);
            (shuffled[i], shuffled[j]) = (shuffled[j], shuffled[i]);
        }
        
        return shuffled;
    }

    private int GetEndpoint()
    {
        return _endpointSelector.Get();
    }

    private static bool IsValidName(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return false;
        }

        // Check if the name is URL-safe
        string escapedName = Uri.EscapeDataString(name);
        return escapedName == name;
    }

    private async Task<string> ExecuteRequestAsync(
        Func<HttpRequestMessage> requestFactory,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Starting request execution with {EndpointCount} configured endpoints: [{Endpoints}]",
            _endpoints.Length, string.Join(", ", _endpoints));
        
        for (int retries = 0; retries < RetryBeforeFail; retries++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            int currentEndpoint;
            try
            {
                currentEndpoint = GetEndpoint();
                _logger.LogDebug("Selected endpoint index {EndpointIndex}: {Endpoint}",
                    currentEndpoint, _endpoints[currentEndpoint]);
            }
            catch (NoHealthyEndpointException ex)
            {
                _logger.LogWarning(ex, "No healthy endpoint available on attempt {AttemptNumber}/{MaxAttempts}",
                    retries + 1, RetryBeforeFail);
                var backoffDuration = TimeSpan.FromMilliseconds(
                    RetryDelayMs * Math.Pow(ExponentialBackoffBase, retries));
                _logger.LogDebug("Backing off for {BackoffMs}ms before retry", backoffDuration.TotalMilliseconds);
                
                try
                {
                    await Task.Delay(backoffDuration, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("Request cancelled during backoff");
                    throw;
                }
                continue;
            }

            _logger.LogDebug("Executing request to {Endpoint} (attempt {AttemptNumber}/{MaxAttempts})",
                _endpoints[currentEndpoint], retries + 1, RetryBeforeFail);

            var request = requestFactory();
            var uriBuilder = new UriBuilder(request.RequestUri!)
            {
                Scheme = "http"
            };
            
            var endpoint = _endpoints[currentEndpoint].Replace("http://", "").Replace("https://", "");
            var parts = endpoint.Split(':', 2);
            uriBuilder.Host = parts[0];
            if (parts.Length > 1 && int.TryParse(parts[1], out int port))
            {
                uriBuilder.Port = port;
            }
            else
            {
                _logger.LogWarning("Endpoint {Endpoint} does not specify a port, using default", endpoint);
            }
            
            request.RequestUri = uriBuilder.Uri;
            _logger.LogDebug("Constructed request URI: {Method} {Uri} (timeout: {TimeoutSeconds}s)",
                request.Method, request.RequestUri, _timeout.TotalSeconds);

            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_timeout);

                _logger.LogDebug("Sending HTTP request to {Uri}", request.RequestUri);
                var response = await _httpClient.SendAsync(request, cts.Token);
                _logger.LogDebug("Received response: {StatusCode} {StatusCodeName}",
                    (int)response.StatusCode, response.StatusCode);

                if (response.StatusCode >= System.Net.HttpStatusCode.InternalServerError)
                {
                    _logger.LogWarning("Server error from {Endpoint}: {StatusCode} {StatusCodeName}. Marking endpoint as failed and retrying",
                        _endpoints[currentEndpoint], (int)response.StatusCode, response.StatusCode);
                    _endpointSelector.OnFailure(currentEndpoint);

                    var backoffDuration = TimeSpan.FromMilliseconds(
                        RetryDelayMs * Math.Pow(ExponentialBackoffBase, retries));
                    
                    try
                    {
                        await Task.Delay(backoffDuration, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogDebug("Request cancelled during backoff");
                        throw;
                    }
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
                    _logger.LogWarning("Non-success status code from {Endpoint}: {StatusCode} {StatusCodeName}. Response: {Response}",
                        _endpoints[currentEndpoint], (int)response.StatusCode, response.StatusCode, errorBody);
                }

                var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
                _logger.LogDebug("Request succeeded: {Endpoint} (response length: {ResponseLength} bytes)",
                    _endpoints[currentEndpoint], responseBody.Length);
                _endpointSelector.OnSuccess(currentEndpoint);

                return responseBody;
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                if (ex is TaskCanceledException && cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug("Request cancelled by caller");
                    throw;
                }
                
                var errorType = ex is TaskCanceledException ? "Timeout" : "Connection";
                _logger.LogWarning(ex, "{ErrorType} error on {Endpoint} (attempt {AttemptNumber}/{MaxAttempts})",
                    errorType, _endpoints[currentEndpoint], retries + 1, RetryBeforeFail);
                _endpointSelector.OnFailure(currentEndpoint);

                var backoffDuration = TimeSpan.FromMilliseconds(
                    RetryDelayMs * Math.Pow(ExponentialBackoffBase, retries));
                _logger.LogDebug("Backing off for {BackoffMs}ms before retry", backoffDuration.TotalMilliseconds);
                
                try
                {
                    await Task.Delay(backoffDuration, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("Request cancelled during backoff");
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error executing request to {Endpoint} (attempt {AttemptNumber}/{MaxAttempts})",
                    _endpoints[currentEndpoint], retries + 1, RetryBeforeFail);
                throw;
            }
        }

        _logger.LogError("Maximum retries ({MaxRetries}) reached. All endpoints failed", RetryBeforeFail);
        throw new MaxRetriesReachedException();
    }

    private async Task<string> ExecuteRequestFromPathAsync(
        string method,
        string path,
        byte[]? body,
        CancellationToken cancellationToken)
    {
        var uri = _httpClient.BaseAddress != null 
            ? new Uri(_httpClient.BaseAddress, path) 
            : new Uri($"http://placeholder{path}", UriKind.Absolute);
        
        HttpRequestMessage RequestFactory()
        {
            var request = new HttpRequestMessage(new HttpMethod(method), uri);
            
            if (body != null && body.Length > 0)
            {
                request.Content = new ByteArrayContent(body);
            }
            
            return request;
        }

        return await ExecuteRequestAsync(RequestFactory, cancellationToken);
    }

    public async Task<string> GetVariableAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            _logger.LogError("Invalid variable name: {Name}", name);
            throw new InvalidNameException(name);
        }

        _logger.LogDebug("Getting variable: {Name}", name);
        return await ExecuteRequestFromPathAsync("GET", $"/get/{name}", null, cancellationToken);
    }

    public async Task<string> GetVariableEventualAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            _logger.LogError("Invalid variable name: {Name}", name);
            throw new InvalidNameException(name);
        }

        _logger.LogDebug("Getting variable (eventual consistency): {Name}", name);
        return await ExecuteRequestFromPathAsync("GET", $"/get_eventual/{name}", null, cancellationToken);
    }

    public async Task<string> PeekVariableAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            _logger.LogError("Invalid variable name: {Name}", name);
            throw new InvalidNameException(name);
        }

        _logger.LogDebug("Peeking variable: {Name}", name);
        return await ExecuteRequestFromPathAsync("GET", $"/peek/{name}", null, cancellationToken);
    }

    public async Task<bool> SetVariableAsync(string name, string value, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            _logger.LogError("Invalid variable name: {Name}", name);
            throw new InvalidNameException(name);
        }

        _logger.LogDebug("Setting variable: {Name} (value length: {ValueLength} bytes)", name, value.Length);
        var response = await ExecuteRequestFromPathAsync(
            "PUT", $"/set/{name}", Encoding.UTF8.GetBytes(value), cancellationToken);
        
        var success = string.Equals(response, "true", StringComparison.OrdinalIgnoreCase);
        _logger.LogDebug("Set variable {Name}: {Success}", name, success);
        return success;
    }

    public async Task<bool> AppendVariableAsync(string name, string value, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            throw new InvalidNameException(name);
        }

        var response = await ExecuteRequestFromPathAsync(
            "PUT", $"/append/{name}", Encoding.UTF8.GetBytes(value), cancellationToken);
        
        return string.Equals(response, "true", StringComparison.OrdinalIgnoreCase);
    }

    public async Task<bool> ReplaceVariableAsync(
        string name,
        string oldValue,
        string newValue,
        CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            throw new InvalidNameException(name);
        }

        var body = $"{oldValue.Length};{oldValue}{newValue}";
        var response = await ExecuteRequestFromPathAsync(
            "PUT", $"/replace/{name}", Encoding.UTF8.GetBytes(body), cancellationToken);
        
        return string.Equals(response, "true", StringComparison.OrdinalIgnoreCase);
    }

    public async Task<bool> CompareAndSetVariableAsync(
        string name,
        string expectedValue,
        string newValue,
        CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            _logger.LogError("Invalid variable name: {Name}", name);
            throw new InvalidNameException(name);
        }

        _logger.LogDebug("Compare-and-set variable: {Name} (expected length: {ExpectedLength}, new length: {NewLength})",
            name, expectedValue.Length, newValue?.Length ?? 0);
        var body = $"{expectedValue.Length};{expectedValue}{newValue}";
        var response = await ExecuteRequestFromPathAsync(
            "PUT", $"/compare_set/{name}", Encoding.UTF8.GetBytes(body), cancellationToken);
        
        var success = string.Equals(response, "true", StringComparison.OrdinalIgnoreCase);
        _logger.LogDebug("Compare-and-set variable {Name}: {Success}", name, success);
        return success;
    }

    public async Task<long> GetAndAddVariableAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            throw new InvalidNameException(name);
        }

        var response = await ExecuteRequestFromPathAsync("GET", $"/get_add/{name}", null, cancellationToken);
        
        if (long.TryParse(response, out long value))
        {
            return value;
        }

        throw new FormatException($"Unable to parse response as long: {response}");
    }

    private async Task<(Channel<UpdateNotification> ValueChannel, Channel<Exception> ErrorChannel)> WatchVariablesInternalAsync(
        string[] names,
        CancellationToken cancellationToken)
    {
        var valueChannel = Channel.CreateUnbounded<UpdateNotification>();
        var errorChannel = Channel.CreateUnbounded<Exception>();

        _ = Task.Run(async () =>
        {
            // Read messages
            var buffer = new byte[32768];

            try
            {
                int currentEndpoint;
                try
                {
                    currentEndpoint = GetEndpoint();
                    _logger.LogDebug("Selected endpoint {EndpointIndex} for WebSocket connection: {Endpoint}",
                        currentEndpoint, _endpoints[currentEndpoint]);
                }
                catch (NoHealthyEndpointException ex)
                {
                    _logger.LogWarning(ex, "No healthy endpoint available for WebSocket connection");
                    await errorChannel.Writer.WriteAsync(ex, cancellationToken);
                    return;
                }

                var ws = new ClientWebSocket();
                var endpointAddress = _endpoints[currentEndpoint].Replace("http://", "").Replace("https://", "");
                var uri = new Uri($"ws://{endpointAddress}/ws");
                
                _logger.LogInformation("Connecting to WebSocket at {Uri}", uri);
                await ws.ConnectAsync(uri, cancellationToken);
                _logger.LogInformation("WebSocket connected successfully to {Uri}", uri);

                // Send watch commands
                _logger.LogDebug("Sending watch commands for {VariableCount} variables: [{Variables}]",
                    names.Length, string.Join(", ", names));
                foreach (var name in names)
                {
                    var cmd = new WatchCommand(name);
                    var json = JsonSerializer.Serialize(cmd);
                    var cmdBuffer = Encoding.UTF8.GetBytes(json);
                    
                    _logger.LogDebug("Sending watch command for variable: {Name}", name);
                    _logger.LogTrace("WebSocket command JSON: {Json}", json);
                    await ws.SendAsync(
                        new ArraySegment<byte>(cmdBuffer),
                        WebSocketMessageType.Text,
                        true,
                        cancellationToken);
                }
                _logger.LogDebug("All watch commands sent successfully");

                var messageBuilder = new StringBuilder();
                
                while (!cancellationToken.IsCancellationRequested && ws.State == WebSocketState.Open)
                {
                    var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken);

                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        var chunk = Encoding.UTF8.GetString(buffer, 0, result.Count);
                        messageBuilder.Append(chunk);
                        
                        // Check if this is the end of the message
                        if (result.EndOfMessage)
                        {
                            var message = messageBuilder.ToString();
                            messageBuilder.Clear();

                            _logger.LogDebug("Received WebSocket message: {Message}", message);
                            
                            try
                            {
                                var response = JsonSerializer.Deserialize<WebSocketCommand>(message);

                                if (response != null && response.Command == nameof(UpdateNotification))
                                {
                                    UpdateNotification updateNotification = response.CastDown<UpdateNotification>();
                                    _logger.LogTrace("Received WebSocket update for key: {Key}", updateNotification.Key);
                                    _endpointSelector.OnSuccess(currentEndpoint);
                                    await valueChannel.Writer.WriteAsync(updateNotification, cancellationToken);
                                }
                                else
                                {
                                    _logger.LogWarning("Received null WebSocket response after deserialization. Message: {Message}", message);
                                }
                            }
                            catch (JsonException ex)
                            {
                                _logger.LogWarning(ex, "Failed to parse WebSocket message as JSON. Message: {Message}", message);
                                await errorChannel.Writer.WriteAsync(new Exception($"Invalid JSON received from WebSocket: {message}", ex), CancellationToken.None);
                            }
                        }
                    }
                    else if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("WebSocket close message received. Status: {Status}, Description: {Description}",
                            result.CloseStatus, result.CloseStatusDescription);
                        await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", cancellationToken);
                        break;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error in WebSocket watch loop");
                await errorChannel.Writer.WriteAsync(ex, CancellationToken.None);
            }
            finally
            {
                _logger.LogDebug("WebSocket watch loop completed, closing channels");
                valueChannel.Writer.Complete();
                errorChannel.Writer.Complete();
            }
        }, cancellationToken);

        return (valueChannel, errorChannel);
    }

    public async Task<(IAsyncEnumerable<UpdateNotification> Updates, IAsyncEnumerable<Exception> Errors)> WatchVariablesAsync(
        string[] names,
        CancellationToken cancellationToken = default)
    {
        // Validate names
        foreach (var name in names)
        {
            if (!IsValidName(name))
            {
                throw new InvalidNameException(name);
            }
        }

        var (valueChannel, errorChannel) = await WatchVariablesInternalAsync(names, cancellationToken);

        return (valueChannel.Reader.ReadAllAsync(cancellationToken), 
                errorChannel.Reader.ReadAllAsync(cancellationToken));
    }

    /// <inheritdoc/>
    public (IAsyncEnumerable<UpdateNotification> Updates, IAsyncEnumerable<Exception> Errors) WatchVariablesAutoReconnect(
        string[] names,
        CancellationToken cancellationToken = default)
    {
        var valueChannel = Channel.CreateUnbounded<UpdateNotification>();
        var errorChannel = Channel.CreateUnbounded<Exception>();

        _ = Task.Run(async () =>
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var (updates, errors) = await WatchVariablesAsync(names, cancellationToken);

                        // Initial fetch
                        _logger.LogDebug("Performing initial fetch for {VariableCount} variables", names.Length);
                        foreach (var variable in names)
                        {
                            try
                            {
                                var value = await GetVariableAsync(variable, cancellationToken);
                                _logger.LogTrace("Initial fetch succeeded for variable: {Variable}", variable);
                                await valueChannel.Writer.WriteAsync(
                                    new UpdateNotification(variable, null, value),
                                    cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Initial fetch failed for variable: {Variable}", variable);
                                await errorChannel.Writer.WriteAsync(ex, cancellationToken);

                                // If initial fetch fails, continue to reconnect
                                continue;
                            }
                        }
                        _logger.LogDebug("Initial fetch completed");

                        // Forward updates and errors
                        var updateTask = Task.Run(async () =>
                        {
                            await foreach (var update in updates.WithCancellation(cancellationToken))
                            {
                                await valueChannel.Writer.WriteAsync(update, cancellationToken);
                            }
                        }, cancellationToken);

                        var errorTask = Task.Run(async () =>
                        {
                            await foreach (var error in errors.WithCancellation(cancellationToken))
                            {
                                await errorChannel.Writer.WriteAsync(error, cancellationToken);
                            }
                        }, cancellationToken);

                        await Task.WhenAny(updateTask, errorTask);
                    }
                    catch (NoHealthyEndpointException ex)
                    {
                        _logger.LogWarning(ex, "No healthy endpoint available, retrying in {DelayMs}ms", RetryDelayMs);
                        try
                        {
                            await Task.Delay(RetryDelayMs, cancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            _logger.LogDebug("Auto-reconnect cancelled during retry delay");
                            throw;
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogError(ex, "Error in auto-reconnect watch loop");
                        await errorChannel.Writer.WriteAsync(ex, CancellationToken.None);
                    }

                    // Wait before reconnecting
                    _logger.LogDebug("Waiting 1 second before reconnecting");
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogDebug("Auto-reconnect cancelled during reconnect delay");
                        throw;
                    }
                }
            }
            finally
            {
                _logger.LogDebug("Auto-reconnect watch loop completed, closing channels");

                valueChannel.Writer.Complete();
                errorChannel.Writer.Complete();
            }
        }, cancellationToken);

        return (valueChannel.Reader.ReadAllAsync(cancellationToken),
                errorChannel.Reader.ReadAllAsync(cancellationToken));
    }
}
