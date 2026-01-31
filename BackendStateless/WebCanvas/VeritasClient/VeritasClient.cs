// Translated from Go implementation using Claude Sonnet

using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using VeritasClient.CircuitBreaker;
using VeritasClient.Exceptions;
using VeritasClient.Models;

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
    private readonly Action<string>? _logDebug;
    private readonly Action<string>? _logWarning;

    /// <summary>
    /// Creates a new VeritasClient with the given endpoints and timeout.
    /// </summary>
    /// <param name="endpoints">List of endpoint addresses</param>
    /// <param name="timeout">Request timeout</param>
    /// <param name="failureThreshold">Number of failures before opening circuit</param>
    /// <param name="successThreshold">Number of successes before closing circuit</param>
    /// <param name="openTimeout">Time to wait before attempting half-open state</param>
    /// <param name="httpClient">Optional HTTP client (for testing)</param>
    /// <param name="logDebug">Optional debug logger</param>
    /// <param name="logWarning">Optional warning logger</param>
    public VeritasClient(
        string[] endpoints,
        TimeSpan timeout,
        int failureThreshold = 3,
        int successThreshold = 2,
        TimeSpan? openTimeout = null,
        HttpClient? httpClient = null,
        Action<string>? logDebug = null,
        Action<string>? logWarning = null)
    {
        if (endpoints == null || endpoints.Length == 0)
        {
            throw new ArgumentException("At least one endpoint must be provided", nameof(endpoints));
        }

        // Shuffle endpoints to distribute load
        _endpoints = ShuffleEndpoints(endpoints);
        _timeout = timeout;
        _httpClient = httpClient ?? new HttpClient();
        _logDebug = logDebug;
        _logWarning = logWarning;
        _endpointSelector = new EndpointSelector(
            _endpoints.Length,
            failureThreshold,
            successThreshold,
            openTimeout ?? TimeSpan.FromSeconds(30));
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
        _logDebug?.Invoke($"Starting request execution with {_endpoints.Length} configured endpoints: [{string.Join(", ", _endpoints)}]");
        
        for (int retries = 0; retries < RetryBeforeFail; retries++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            int currentEndpoint;
            try
            {
                currentEndpoint = GetEndpoint();
                _logDebug?.Invoke($"Selected endpoint index {currentEndpoint}: {_endpoints[currentEndpoint]}");
            }
            catch (NoHealthyEndpointException ex)
            {
                _logWarning?.Invoke($"No healthy endpoint available on attempt {retries + 1}/{RetryBeforeFail}: {ex.Message}");
                var backoffDuration = TimeSpan.FromMilliseconds(
                    RetryDelayMs * Math.Pow(ExponentialBackoffBase, retries));
                _logDebug?.Invoke($"Backing off for {backoffDuration.TotalMilliseconds}ms before retry");
                await Task.Delay(backoffDuration, cancellationToken);
                continue;
            }

            _logDebug?.Invoke($"Executing request to {_endpoints[currentEndpoint]} (attempt {retries + 1}/{RetryBeforeFail})");

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
            
            request.RequestUri = uriBuilder.Uri;
            _logDebug?.Invoke($"Constructed request URI: {request.Method} {request.RequestUri} (timeout: {_timeout.TotalSeconds}s)");

            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_timeout);

                _logDebug?.Invoke($"Sending HTTP request to {request.RequestUri}...");
                var response = await _httpClient.SendAsync(request, cts.Token);
                _logDebug?.Invoke($"Received response: {(int)response.StatusCode} {response.StatusCode}");

                if (response.StatusCode >= System.Net.HttpStatusCode.InternalServerError)
                {
                    _logWarning?.Invoke($"Server error from {_endpoints[currentEndpoint]}: {(int)response.StatusCode} {response.StatusCode}. Marking endpoint as failed and retrying...");
                    _endpointSelector.OnFailure(currentEndpoint);

                    var backoffDuration = TimeSpan.FromMilliseconds(
                        RetryDelayMs * Math.Pow(ExponentialBackoffBase, retries));
                    await Task.Delay(backoffDuration, cancellationToken);
                    continue;
                }

                var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
                _logDebug?.Invoke($"Request succeeded: {_endpoints[currentEndpoint]} (response length: {responseBody.Length} bytes)");
                _endpointSelector.OnSuccess(currentEndpoint);

                return responseBody;
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                if (ex is TaskCanceledException && cancellationToken.IsCancellationRequested)
                {
                    _logDebug?.Invoke("Request cancelled by caller");
                    throw;
                }
                
                var errorType = ex is TaskCanceledException ? "Timeout" : "Connection";
                _logWarning?.Invoke($"{errorType} error on {_endpoints[currentEndpoint]} (attempt {retries + 1}/{RetryBeforeFail}): {ex.GetType().Name}: {ex.Message}");
                _endpointSelector.OnFailure(currentEndpoint);

                var backoffDuration = TimeSpan.FromMilliseconds(
                    RetryDelayMs * Math.Pow(ExponentialBackoffBase, retries));
                _logDebug?.Invoke($"Backing off for {backoffDuration.TotalMilliseconds}ms before retry");
                await Task.Delay(backoffDuration, cancellationToken);
            }
        }

        _logWarning?.Invoke($"Maximum retries ({RetryBeforeFail}) reached. All endpoints failed.");
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
            throw new InvalidNameException(name);
        }

        return await ExecuteRequestFromPathAsync("GET", $"/get/{name}", null, cancellationToken);
    }

    public async Task<string> GetVariableEventualAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            throw new InvalidNameException(name);
        }

        return await ExecuteRequestFromPathAsync("GET", $"/get_eventual/{name}", null, cancellationToken);
    }

    public async Task<string> PeekVariableAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            throw new InvalidNameException(name);
        }

        return await ExecuteRequestFromPathAsync("GET", $"/peek/{name}", null, cancellationToken);
    }

    public async Task<bool> SetVariableAsync(string name, string value, CancellationToken cancellationToken = default)
    {
        if (!IsValidName(name))
        {
            throw new InvalidNameException(name);
        }

        var response = await ExecuteRequestFromPathAsync(
            "PUT", $"/set/{name}", Encoding.UTF8.GetBytes(value), cancellationToken);
        
        return string.Equals(response, "true", StringComparison.OrdinalIgnoreCase);
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
            throw new InvalidNameException(name);
        }

        var body = $"{expectedValue.Length};{expectedValue}{newValue}";
        var response = await ExecuteRequestFromPathAsync(
            "PUT", $"/compare_set/{name}", Encoding.UTF8.GetBytes(body), cancellationToken);
        
        return string.Equals(response, "true", StringComparison.OrdinalIgnoreCase);
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
            try
            {
                int currentEndpoint;
                try
                {
                    currentEndpoint = GetEndpoint();
                }
                catch (NoHealthyEndpointException ex)
                {
                    _logWarning?.Invoke("No healthy endpoint available.");
                    await errorChannel.Writer.WriteAsync(ex, cancellationToken);
                    return;
                }

                var ws = new ClientWebSocket();
                var uri = new Uri($"ws://{_endpoints[currentEndpoint].Replace("http://", "").Replace("https://", "")}/ws");
                
                await ws.ConnectAsync(uri, cancellationToken);

                // Send watch commands
                foreach (var name in names)
                {
                    var cmd = WebSocketCommand.FromWatchCommand(name);
                    var json = JsonSerializer.Serialize(cmd);
                    var cmdBuffer = Encoding.UTF8.GetBytes(json);
                    
                    await ws.SendAsync(
                        new ArraySegment<byte>(cmdBuffer),
                        WebSocketMessageType.Text,
                        true,
                        cancellationToken);
                }

                // Read messages
                var buffer = new byte[4096];
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
                            
                            try
                            {
                                var response = JsonSerializer.Deserialize<WebSocketResponse>(message);
                                
                                if (response != null)
                                {
                                    _endpointSelector.OnSuccess(currentEndpoint);
                                    await valueChannel.Writer.WriteAsync(response.ToUpdateNotification(), cancellationToken);
                                }
                            }
                            catch (JsonException ex)
                            {
                                _logWarning?.Invoke($"Failed to parse WebSocket message as JSON: {message}. Error: {ex.Message}");
                                await errorChannel.Writer.WriteAsync(new Exception($"Invalid JSON received from WebSocket: {message}"), CancellationToken.None);
                            }
                        }
                    }
                    else if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", cancellationToken);
                        break;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await errorChannel.Writer.WriteAsync(ex, CancellationToken.None);
            }
            finally
            {
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
                        foreach (var variable in names)
                        {
                            try
                            {
                                var value = await GetVariableAsync(variable, cancellationToken);
                                await valueChannel.Writer.WriteAsync(
                                    new UpdateNotification
                                    {
                                        Key = variable,
                                        NewValue = value,
                                        OldValue = value
                                    },
                                    cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                await errorChannel.Writer.WriteAsync(ex, cancellationToken);
                            }
                        }

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
                    catch (NoHealthyEndpointException)
                    {
                        _logWarning?.Invoke("No healthy endpoint available, retrying...");
                        await Task.Delay(RetryDelayMs, cancellationToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        await errorChannel.Writer.WriteAsync(ex, CancellationToken.None);
                    }

                    // Wait before reconnecting
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
            }
            finally
            {
                valueChannel.Writer.Complete();
                errorChannel.Writer.Complete();
            }
        }, cancellationToken);

        return (valueChannel.Reader.ReadAllAsync(cancellationToken),
                errorChannel.Reader.ReadAllAsync(cancellationToken));
    }
}
