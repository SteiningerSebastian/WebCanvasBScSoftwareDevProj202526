# VeritasClient

A C# client library for interacting with the Veritas distributed key-value store.

## Features

- **Linearizable Operations**: Get, Set, Append, Replace, Compare-and-Set operations with strong consistency guarantees
- **Eventual Consistency**: Support for eventually consistent reads when performance is more important than consistency
- **Fast Local Reads**: Peek operations for reading from the local node without consistency guarantees
- **Circuit Breaker Pattern**: Automatic endpoint health tracking and failover
- **Retry Logic**: Built-in exponential backoff retry mechanism
- **WebSocket Support**: Watch for real-time variable updates with optional auto-reconnection
- **Async/Await**: Full async support using modern C# patterns

## Installation

Add the VeritasClient project to your solution and reference it in your project.

## Usage

### Creating a Client

```csharp
using VeritasClient;

// Define your Veritas endpoints
var endpoints = new[] 
{
    "http://localhost:8080",
    "http://localhost:8081",
    "http://localhost:8082"
};

// Create the client with a 5-second timeout
var client = new VeritasClient(
    endpoints: endpoints,
    timeout: TimeSpan.FromSeconds(5),
    failureThreshold: 3,      // Open circuit after 3 failures
    successThreshold: 2,       // Close circuit after 2 successes
    openTimeout: TimeSpan.FromSeconds(30)  // Retry after 30 seconds
);
```

### Basic Operations

#### Get Variable (Linearizable)
```csharp
string value = await client.GetVariableAsync("myVariable");
Console.WriteLine($"Value: {value}");
```

#### Get Variable (Eventual Consistency)
```csharp
string value = await client.GetVariableEventualAsync("myVariable");
```

#### Peek Variable (Local, No Consistency Guarantee)
```csharp
string value = await client.PeekVariableAsync("myVariable");
```

#### Set Variable
```csharp
bool success = await client.SetVariableAsync("myVariable", "newValue");
if (success)
{
    Console.WriteLine("Variable set successfully");
}
```

#### Append to Variable
```csharp
bool success = await client.AppendVariableAsync("myList", "newItem");
```

#### Replace Variable
```csharp
bool success = await client.ReplaceVariableAsync(
    name: "myVariable",
    oldValue: "currentValue",
    newValue: "newValue"
);
```

#### Compare and Set
```csharp
bool success = await client.CompareAndSetVariableAsync(
    name: "counter",
    expectedValue: "10",
    newValue: "11"
);
```

#### Get and Add (Atomic Counter)
```csharp
long previousValue = await client.GetAndAddVariableAsync("counter");
Console.WriteLine($"Got unique value: {previousValue}");
```

### Watching Variables

#### Watch with Manual Reconnection
```csharp
var cancellationToken = new CancellationTokenSource().Token;

var (updates, errors) = await client.WatchVariablesAsync(
    new[] { "variable1", "variable2" },
    cancellationToken
);

// Process updates
await foreach (var update in updates.WithCancellation(cancellationToken))
{
    Console.WriteLine($"Variable {update.Key} changed from '{update.OldValue}' to '{update.NewValue}'");
}

// Handle errors
await foreach (var error in errors.WithCancellation(cancellationToken))
{
    Console.WriteLine($"Error: {error.Message}");
}
```

#### Watch with Auto-Reconnection
```csharp
var cancellationToken = new CancellationTokenSource().Token;

var (updates, errors) = client.WatchVariablesAutoReconnect(
    new[] { "variable1", "variable2" },
    cancellationToken
);

// This will automatically reconnect if the connection is lost
await foreach (var update in updates.WithCancellation(cancellationToken))
{
    Console.WriteLine($"Variable {update.Key} updated to '{update.NewValue}'");
}
```

### Error Handling

```csharp
using VeritasClient.Exceptions;

try
{
    var value = await client.GetVariableAsync("myVariable");
}
catch (InvalidNameException ex)
{
    Console.WriteLine($"Invalid variable name: {ex.VariableName}");
}
catch (NoHealthyEndpointException)
{
    Console.WriteLine("No healthy endpoints available");
}
catch (MaxRetriesReachedException)
{
    Console.WriteLine("Maximum retries reached");
}
```

## Service Discovery

The client supports multiple service discovery mechanisms:

### Static Discovery

```csharp
using VeritasClient.ServiceDiscovery;

var discovery = new StaticServiceDiscovery(new[] 
{
    "http://localhost:8080",
    "http://localhost:8081"
});

var client = new VeritasClient(
    discovery,
    timeout: TimeSpan.FromSeconds(5)
);
```

### DNS-Based Discovery

```csharp
var discovery = new DnsServiceDiscovery(
    dnsName: "veritas-service.example.com",
    port: 8080,
    scheme: "http"
);

var client = new VeritasClient(
    discovery,
    timeout: TimeSpan.FromSeconds(5),
    discoveryRefreshInterval: TimeSpan.FromMinutes(5) // Refresh endpoints every 5 minutes
);
```

### Kubernetes Service Discovery

```csharp
var discovery = new KubernetesServiceDiscovery(
    serviceName: "veritas-service",
    @namespace: "default",
    port: 8080
);

var client = new VeritasClient(
    discovery,
    timeout: TimeSpan.FromSeconds(5),
    discoveryRefreshInterval: TimeSpan.FromMinutes(1)
);
```

### HTTP-Based Discovery

```csharp
var discovery = new HttpServiceDiscovery(
    discoveryUrl: "http://config-server/api/veritas-endpoints",
    timeout: TimeSpan.FromSeconds(10)
);

var client = new VeritasClient(
    discovery,
    timeout: TimeSpan.FromSeconds(5),
    discoveryRefreshInterval: TimeSpan.FromMinutes(5)
);
```

### Composite Discovery (Fallback Strategy)

```csharp
var discovery = new CompositeServiceDiscovery(
    new IServiceDiscovery[]
    {
        new KubernetesServiceDiscovery("veritas-service", "default"),
        new DnsServiceDiscovery("veritas.example.com", 8080),
        new StaticServiceDiscovery(new[] { "http://fallback:8080" })
    }
);

var client = new VeritasClient(
    discovery,
    timeout: TimeSpan.FromSeconds(5),
    discoveryRefreshInterval: TimeSpan.FromMinutes(1)
);
```

## Service Registration

Use Veritas as a service registry for dynamic service registration and discovery:

### Registering a Service

```csharp
using VeritasClient.ServiceRegistration;

var client = new VeritasClient(endpoints, TimeSpan.FromSeconds(5));

var registrationHandler = new ServiceRegistrationHandler(
    client,
    serviceName: "my-service",
    logError: msg => Console.WriteLine($"[ERROR] {msg}")
);

// Register a new service
var serviceReg = new ServiceRegistration
{
    Id = "my-service-v1",
    Endpoints = new List<ServiceEndpoint>
    {
        new ServiceEndpoint
        {
            Id = Guid.NewGuid().ToString(),
            Address = "192.168.1.10",
            Port = 8080,
            Timestamp = DateTime.UtcNow
        }
    },
    Meta = new Dictionary<string, string>
    {
        { "version", "1.0.0" },
        { "region", "us-east" }
    }
};

await registrationHandler.RegisterOrUpdateServiceAsync(serviceReg);
```

### Registering Individual Endpoints

```csharp
var endpoint = new ServiceEndpoint
{
    Id = Guid.NewGuid().ToString(),
    Address = "192.168.1.11",
    Port = 8080,
    Timestamp = DateTime.UtcNow
};

await registrationHandler.RegisterOrUpdateEndpointAsync(endpoint);
```

### Watching Service Changes

```csharp
// Listen for service registration updates
registrationHandler.AddListener(registration =>
{
    Console.WriteLine($"Service updated: {registration.Id}");
    Console.WriteLine($"Endpoints: {registration.Endpoints.Count}");
});

// Listen for endpoint changes
registrationHandler.AddEndpointListener(update =>
{
    Console.WriteLine($"Added endpoints: {update.AddedEndpoints.Count}");
    Console.WriteLine($"Removed endpoints: {update.RemovedEndpoints.Count}");
    
    foreach (var ep in update.AddedEndpoints)
    {
        Console.WriteLine($"  + {ep}");
    }
    
    foreach (var ep in update.RemovedEndpoints)
    {
        Console.WriteLine($"  - {ep}");
    }
});
```

### Resolving Service Registration

```csharp
var registration = await registrationHandler.ResolveServiceAsync();

Console.WriteLine($"Service: {registration.Id}");
foreach (var endpoint in registration.Endpoints)
{
    Console.WriteLine($"  Endpoint: {endpoint}");
}
```

### Cleaning Up Old Endpoints

```csharp
// Remove endpoints that haven't been updated in 5 minutes
await registrationHandler.TryCleanupOldEndpointsAsync(TimeSpan.FromMinutes(5));
```

### Heartbeat Pattern

```csharp
// Periodic endpoint registration to maintain presence
using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));

var myEndpoint = new ServiceEndpoint
{
    Id = "my-instance-id",
    Address = "192.168.1.10",
    Port = 8080,
    Timestamp = DateTime.UtcNow
};

while (await timer.WaitForNextTickAsync())
{
    try
    {
        myEndpoint.Timestamp = DateTime.UtcNow;
        await registrationHandler.RegisterOrUpdateEndpointAsync(myEndpoint);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Failed to update endpoint: {ex.Message}");
    }
}
```

### Service Registration Exceptions

```csharp
using VeritasClient.ServiceRegistration;

try
{
    await registrationHandler.RegisterOrUpdateServiceAsync(serviceReg);
}
catch (ServiceNotFoundException)
{
    Console.WriteLine("Service not found");
}
catch (ServiceRegistrationChangedException)
{
    Console.WriteLine("Service was modified by another process, retry");
}
catch (ServiceRegistrationFailedException ex)
{
    Console.WriteLine($"Registration failed: {ex.Message}");
}
```

### Disposing Resources

```csharp
// Don't forget to dispose when done
registrationHandler.Dispose();
client.Dispose();
```

### Logging

You can provide custom logging callbacks:

```csharp
var client = new VeritasClient(
    endpoints: endpoints,
    timeout: TimeSpan.FromSeconds(5),
    logDebug: msg => Console.WriteLine($"[DEBUG] {msg}"),
    logWarning: msg => Console.WriteLine($"[WARNING] {msg}")
);
```

## Architecture

### Circuit Breaker

The client uses a circuit breaker pattern to track endpoint health:
- **Closed**: Normal operation, requests are sent to the endpoint
- **Open**: After reaching failure threshold, endpoint is marked unhealthy
- **Half-Open**: After timeout, one request is tried to test if endpoint recovered

### Retry Logic

- Automatic retry with exponential backoff
- Default: 8 retries with 32ms base delay and 1.5x exponential backoff
- Falls back to different endpoints on failure

### Variable Name Validation

Variable names must be URL-safe (no special characters that need escaping).

## Models

### UpdateNotification
```csharp
public class UpdateNotification
{
    public string Key { get; set; }
    public string OldValue { get; set; }
    public string NewValue { get; set; }
}
```

## Exception Types

- **InvalidNameException**: Variable name contains invalid characters
- **NoHealthyEndpointException**: All endpoints are marked unhealthy
- **MaxRetriesReachedException**: Maximum retry attempts exceeded
- **MessageTypeNotSupportedException**: Unsupported WebSocket message type received

## Thread Safety

The VeritasClient is thread-safe and can be used from multiple threads concurrently. Consider using a singleton instance for your application.

## License

This client is part of the WebCanvas project.
