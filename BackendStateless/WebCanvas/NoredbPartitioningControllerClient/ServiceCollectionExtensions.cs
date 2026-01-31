using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NoredbPartitioningControllerClient;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the PartitioningControllerClient as a singleton service to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="serviceAddress">Optional service address (defaults to Kubernetes service DNS)</param>
    /// <param name="configureLogging">Optional action to configure error logging</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddPartitioningControllerClient(
        this IServiceCollection services,
        string? serviceAddress = null,
        Action<string>? configureLogging = null)
    {
        services.AddSingleton<IPartitioningControllerClient>(sp =>
        {
            return new PartitioningControllerClient(serviceAddress, configureLogging, configureLogging);
        });

        return services;
    }

    /// <summary>
    /// Adds the PartitioningControllerClient as a singleton service with logger integration.
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="serviceAddress">Optional service address (defaults to Kubernetes service DNS)</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddPartitioningControllerClientWithLogging(
        this IServiceCollection services,
        string? serviceAddress = null)
    {
        services.AddSingleton<IPartitioningControllerClient>(sp =>
        {
            var logger = sp.GetService<ILogger<PartitioningControllerClient>>();
            
            Action<string>? logError = logger != null
                ? (msg) => logger.LogError(msg)
                : null;

            Action<string>? logInfo = logger != null
                ? (msg) => logger.LogInformation(msg)
                : null;

            return new PartitioningControllerClient(serviceAddress, logError, logInfo);
        });

        return services;
    }
}
