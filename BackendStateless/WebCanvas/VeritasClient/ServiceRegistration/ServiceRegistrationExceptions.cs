// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.ServiceRegistration;

public class ServiceRegistrationException : Exception
{
    public ServiceRegistrationException(string message) : base(message) { }
    public ServiceRegistrationException(string message, Exception innerException) : base(message, innerException) { }
}

public class ServiceNotFoundException : ServiceRegistrationException
{
    public ServiceNotFoundException() : base("Service not found") { }
}

public class ServiceRegistrationFailedException : ServiceRegistrationException
{
    public ServiceRegistrationFailedException() : base("Service registration failed") { }
    public ServiceRegistrationFailedException(Exception innerException) 
        : base("Service registration failed", innerException) { }
}

public class ServiceRegistrationChangedException : ServiceRegistrationException
{
    public ServiceRegistrationChangedException() : base("Service registration changed") { }
}

public class EndpointRegistrationFailedException : ServiceRegistrationException
{
    public EndpointRegistrationFailedException() : base("Endpoint registration failed") { }
    public EndpointRegistrationFailedException(Exception innerException) 
        : base("Endpoint registration failed", innerException) { }
}
