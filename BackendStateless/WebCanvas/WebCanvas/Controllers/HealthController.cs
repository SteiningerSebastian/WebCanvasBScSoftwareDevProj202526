using Microsoft.AspNetCore.Mvc;

namespace WebCanvas.Controllers;

[ApiController]
[Route("api/[controller]")]
public class HealthController : ControllerBase
{
    private readonly ILogger<HealthController> _logger;

    public HealthController(ILogger<HealthController> logger)
    {
        _logger = logger;
    }

    [HttpGet]
    public IActionResult Get()
    {
        _logger.LogInformation("Health check endpoint called");
        return Ok(new { status = "healthy", timestamp = DateTime.UtcNow });
    }

    [HttpGet("ready")]
    public IActionResult GetReadiness()
    {
        _logger.LogInformation("Readiness check endpoint called");
        return Ok(new { ready = true, timestamp = DateTime.UtcNow });
    }

    [HttpGet("live")]
    public IActionResult GetLiveness()
    {
        _logger.LogInformation("Liveness check endpoint called");
        return Ok(new { alive = true, timestamp = DateTime.UtcNow });
    }
}