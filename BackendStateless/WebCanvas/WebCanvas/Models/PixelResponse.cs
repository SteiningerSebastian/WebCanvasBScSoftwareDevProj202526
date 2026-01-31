namespace WebCanvas.Models
{
    /// <summary>
    /// Response containing pixel data.
    /// </summary>
    public class PixelResponse
    {
        public uint X { get; set; }
        public uint Y { get; set; }
        public RGBColor Color { get; set; }
        public bool Success { get; set; }
        public string? ErrorMessage { get; set; }
    }
}
