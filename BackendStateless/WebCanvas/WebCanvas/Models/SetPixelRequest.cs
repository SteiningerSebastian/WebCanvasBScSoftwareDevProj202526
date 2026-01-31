namespace WebCanvas.Models
{
    /// <summary>
    /// Request to set a pixel on the canvas.
    /// </summary>
    public class SetPixelRequest
    {
        public uint X { get; set; }
        public uint Y { get; set; }
        public RGBColor Color { get; set; }
    }
}
