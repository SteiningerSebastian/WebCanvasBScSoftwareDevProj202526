namespace WebCanvas.Models
{
    /// <summary>
    /// Request to get a specific pixel from the canvas.
    /// </summary>
    public class GetPixelRequest
    {
        public uint X { get; set; }
        public uint Y { get; set; }
    }

}
