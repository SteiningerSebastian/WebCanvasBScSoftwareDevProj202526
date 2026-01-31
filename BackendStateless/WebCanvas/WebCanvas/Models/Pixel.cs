namespace WebCanvas.Models;

/// <summary>
/// Represents a pixel on the canvas with its position and color.
/// </summary>
public class Pixel
{
    /// <summary>
    /// The X coordinate of the pixel.
    /// </summary>
    public uint X { get; set; }

    /// <summary>
    /// The Y coordinate of the pixel.
    /// </summary>
    public uint Y { get; set; }

    /// <summary>
    /// The color of the pixel in RGB format (24-bit stored as uint for convenience).
    /// Lower 24 bits: RGB (Red=bits 0-7, Green=bits 8-15, Blue=bits 16-23)
    /// </summary>
    public RGBColor Color { get; init; } = new();

    /// <summary>
    /// Converts the pixel to a unique key for storage.
    /// </summary>
    public uint ToKey()
    {
        // Combine X and Y into a single 32-bit key
        // Assuming 16-bit max for X and Y coordinates
        return (X << 16) | (Y & 0xFFFF);
    }

    /// <summary>
    /// Creates a Pixel from a storage key.
    /// </summary>
    public static Pixel FromKey(uint key, RGBColor color)
    {
        return new Pixel
        {
            X = key >> 16,
            Y = key & 0xFFFF,
            Color = color
        };
    }
}
