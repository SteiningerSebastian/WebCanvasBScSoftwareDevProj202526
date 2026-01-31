namespace WebCanvas.Models
{
    public struct RGBColor
    {
        public byte R { get; init; }
        public byte G { get; init; }
        public byte B { get; init; }
        public RGBColor(byte r, byte g, byte b)
        {
            R = r;
            G = g;
            B = b;
        }

        public void ToBytes(Span<byte> buffer)
        {
            if (buffer.Length < 3)
            {
                throw new ArgumentException("Buffer must be at least 3 bytes long", nameof(buffer));
            }
            buffer[0] = R;
            buffer[1] = G;
            buffer[2] = B;
        }

        public byte[] ToBytes()
        {
            return new byte[] { R, G, B };
        }

        public static RGBColor FromBytes(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < 3)
            {
                throw new ArgumentException("Buffer must be at least 3 bytes long", nameof(buffer));
            }
            return new RGBColor(buffer[0], buffer[1], buffer[2]);
        }
    }
}
