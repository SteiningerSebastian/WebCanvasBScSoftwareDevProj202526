// Translated from Go implementation using Claude Sonnet

namespace VeritasClient.Exceptions;

public class MessageTypeNotSupportedException : Exception
{
    public int MessageType { get; }
    public byte[] MessageBytes { get; }

    public MessageTypeNotSupportedException(int messageType, byte[] message)
        : base($"Message-Type {messageType} is not supported. Message: {System.Text.Encoding.UTF8.GetString(message)}")
    {
        MessageType = messageType;
        MessageBytes = message;
    }
}
