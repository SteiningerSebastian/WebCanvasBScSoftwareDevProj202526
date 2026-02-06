using System;
using System.Collections.Generic;
using System.Text;

namespace VeritasClient.Models
{
    public record WatchCommand : WebSocketCommand
    {
        public WatchCommand(string key)
        {
            Command = "WatchCommand";
            Parameters = new[] { new[] { "key", key } };
        }
    }
}
