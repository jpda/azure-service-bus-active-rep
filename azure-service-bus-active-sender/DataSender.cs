using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace azure_service_bus_active_sender
{
    public class DataSender
    {
        private List<ActiveReplicationQueueClient> _clients;

        public DataSender(List<ActiveReplicationQueueClient> clients)
        {
            _clients = clients;
        }

        public async Task SendOrderedMessages(params string[] messages)
        {
            var sessionId = Guid.NewGuid().ToString();
            var timestamp = DateTime.UtcNow;

            Console.WriteLine($"Sending session {sessionId} at {timestamp.ToString("o")}");
            var msg = messages.Select(x => new Message(System.Text.Encoding.UTF8.GetBytes($"{DateTime.UtcNow.ToString("o")}-{x}")) { SessionId = sessionId, MessageId = Guid.NewGuid().ToString() }).ToList();

            foreach (var c in _clients)
            {
                foreach (var m in msg)
                {
                    if (m.UserProperties.ContainsKey("IsPrimary")) continue;
                    m.UserProperties.Add("IsPrimary", c.IsPrimaryQueue);
                }
                await c.SendAsync(msg);
                Console.WriteLine($"Sent messages to {c.Path}");
            }
        }
    }
}
