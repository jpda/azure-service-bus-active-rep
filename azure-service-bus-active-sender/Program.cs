using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace azure_service_bus_active_sender
{
    class Program
    {
        private const string primarySb = "Endpoint=sb://active-east.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=kypBngREny1YEVc/lY7zlzPGfZKStgwrmVpGqtxA8mU=";
        private const string secondarySb = "Endpoint=sb://active-ncentral.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=gBKpMFiXu+9OFfDS3WpKVUDLOucjconK9EOrICCHZC0=";

        static void Main(string[] args)
        {
            var s = new DataSender(primarySb, secondarySb);
            Task.WaitAll(s.SendOrderedMessages("1", "2", "3"));
            Console.WriteLine("all finished");
            Console.ReadLine();
        }
    }

    public class DataSender
    {
        private List<QueueClient> _clients;

        public DataSender(List<QueueClient> clients)
        {
            _clients = clients;
        }

        public DataSender(params string[] queueEndpoints) : this(queueEndpoints.Select(x => new QueueClient(x, "events")).ToList()) { }

        public async Task SendOrderedMessages(params string[] messages)
        {
            var sessionId = Guid.NewGuid().ToString();
            var timestamp = DateTime.UtcNow;

            Console.WriteLine($"Sending session {sessionId} at {timestamp.ToString("o")}");
            var msg = messages.Select(x => new Message(System.Text.Encoding.UTF8.GetBytes($"{DateTime.UtcNow.ToString("o")}-{x}")) { SessionId = sessionId }).ToList();

            foreach (var c in _clients)
            {
                await c.SendAsync(msg);
                Console.WriteLine($"Sent messages to {c.Path}");
            }
        }
    }
}
