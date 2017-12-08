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
            var sbClients = new List<ActiveReplicationQueueClient>()
            {
                new ActiveReplicationQueueClient(primarySb, "events", true),
                new ActiveReplicationQueueClient(secondarySb, "events", false),
            };
            var s = new DataSender(sbClients);
            Task.WaitAll(s.SendOrderedMessages("1", "2", "3"));
            Console.WriteLine("all finished");
            Console.ReadLine();
        }
    }

    public class ActiveReplicationQueueClient : QueueClient
    {
        public bool IsPrimaryQueue { get; set; }
        public string Namespace { get; set; }
        public string ClientRef { get; set; }

        public ActiveReplicationQueueClient(ServiceBusConnectionStringBuilder connectionStringBuilder, bool isPrimary = true, ReceiveMode receiveMode = ReceiveMode.PeekLock, RetryPolicy retryPolicy = null) : base(connectionStringBuilder, receiveMode, retryPolicy)
        {
            IsPrimaryQueue = isPrimary;
            Namespace = connectionStringBuilder.Endpoint;
        }

        public ActiveReplicationQueueClient(string connectionString, string entityPath, bool isPrimary = true, ReceiveMode receiveMode = ReceiveMode.PeekLock, RetryPolicy retryPolicy = null) : base(connectionString, entityPath, receiveMode, retryPolicy)
        {
            IsPrimaryQueue = isPrimary;
            var sb = connectionString.Split('=')[1];
            if (Uri.TryCreate(sb, UriKind.Absolute, out Uri endpoint))
            {
                Namespace = endpoint.Host;
            }
        }
    }

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
                    m.UserProperties.Add("IsPrimary", c.IsPrimaryQueue);
                }
                await c.SendAsync(msg);
                Console.WriteLine($"Sent messages to {c.Path}");
            }
        }
    }
}
