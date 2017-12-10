using System;
using System.Collections.Generic;
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
            var more = false;
            do
            {
                Console.WriteLine("How many?");
                var response = Console.ReadLine();
                if (int.TryParse(response, out var iterations))
                {
                    var messages = new List<string>();
                    var sessionId = Guid.NewGuid().ToString();
                    for (var i = 0; i < iterations; i++)
                    {
                        messages.Add($"{i}");
                        Task.WaitAll(s.SendOrderedMessages(sessionId, messages.ToArray()));
                    }
                    more = true;
                }
                else
                {
                    more = false;
                }
            } while (more);
        }
    }
}
