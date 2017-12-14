using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace azure_service_bus_active_sender
{
    class Program
    {
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("appsettings.json", false)
                .AddEnvironmentVariables();
            var config = builder.Build();

            var primarySb = config["ServiceBus:Primary"];
            var secondarySb = config["ServiceBus:Secondary"];

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
