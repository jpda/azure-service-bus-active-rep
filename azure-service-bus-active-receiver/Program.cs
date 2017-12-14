using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using azure_service_bus_active_receiver_lib;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;

namespace azure_service_bus_active_receiver
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
            var docStorage = config["DocumentDb:Endpoint"];
            var docKey = config["DocumentDb:Key"];

            System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.TextWriterTraceListener(Console.Out));
            var docClient = new DocumentClient(new Uri(docStorage), docKey);
            var eventsRepo = new EventRepo<WorkItem>(docClient, "events", "journal");
            var sbClients = new List<ActiveReplicationQueueClient>()
            {
                new ActiveReplicationQueueClient(primarySb, "events", true),
                new ActiveReplicationQueueClient(secondarySb, "events", false),
            };
            var r = new DataReceiver<string>(eventsRepo, sbClients, (x) =>
            {
                var message = $"Processing message {x.SessionId}.{x.MessageId}; body: {x.Body}";
                Console.WriteLine(message);
                return Task.FromResult(message);
            });
            Console.WriteLine("Waiting...any key to exit");
            r.Start();
            Console.ReadLine();
        }
    }
}