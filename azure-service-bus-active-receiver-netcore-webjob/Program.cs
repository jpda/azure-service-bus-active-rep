using azure_service_bus_active_receiver_lib;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace azure_service_bus_active_receiver_netcore_webjob
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var host = new JobHost();
            host.Call(typeof(Program).GetMethod("Go"));
            host.RunAndBlock();
        }

        [NoAutomaticTrigger]
        public static void Go()
        {
            var builder = new ConfigurationBuilder().AddEnvironmentVariables();
            var config = builder.Build();

            var primarySb = config["PrimaryServiceBusConnectionString"];
            var secondarySb = config["SecondaryServiceBusConnectionString"];
            var docStorage = config["DocumentDbEndpoint"];
            var docKey = config["DocumentDbKey"];

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
        }
    }
}