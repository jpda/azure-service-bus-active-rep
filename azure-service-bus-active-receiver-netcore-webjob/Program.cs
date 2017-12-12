using azure_service_bus_active_receiver_lib;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace azure_service_bus_active_receiver_netcore_webjob
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = new JobHost();
            var hostConfig = new JobHostConfiguration();

            host.Call(typeof(Program).GetMethod("Go"));
            host.RunAndBlock();
        }

        public const string primarySb = "Endpoint=sb://active-east.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=r63QRPX6htDT/IJPUBH7AeWrO1zu9bY/q7OJSM2CGt0=";
        public const string secondarySb = "Endpoint=sb://active-ncentral.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=kTD23N+iwv/6UCeVQ5yytabob8aX5pYSs5YXjndf6Yw=";
        public const string docStorage = "https://sbdocjournal.documents.azure.com:443/";
        public const string docKey = "kxOj6FKuf7Ul78mdaa1o6myaT26nb9LLc52OXlWS3SYkBcRPX5w1dlHf0phO9AMUZewBWWAnYbamv7LFfhtIfA==";

        [NoAutomaticTrigger]
        public void Go()
        {
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