using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents.Client;

namespace azure_service_bus_active_receiver
{
    class Program
    {
        public const string primarySb = "Endpoint=sb://active-east.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=r63QRPX6htDT/IJPUBH7AeWrO1zu9bY/q7OJSM2CGt0=";
        public const string secondarySb = "Endpoint=sb://active-ncentral.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=kTD23N+iwv/6UCeVQ5yytabob8aX5pYSs5YXjndf6Yw=";
        public const string docStorage = "https://sbdocjournal.documents.azure.com:443/";
        public const string docKey = "kxOj6FKuf7Ul78mdaa1o6myaT26nb9LLc52OXlWS3SYkBcRPX5w1dlHf0phO9AMUZewBWWAnYbamv7LFfhtIfA==";

        static void Main(string[] args)
        {
            var docClient = new DocumentClient(new Uri(docStorage), docKey);
            var eventsRepo = new EventRepo<WorkItem>(docClient, "events", "journal");
            var sbClients = new List<ActiveReplicationQueueClient>()
            {
                new ActiveReplicationQueueClient(primarySb, "events", true),
                new ActiveReplicationQueueClient(secondarySb, "events", false),
            };
            var r = new DataReceiver(eventsRepo, sbClients);
            Console.WriteLine("Waiting...any key to exit");
            r.ReceiveMessage();
            Console.ReadLine();
        }
    }
}