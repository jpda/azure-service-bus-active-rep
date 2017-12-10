using System;
using System.Collections.Generic;
using Microsoft.Azure.Storage;

namespace azure_service_bus_active_receiver
{
    class Program
    {
        public const string primarySb = "Endpoint=sb://active-east.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=r63QRPX6htDT/IJPUBH7AeWrO1zu9bY/q7OJSM2CGt0=";
        public const string secondarySb = "Endpoint=sb://active-ncentral.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=kTD23N+iwv/6UCeVQ5yytabob8aX5pYSs5YXjndf6Yw=";
        public const string storage = "DefaultEndpointsProtocol=https;AccountName=sbjournal;AccountKey=exbkLArPQAXx9IEgh9cdMqGyBaAfGSAMYDN47tXzifz1BLqaqTF0uNGXE0O9yVDEEbcIHNM9yA5bzbb0Nvj2Gg==;TableEndpoint=https://sbjournal.table.cosmosdb.azure.com:443/;";
        //public const string storage = "DefaultEndpointsProtocol=https;AccountName=jpdidentitystore;AccountKey=eRrzu2cHOcqVhkbgdbgTERqgH4JfdZ6TSZYp3n/u/iBztNACNGytNOg0K2y0+BCP2NyQ5vrzu2LydVaY229fcg==;BlobEndpoint=https://jpdidentitystore.blob.core.windows.net/;QueueEndpoint=https://jpdidentitystore.queue.core.windows.net/;TableEndpoint=https://jpdidentitystore.table.core.windows.net/;FileEndpoint=https://jpdidentitystore.file.core.windows.net/;";

        static void Main(string[] args)
        {
            var account = CloudStorageAccount.Parse(storage);
            var table = account.CreateCloudTableClient().GetTableReference("events");
            var sbClients = new List<ActiveReplicationQueueClient>()
            {
                new ActiveReplicationQueueClient(primarySb, "events", true),
                //new ActiveReplicationQueueClient(secondarySb, "events", false),
            };

            var r = new DataReceiver(table, sbClients);
            r.ReceiveMessage();
            Console.WriteLine("Waiting...");
            Console.ReadLine();
        }
    }
}