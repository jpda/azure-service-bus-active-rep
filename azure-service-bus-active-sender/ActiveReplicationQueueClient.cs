using Microsoft.Azure.ServiceBus;
using System;

namespace azure_service_bus_active_sender
{
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
}
