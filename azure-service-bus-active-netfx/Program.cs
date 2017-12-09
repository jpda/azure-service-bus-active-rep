using Microsoft.Azure.CosmosDB.Table;
using Microsoft.Azure.Storage;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace azure_service_bus_active_netfx
{
    class Program
    {
        public const string primarySb = "Endpoint=sb://active-east.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=r63QRPX6htDT/IJPUBH7AeWrO1zu9bY/q7OJSM2CGt0=";
        public const string secondarySb = "Endpoint=sb://active-ncentral.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=kTD23N+iwv/6UCeVQ5yytabob8aX5pYSs5YXjndf6Yw=";
        public const string storage = "DefaultEndpointsProtocol=https;AccountName=sbjournal;AccountKey=exbkLArPQAXx9IEgh9cdMqGyBaAfGSAMYDN47tXzifz1BLqaqTF0uNGXE0O9yVDEEbcIHNM9yA5bzbb0Nvj2Gg==;TableEndpoint=https://sbjournal.table.cosmosdb.azure.com:443/;";

        static void Main(string[] args)
        {
            var account = CloudStorageAccount.Parse(storage);
            var table = account.CreateCloudTableClient().GetTableReference("events");
            var sbClients = new List<QueueClient>()
            {
                QueueClient.CreateFromConnectionString(primarySb, "events"),
                //QueueClient.CreateFromConnectionString(secondarySb, "events")
            };

            var r = new DataReceiver(sbClients);
            r.ReceiveMessage();
            Console.WriteLine("Waiting...");
            Console.ReadLine();
        }
    }

    //public class ActiveReplicationQueueClient : QueueClient
    //{
    //    public bool IsPrimaryQueue { get; set; }
    //    public string Namespace { get; set; }
    //    public string ClientRef { get; set; }

    //    public ActiveReplicationQueueClient(ServiceBusConnectionStringBuilder connectionStringBuilder, bool isPrimary = true, ReceiveMode receiveMode = ReceiveMode.PeekLock, RetryPolicy retryPolicy = null) : base(connectionStringBuilder, receiveMode, retryPolicy)
    //    {
    //        IsPrimaryQueue = isPrimary;
    //        Namespace = connectionStringBuilder.Endpoint;
    //    }

    //    public ActiveReplicationQueueClient(string connectionString, string entityPath, bool isPrimary = true, ReceiveMode receiveMode = ReceiveMode.PeekLock, RetryPolicy retryPolicy = null) : base(connectionString, entityPath, receiveMode, retryPolicy)
    //    {
    //        IsPrimaryQueue = isPrimary;
    //        var sb = connectionString.Split('=')[1];
    //        if (Uri.TryCreate(sb, UriKind.Absolute, out Uri endpoint))
    //        {
    //            Namespace = endpoint.Host;
    //        }
    //    }

    //    public void RegisterSessionHandler(Func<IMessageSession, Message, CancellationToken, Task> handler, SessionHandlerOptions opts, string clientRefId)
    //    {
    //        ClientRef = clientRefId;
    //        base.RegisterSessionHandler(handler, opts);
    //    }
    //}

    public class ClientAwareSessionHandler : IMessageSessionAsyncHandler
    {
        CloudTable _table;
        private const string storage = "DefaultEndpointsProtocol=https;AccountName=sbjournal;AccountKey=exbkLArPQAXx9IEgh9cdMqGyBaAfGSAMYDN47tXzifz1BLqaqTF0uNGXE0O9yVDEEbcIHNM9yA5bzbb0Nvj2Gg==;TableEndpoint=https://sbjournal.table.cosmosdb.azure.com:443/;";
        public ClientAwareSessionHandler()
        {
            var account = CloudStorageAccount.Parse(storage);
            var table = account.CreateCloudTableClient().GetTableReference("events");
            _table = table;
        }
        public Task OnCloseSessionAsync(MessageSession session)
        {
            throw new NotImplementedException();
        }

        public async Task OnMessageAsync(MessageSession session, BrokeredMessage message)
        {
            var receiptTime = DateTime.UtcNow;
            var enqueuedTime = message.ScheduledEnqueueTimeUtc;
            var drift = receiptTime - enqueuedTime;
            //var client = _clients.Single(x => x.ClientRef == session.ClientId);
            //if (client.IsPrimaryQueue)
            //{
            //    Console.ForegroundColor = ConsoleColor.Green;
            //}
            //else
            //{
            //    Console.ForegroundColor = ConsoleColor.Yellow;
            //}

            Console.WriteLine($"Session received; Enqueued Time: {enqueuedTime}, Receipt Time: {receiptTime}, Drift: {drift}");
            var stream = message.GetBody<Stream>();
            var reader = new StreamReader(stream);
            var body = reader.ReadToEnd();
            Console.WriteLine($"Received Session: {session.SessionId} message: SequenceNumber: {message.SequenceNumber} Body:{body}");

            var messageStatus = await GetMessageFromLog(session.SessionId, message.MessageId);
            if (messageStatus == null) // no record exists, so let's create one and start working
            {
                var w = new WorkItem(session.SessionId, message.MessageId)
                {
                    Status = WorkItemStatus.Working,
                    LocalArrivalTime = receiptTime,
                    //WorkerIdentity = client.ClientId,
                    SequenceNumber = message.SequenceNumber,
                };
                var sw = new Stopwatch();
                sw.Start();
                var workItem = await AddWorkItemToLog(w);
                sw.Stop();
                Console.WriteLine($"Took {sw.ElapsedMilliseconds}ms to write to log");
                if (workItem.CanProcess)
                {
                    await ProcessMessage(message);
                }
            }
            else //record exists, so let's interrogate it 
            {
                Console.WriteLine($"{messageStatus.SessionId} {messageStatus.MessageId} currently {messageStatus.Status} by {messageStatus.WorkerIdentity}; CanProcess? {messageStatus.CanProcess}");
                if (messageStatus.CanProcess)
                {
                    Console.WriteLine("I can do work");
                }
            }
            await session.CompleteAsync(message.LockToken);
        }

        public Task OnSessionLostAsync(Exception exception)
        {
            throw new NotImplementedException();
        }

        public async Task ProcessMessage(BrokeredMessage message)
        {
            await Task.Delay(10000);
        }

        public async Task<WorkItem> GetMessageFromLog(string sessionId, string messageId)
        {
            var retrieveOperation = TableOperation.Retrieve<WorkItem>(sessionId, messageId);
            var result = await _table.ExecuteAsync(retrieveOperation);
            if (result.HttpStatusCode < 205)
            {
                var r = (WorkItem)result.Result;
                Console.WriteLine($"Receive Time: {r.Timestamp} Found message {r.MessageId} in session {r.SessionId}: {r.Status}");
                return r;
            }
            else return null;
        }

        public async Task<WorkItem> AddWorkItemToLog(WorkItem item)
        {
            item.Status = WorkItemStatus.Working;
            var t = TableOperation.Insert(item, true);
            var result = await _table.ExecuteAsync(t);
            if (result.HttpStatusCode == 200)
            {
                return result.Result as WorkItem;
            }
            if (result.HttpStatusCode > 204)
            {
                //check this
                return result.Result as WorkItem;
            }
            return item; // returns original item
        }

        public async Task<WorkItem> UpdateWorkItem(WorkItem item)
        {
            var t = TableOperation.Replace(item);
            var result = await _table.ExecuteAsync(t);
            if (result.HttpStatusCode == 200)
            {
                return item;
            }
            return item;
        }
    }

    public class TableOps
    {
        private CloudTable _table;
        public TableOps(CloudTable table)
        {
            _table = table;
        }
    }

    public class DataReceiver
    {
        private List<QueueClient> _clients;

        public DataReceiver(List<QueueClient> clients)
        {
            _clients = clients;
        }

        public void ReceiveMessage()
        {
            var opts = new SessionHandlerOptions()
            {
                AutoComplete = false,
                AutoRenewTimeout = TimeSpan.FromSeconds(120),
                MaxConcurrentSessions = 2,
                MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            foreach (var c in _clients)
            {
                c.RegisterSessionHandler(typeof(ClientAwareSessionHandler));
            }
        }
    }

    public class WorkItem : TableEntity
    {
        [IgnoreProperty]
        public string SessionId => PartitionKey;
        [IgnoreProperty]
        public string MessageId => RowKey;
        [IgnoreProperty]
        public bool CanProcess
        {
            get
            {
                return Status == WorkItemStatus.Unassigned;
            }
        }
        public long SequenceNumber { get; set; }
        public DateTime LocalArrivalTime { get; set; }
        public WorkItemStatus Status { get; set; }
        public string WorkerIdentity { get; set; }

        public WorkItem(string sessionId, string messageId)
        {
            PartitionKey = sessionId;
            RowKey = messageId;
        }

        public WorkItem() { }
    }

    public enum WorkItemStatus
    {
        Unassigned,
        Working,
        Completed,
        Faulted,
        Unknown
    }
}
