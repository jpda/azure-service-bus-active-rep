using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Text;
using Microsoft.Azure.Storage;
using System.Diagnostics;

namespace azure_service_bus_active_receiver
{
    public class DataReceiver
    {
        private List<ActiveReplicationQueueClient> _clients;
        private CloudTable _table;

        public DataReceiver(CloudTable table, List<ActiveReplicationQueueClient> clients)
        {
            _clients = clients;
            _table = table;
        }

        public void ReceiveMessage()
        {
            var opts = new SessionHandlerOptions(ExceptionReceivedHandler)
            {
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromSeconds(120),
                MaxConcurrentSessions = 2,
                MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            foreach (var c in _clients)
            {
                c.RegisterSessionHandler(ProcessSessionMessagesAsync, opts);
            }
        }

        public async Task ProcessSessionMessagesAsync(IMessageSession session, Message message, CancellationToken token)
        {
            //await session.CompleteAsync(message.SystemProperties.LockToken);
            //return;
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

            Console.WriteLine($"{session.ClientId} Session received; Enqueued Time: {enqueuedTime}, Receipt Time: {receiptTime}, Drift: {drift}");
            Console.WriteLine($"Received Session: {session.SessionId} message: SequenceNumber: {message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            var messageStatus = await GetMessageFromLog(session.SessionId, message.MessageId);
            if (messageStatus == null) // no record exists, so let's create one and start working
            {
                var w = new WorkItem(session.SessionId, message.MessageId)
                {
                    Status = WorkItemStatus.Working,
                    LocalArrivalTime = receiptTime,
                    //WorkerIdentity = client.ClientId,
                    SequenceNumber = message.SystemProperties.SequenceNumber,
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
            await session.CompleteAsync(message.SystemProperties.LockToken);
        }

        public static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs x)
        {
            Console.BackgroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.UtcNow.ToString("o")}:{x.ExceptionReceivedContext.Endpoint}:{x.ExceptionReceivedContext.Action}:{x.Exception.Message}");
            return Task.CompletedTask;
        }

        public async Task<WorkItem> GetMessageFromLog(string sessionId, string messageId)
        {
            var query = new TableQuery<WorkItem>().Where(TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, sessionId), TableOperators.And, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, messageId)));
            try
            {
                var results = await _table.ExecuteQuerySegmentedAsync(query, null); // should loop here if potentially a single session has > 1000 messages
                if (results.Any())
                {
                    foreach (var r in results)
                    {
                        Console.WriteLine($"Receive Time: {r.Timestamp} Found message {r.MessageId} in session {r.SessionId}: {r.Status}");
                    }
                    return results.Single();
                }
                return null;
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
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

        public async Task ProcessMessage(Message message)
        {
            await Task.Delay(10000);
        }
    }
}
