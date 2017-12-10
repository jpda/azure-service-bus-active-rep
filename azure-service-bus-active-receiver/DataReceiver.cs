using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Text;
using System.Diagnostics;
using Microsoft.Azure.Documents.Client;

namespace azure_service_bus_active_receiver
{
    public class DataReceiver
    {
        private List<ActiveReplicationQueueClient> _clients;
        private EventRepo<WorkItem> _events;

        public DataReceiver(EventRepo<WorkItem> events, List<ActiveReplicationQueueClient> clients)
        {
            _events = events;
            _clients = clients;
        }

        public void ReceiveMessage()
        {
            var opts = new SessionHandlerOptions(ExceptionReceivedHandler)
            {
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromSeconds(120),
                MaxConcurrentSessions = 10,
                MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            foreach (var c in _clients)
            {
                c.RegisterSessionHandler(ProcessSessionMessagesAsync, opts);
            }
        }

        public async Task ProcessSessionMessagesAsync(IMessageSession session, Message message, CancellationToken token)
        {
            var isPrimary = message.UserProperties.ContainsKey("IsPrimary") ? (bool)message.UserProperties.Single(x => x.Key == "IsPrimary").Value : true;
            var receiptTime = DateTime.UtcNow;
            var enqueuedTime = message.SystemProperties.EnqueuedTimeUtc;
            var drift = receiptTime - enqueuedTime;

            if (isPrimary)
            {
                Console.ForegroundColor = ConsoleColor.Green;
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
            }

            var wait = false;
            var loop = 0;
            do
            {
                // todo: introduce delay manager here for retry backoff
                var workItem = await CheckForExistingWorkItem(session, message);
                Console.WriteLine($"{workItem.SessionId} {workItem.MessageId}: {workItem.Status}");
                switch (workItem.Status)
                {
                    case WorkItemStatus.Completed:
                        await session.CompleteAsync(message.SystemProperties.LockToken);
                        return;
                    case WorkItemStatus.Working:
                        Console.WriteLine($"Work item shows as in progress, waiting {loop} sec to recheck");
                        loop++;
                        wait = true;
                        await Task.Delay(loop * 1000);
                        if (loop > 5)
                        { // could be broken or other process is out of control, complete after some reasonable time frame
                            wait = false;
                            await session.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        break;
                    case WorkItemStatus.Faulted:
                        // could be an alternative here, like moving to DLQ
                        await session.CompleteAsync(message.SystemProperties.LockToken);
                        return;
                    case WorkItemStatus.Unassigned:
                        workItem.Status = WorkItemStatus.Working;
                        workItem.LocalArrivalTime = receiptTime;
                        workItem.SequenceNumber = message.SystemProperties.SequenceNumber;
                        string workItemId;
                        try
                        {
                            workItemId = await AddWorkItemToLog(workItem);
                        }
                        catch (Exception ex) //todo: put this into a loop; faulted due to conflict, pause and re-check
                        {
                            Console.WriteLine($"Concurrency error: {ex.Message}");
                            wait = true;
                            break;
                        }
                        try
                        {
                            await ProcessMessage(message);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            workItem.Status = WorkItemStatus.Faulted;
                            return;
                        }
                        await session.CompleteAsync(message.SystemProperties.LockToken);
                        workItem.Status = WorkItemStatus.Completed;
                        await UpdateWorkItem(workItemId, workItem);
                        break;
                    default:
                        break;
                }
            } while (wait);
        }

        private async Task<WorkItem> CheckForExistingWorkItem(IMessageSession session, Message message)
        {
            // if the work item doesn't exist in the database, we'll get a stub listed as unassigned
            var workItem = await GetMessageFromLog(session.SessionId, message.MessageId);
            Console.WriteLine($"{workItem.SessionId} {workItem.MessageId} currently {workItem.Status}; CanProcess? {workItem.CanProcess}");

            return workItem;
        }

        public static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs x)
        {
            Console.BackgroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.UtcNow.ToString("o")}:{x.ExceptionReceivedContext.Endpoint}:{x.ExceptionReceivedContext.Action}:{x.Exception.Message}");
            return Task.CompletedTask;
        }

        public async Task<WorkItem> GetMessageFromLog(string sessionId, string messageId)
        {
            var thing = await _events.GetEventAsync(sessionId, messageId);
            return thing == null ? new WorkItem(sessionId, messageId, WorkItemStatus.Unassigned) : thing;
        }

        public async Task<string> AddWorkItemToLog(WorkItem item)
        {
            item.Status = WorkItemStatus.Working;
            var doc = await _events.CreateItemAsync(item);
            return doc.Id; // returns original item
        }

        public async Task<string> UpdateWorkItem(string docId, WorkItem item)
        {
            var doc = await _events.UpdateItemAsync(docId, item);
            return doc.Id;
        }

        public async Task ProcessMessage(Message message)
        {
            await Task.Delay(150);
        }
    }
}
