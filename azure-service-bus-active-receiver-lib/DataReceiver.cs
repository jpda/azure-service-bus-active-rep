using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;

namespace azure_service_bus_active_receiver_lib
{

    public class DataReceiverEvents
    {
        public Func<IMessageSession, Message, Task> WorkItemCompletedAction { get; set; }
    }

    public class DataReceiver<T>
    {
        private IList<ActiveReplicationQueueClient> _clients;
        private IEventRepo<WorkItem> _events;
        private Func<Message, Task> _messageProcessAction;
        private SessionHandlerOptions _sessionOptions;
        private IDictionary<WorkItemStatus, Func<IMessageSession, Message, Task>> _workItemStatusAction;

        public DataReceiver(IEventRepo<WorkItem> events, IList<ActiveReplicationQueueClient> clients, Func<Message, Task> messageProcessAction, SessionHandlerOptions sessionOptions = null)
        {
            _events = events;
            _clients = clients;
            _messageProcessAction = messageProcessAction;
            if (sessionOptions == null)
            {
                sessionOptions = new SessionHandlerOptions(ExceptionReceivedHandler)
                {
                    AutoComplete = false,
                    MaxAutoRenewDuration = TimeSpan.FromSeconds(120),
                    MaxConcurrentSessions = 10,
                    MessageWaitTimeout = TimeSpan.FromSeconds(30)
                };
            }
            _sessionOptions = sessionOptions;
        }

        public void Start()
        {
            foreach (var c in _clients)
            {
                c.RegisterSessionHandler(ProcessSessionMessagesAsync, _sessionOptions);
            }
        }

        private async Task ProcessSessionMessagesAsync(IMessageSession session, Message message, CancellationToken token)
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
                Trace.TraceInformation($"{workItem.SessionId} {workItem.MessageId}: {workItem.Status}");

                // todo: should also probably make this extensible so each action can be customized
                //await _workItemStatusAction[workItem.Status](session, message);

                switch (workItem.Status)
                {
                    case WorkItemStatus.Completed:
                        await session.CompleteAsync(message.SystemProperties.LockToken);
                        return;
                    case WorkItemStatus.Working:
                        Trace.TraceInformation($"Work item shows as in progress, waiting {loop} sec to recheck");
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
                            Trace.TraceError($"Data conflict: {ex.Message}");
                            wait = true;
                            break;
                        }
                        try
                        {
                            await ProcessMessageAsync(message);
                        }
                        catch (Exception ex)
                        {
                            Trace.TraceError(ex.Message);
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
            var workItem = await GetEventFromLog(session.SessionId, message.MessageId);
            Trace.TraceInformation($"{workItem.SessionId} {workItem.MessageId} currently {workItem.Status}; CanProcess? {workItem.CanProcess}");
            return workItem;
        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs x)
        {
            Trace.TraceError($"{DateTime.UtcNow.ToString("o")}:{x.ExceptionReceivedContext.Endpoint}:{x.ExceptionReceivedContext.Action}:{x.Exception.Message}");
            return Task.CompletedTask;
        }

        private async Task<WorkItem> GetEventFromLog(string sessionId, string messageId)
        {
            var thing = await _events.GetEventAsync(sessionId, messageId);
            return thing ?? new WorkItem(sessionId, messageId, WorkItemStatus.Unassigned);
        }

        private async Task<string> AddWorkItemToLog(WorkItem item)
        {
            item.Status = WorkItemStatus.Working;
            var doc = await _events.CreateEventAsync(item);
            return doc.Id;
        }

        private async Task<string> UpdateWorkItem(string docId, WorkItem item)
        {
            var doc = await _events.UpdateEventAsync(docId, item);
            return doc.Id;
        }

        private async Task ProcessMessageAsync(Message message)
        {
            await _messageProcessAction(message);
        }
    }
}