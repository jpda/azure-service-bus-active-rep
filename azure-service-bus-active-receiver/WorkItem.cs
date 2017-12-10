using System;

namespace azure_service_bus_active_receiver
{
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
        [IgnoreProperty]
        public WorkItemStatus Status { get; set; }
        public string WorkerIdentity { get; set; }

        public WorkItem(string sessionId, string messageId)
        {
            PartitionKey = sessionId;
            RowKey = messageId;
        }

        public WorkItem() { }
    }
}
