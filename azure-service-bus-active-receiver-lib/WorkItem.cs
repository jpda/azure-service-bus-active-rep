using System;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace azure_service_bus_active_receiver_lib
{
    public class WorkItem
    {
        [JsonProperty(PropertyName = "id")]
        public string Id => $"{SessionId}_{MessageId}";
        [JsonProperty(PropertyName = "sessionId")]
        public string SessionId { get; set; }
        [JsonProperty(PropertyName = "messageId")]
        public string MessageId { get; set; }
        [JsonIgnore]
        public bool CanProcess
        {
            get
            {
                return Status == WorkItemStatus.Unassigned;
            }
        }
        [JsonProperty(PropertyName = "sequence")]
        public long SequenceNumber { get; set; }
        [JsonProperty(PropertyName = "localArrivalTime")]
        public DateTime LocalArrivalTime { get; set; }
        [JsonProperty(PropertyName = "status")]
        public WorkItemStatus Status { get; set; }
        [JsonProperty(PropertyName = "workerId")]
        public string WorkerIdentity { get; set; }

        public WorkItem(string sessionId, string messageId, WorkItemStatus status)
        {
            SessionId = sessionId;
            MessageId = messageId;
            Status = status;
        }
        public static string GenerateId(string sessionId, string messageId)
        {
            return $"{sessionId}_{messageId}";
        }
    }
}