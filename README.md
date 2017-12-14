# azure-service-bus-active-rep

## Active replication example for Azure Service Bus using Cosmos DB as a journal
An example of one way to maintain consistency + availability of Azure Service Bus across regions using an out-of-band processing journal
- Send availability via sending the same messages to _n_ queues
- Receive availability by logging work items in Cosmos DB (in strong consistency mode) to prevent processors from re-processing messages

## Projects
- [azure-service-bus-active-reciever-lib](azure-service-bus-active-receiver-lib/)
  - contains the bulk of the [receiver code](azure-service-bus-active-receiver-lib/DataReceiver.cs)
  - contains [`ActiveReplicationQueueClient`](azure-service-bus-active-receiver-lib/ActiveReplicationQueueClient.cs), a derived `QueueClient` that has some additional metadata; this is _optional_ and is only used for some log highlighting and tossing additional metadata into a `Message`
- [azure-service-bus-active-receiver](azure-service-bus-active-receiver)
  - contains a .net core console wrapper, with initialization
  - instantiates two queue clients, but could also use a single one (e.g., if deployed into a single region in Azure, while another instance in a failover region may have a different connection string)
- [azure-service-bus-active-receiver-netcore-webjob](azure-service-bus-active-receiver-netcore-webjob)
  - an Azure continuous Webjob wrapper for the receiver

## Notes
- Senders are using Service Bus Sessions to group and linearize messages
- Cosmos DB is using the Document API, mostly because there is no Cosmos Table API SDK for .net core yet - Tables would probably be a better use here than documents
- Cosmos DB is in strong consistency mode

## todo
- Switch from Session + Message journaling to just sessions, to prevent a race where messages are potentially processed out of order
- better docs
- clean-up and refactor to be more flexible with events

## To use this sample, you'll need
- _n_ Service Bus Queues
- A Cosmos DB account, with a document collection
