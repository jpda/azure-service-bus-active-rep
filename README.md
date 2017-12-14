# azure-service-bus-active-rep

## Active replication example for Azure Service Bus using Cosmos DB as a journal
An example of one way to maintain consistency + availability of Azure Service Bus across regions using an out-of-band processing journal
- Send availability via sending the same messages to _n_ queues
- Receive availability by logging work items in Cosmos DB (in strong consistency mode) to prevent processors from re-processing messages

## Projects
- [/jpda/azure-service-bus-active-rep/tree/master/azure-service-bus-active-receiver-lib](Main lib)

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
