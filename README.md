# azure-service-bus-active-rep

## Active replication example for Azure Service Bus using Cosmos DB as a journal
An example of one way to maintain consistency + availability of Azure Service Bus across regions using an out-of-band processing journal
- Send availability via sending the same messages to _n_ queues
- Receive availability by logging work items in Cosmos DB (in strong consistency mode) to prevent processors from re-processing messages

## todo
- Switch from Session + Message journaling to just sessions, to prevent a race where messages are potentially processed out of order
- better docs
- clean-up and refactor to be more flexible with events