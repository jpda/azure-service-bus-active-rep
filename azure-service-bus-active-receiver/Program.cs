using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Text;

namespace azure_service_bus_active_receiver
{
    class Program
    {
        public const string primarySb = "Endpoint=sb://active-east.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=r63QRPX6htDT/IJPUBH7AeWrO1zu9bY/q7OJSM2CGt0=";
        public const string secondarySb = "Endpoint=sb://active-ncentral.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=kTD23N+iwv/6UCeVQ5yytabob8aX5pYSs5YXjndf6Yw=";
        static void Main(string[] args)
        {
            var r = new DataReceiver(primarySb, secondarySb);
            r.ReceiveMessage();
            Console.WriteLine("Waiting...");
            Console.ReadLine();
        }
    }

    public class DataReceiver
    {
        private List<QueueClient> _clients;

        public DataReceiver(List<QueueClient> clients)
        {
            _clients = clients;
        }

        public DataReceiver(params string[] queueEndpoints) : this(queueEndpoints.Select(x => new QueueClient(x, "events")).ToList()) { }

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

        static async Task ProcessSessionMessagesAsync(IMessageSession session, Message message, CancellationToken token)
        {
            Console.WriteLine($"Received Session: {session.SessionId} message: SequenceNumber: {message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await session.CompleteAsync(message.SystemProperties.LockToken);
        }

        public static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs x)
        {
            Console.BackgroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.UtcNow.ToString("o")}:{x.ExceptionReceivedContext.Endpoint}:{x.ExceptionReceivedContext.Action}:{x.Exception.Message}");
            return Task.CompletedTask;
        }
    }
}
