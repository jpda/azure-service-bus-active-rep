using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;

namespace azure_service_bus_active_receiver_lib
{
    public interface IEventRepo<T> where T : class
    {
        Task<Document> CreateEventAsync(T item);
        Task DeleteEventAsync(string id);
        Task<T> GetEventAsync(string sessionId, string messageId);
        Task<T> GetEventAsync(string id);
        Task<IEnumerable<T>> GetEventsAsync(Expression<Func<T, bool>> predicate);
        Task<Document> UpdateEventAsync(string id, T item);
    }
}