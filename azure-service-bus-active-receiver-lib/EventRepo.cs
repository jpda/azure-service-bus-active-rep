using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace azure_service_bus_active_receiver_lib
{
    public class EventRepo<T> : IEventRepo<T> where T : class
    {
        private DocumentClient _client;
        private string _databaseId;
        private string _collectionId;
        public EventRepo(string endpoint, string key, string db, string collection) : this(new DocumentClient(new Uri(endpoint), key), db, collection) { }
        public EventRepo(DocumentClient client, string db, string collection)
        {
            _client = client;
            _databaseId = db;
            _collectionId = collection;
        }

        public async Task<T> GetEventAsync(string sessionId, string messageId)
        {
            return await GetEventAsync(WorkItem.GenerateId(sessionId, messageId));
        }

        public async Task<T> GetEventAsync(string id)
        {
            try
            {
                var document = await _client.ReadDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id));
                return (T)(dynamic)document.Resource;
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null;
                }
                else
                {
                    throw;
                }
            }
        }

        public async Task<IEnumerable<T>> GetEventsAsync(Expression<Func<T, bool>> predicate)
        {
            IDocumentQuery<T> query = _client.CreateDocumentQuery<T>(
                UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId),
                new FeedOptions { MaxItemCount = -1 })
                .Where(predicate)
                .AsDocumentQuery();

            List<T> results = new List<T>();
            while (query.HasMoreResults)
            {
                results.AddRange(await query.ExecuteNextAsync<T>());
            }

            return results;
        }

        public async Task<Document> CreateEventAsync(T item)
        {
            var response = await _client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), item);
            return response.Resource;
        }

        public async Task<Document> UpdateEventAsync(string id, T item)
        {
            var response = await _client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id), item);
            return response.Resource;
        }

        public async Task DeleteEventAsync(string id)
        {
            await _client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id));
        }
    }
}