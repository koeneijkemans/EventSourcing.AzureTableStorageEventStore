using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kei.EventSourcing.AzureTableStorageEventStore
{
    public class AzureTableStorageEventStore : EventStore
    {
        private CloudTable _cloudTable;

        /// <summary>
        /// Initalizes a new instance of the <see cref="AzureTableStorageEventStore" />
        /// </summary>
        /// <param name="connectionString">Connection string of the azure event store.</param>
        /// <param name="eventPublisher">Instance of the event publisher.</param>
        public AzureTableStorageEventStore(string connectionString, EventPublisher eventPublisher)
            : base(eventPublisher)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            CloudTableClient client = account.CreateCloudTableClient();
            _cloudTable = client.GetTableReference("events");
            _cloudTable.CreateIfNotExistsAsync();
        }

        /// <summary>
        /// Retrieves all events with the provided aggregate id
        /// </summary>
        /// <param name="aggregateId">The aggregate id</param>
        /// <returns>A list of events</returns>
        public override List<Event> Get(Guid aggregateId)
        {
            TableQuery<TableEvent> query = new TableQuery<TableEvent>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, aggregateId.ToString()));

            List<Event> allEvents = new List<Event>();
            TableContinuationToken token = new TableContinuationToken();

            do
            {
                TableQuerySegment<TableEvent> events = _cloudTable.ExecuteQuerySegmented(query, token);
                token = events.ContinuationToken;
            
                foreach (var @event in events.Results)
                {
                    allEvents.Add(ToEvent(@event));
                }
            } while (token != null);

            return allEvents
                .OrderBy(e => e.Order)
                .ToList();
        }

        protected override void SaveInStore(Event @event)
        {
            TableEvent tableEvent = new TableEvent
            {
                PartitionKey = @event.AggregateRootId.ToString(),
                RowKey = DateTime.UtcNow.Ticks.ToString(),
                AggregateId = @event.AggregateRootId,
                EventType = @event.GetType().Name,
                Order = @event.Order,
                Data = JsonConvert.SerializeObject(@event)
            };

            TableOperation newEvent = TableOperation.Insert(tableEvent);
            _cloudTable.Execute(newEvent);
        }
    }
}
