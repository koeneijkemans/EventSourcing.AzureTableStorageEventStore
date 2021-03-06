using Microsoft.Azure.Cosmos.Table;
using System;

namespace Kei.EventSourcing.AzureTableStorageEventStore
{
    internal class TableEvent : TableEntity, IEventStoreItem
    {
        public Guid AggregateId { get; set; }

        public string EventType { get; set; }

        public int Order { get; set; }

        public string Data { get; set; }
    }
}
