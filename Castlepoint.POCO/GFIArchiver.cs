using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class GFIMailbox
    {
        public string DisplayName { get; set; }
        public string EmailAddress { get; set; }
        public string Name { get; set; }
        public string Id { get; set; }
    }

    public class GFIMessage : POCO.EMail
    {
        public long MarcId { get; set; }
        public DateTime ArchivedDate { get; set; }
    }

    public class GFIMessageEntity:AbstractDataEntity
    {
        public GFIMessageEntity() { }
        public GFIMessageEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.UniqueId = "";
            this.TimeCreated = Utils.AzureTableMinDateTime;
            this.TimeLastModified = Utils.AzureTableMinDateTime;
            this.ItemCount = 0;
            this.Name = "";
            this.Version = 0;
            this.CPStatus = "";
        }
    }

    public class GFIMessageBatchStatus
    {
        public GFIMessageBatchStatus() { }
        public GFIMessageBatchStatus(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public Guid BatchGuid { get; set; }
        public string BatchStatus { get; set; }
        public string JsonFileProcessResult { get; set; }
    }
}
