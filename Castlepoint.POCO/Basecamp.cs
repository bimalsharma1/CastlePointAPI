using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class BasecampEntity
    {
        public int id { get; set; }
        public string status { get; set; }
        public DateTime created_at { get; set; }
        public DateTime updated_at { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public string purpose { get; set; }
        public bool clients_enabled { get; set; }
        public string bookmark_url { get; set; }
        public string url { get; set; }
        public string app_url { get; set; }

        public List<BasecampDock> dock { get; set; }

    }

    public class BasecampDock
    {
        public int id { get; set; }
        public string title { get; set; }
        public string name { get; set; }
        public bool enabled { get; set; }
	    public string position { get; set; }
        public string url { get; set; }
        public string app_url { get; set; }
    }

    public class BasecampVault
    {
        public int id { get; set; }
        public string status { get; set; }
        public bool visible_to { get; set; }
        public DateTime created_at { get; set; }
        public DateTime updated_at { get; set; }
        public string title { get; set; }
        public bool inherits_status { get; set; }
        public string type { get; set; }
        public string url { get; set; }
        public int documents_count { get; set; }
        public string documents_url { get; set; }
        public int uploads_count { get; set; }
        public string uploads_url { get; set; }
        public int vaults_count { get; set; }
        public string vaults_url { get; set; }

    }

    public class BasecampDocument
    {
        public int id { get; set; }
        public string status {get; set;}
        public bool visible_to_clients { get; set; }

        public DateTime created_at { get; set; }
        public DateTime updated_at { get; set; }
        public string title { get; set; }
        public bool inherits_status { get; set; }
        public string type { get; set; }
        public string url { get; set; }
        public string app_url { get; set; }
        public string content { get; set; }

    }

    public class BasecampDocumentEntity : AbstractDataEntity
    {
        public BasecampDocumentEntity() { }
        public BasecampDocumentEntity(string partitionKey, string rowKey)
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
        public string Title { get; set; }
    }

    public class BasecampFileBatchStatus
    {
        public BasecampFileBatchStatus() { }
        public BasecampFileBatchStatus(string partitionKey, string rowKey)
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
    public class BasecampProjectEntity:AbstractDataEntity
    {
        public BasecampProjectEntity() { }
        public BasecampProjectEntity(string partitionKey, string rowKey)
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
}
