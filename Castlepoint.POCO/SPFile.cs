using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class SPFile:FileBatch
    {
        public SPFile() { }
        public SPFile(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.UniqueId = "";
            this.CreationTime = Utils.AzureTableMinDateTime;
            this.LastModifiedTime = Utils.AzureTableMinDateTime;
            this.ItemCount = 0;
            this.Name = "";
            this.Version = 0;
            this.CPFolderStatus = "";
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string SourceFileName { get; set; }
        public string ItemUri { get; set; }
        public string SiteUrl { get; set; }
        public string SourceRelativeUrl { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime CreationTime { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public string UniqueId { get; set; }
        public long Version { get; set; }
        public string CPFolderStatus { get; set; }
        public long SizeInBytes { get; set; }
        public Guid OrganisationId { get; set; }
        public string MIMEType { get; set; }
    }

    public class SPFileBatchStatus
    {
        public SPFileBatchStatus() { }
        public SPFileBatchStatus(string partitionKey, string rowKey)
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
