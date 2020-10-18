using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class NTFSFolder
    {
        public NTFSFolder() { }
        public NTFSFolder(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.UniqueId = "";
            this.TimeCreated = Utils.AzureTableMinDateTime;
            this.TimeLastModified = Utils.AzureTableMinDateTime;
            this.ItemCount = 0;
            this.Name = "";
            this.Version = 0;
            this.CPFolderStatus = "";
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public string UniqueId { get; set; }
        public long Version { get; set; }
        public string CPFolderStatus { get; set; }
    }
}
