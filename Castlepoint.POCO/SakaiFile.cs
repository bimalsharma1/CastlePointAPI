using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class SakaiFile : FileBatch
    {
        public SakaiFile() { }
        public SakaiFile(string partitionKey, string rowKey)
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
        public long SizeInBytes { get; set; }
        public string SourceFileName { get; set; }
        public string ItemUri { get; set; }
        public string SourceRelativeUrl { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime CreationTime { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public string UniqueId { get; set; }
        public long Version { get; set; }
        public string CPFolderStatus { get; set; }
        public string CreatedBy { get; set; }
        public string LastModifiedBy { get; set; }

    }

    public class SakaiFile_Patch001
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceRelativeUrl { get; set; }
    }

    public class Sakai_Patch001
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
    }

    public class Sakai_Patch003
    {
        public string ItemUri { get; set; }
    }

    //public class SakaiFileBatchStatus
    //{
    //    public SakaiFileBatchStatus() { }
    //    public SakaiFileBatchStatus(string partitionKey, string rowKey)
    //    {
    //        this.PartitionKey = partitionKey;
    //        this.RowKey = rowKey;
    //    }
    //    public string PartitionKey { get; set; }
    //    public string RowKey { get; set; }
    //    public Guid BatchGuid { get; set; }
    //    public string BatchStatus { get; set; }
    //    public string JsonFileProcessResult { get; set; }
    //}
}
