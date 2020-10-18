using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class FileBatch
    {
        public string BatchStatus { get; set; }
        public Guid BatchGuid { get; set; }
        public string JsonFileProcessResult { get; set; }
    }

    /// <summary>
    /// Used to update File Batch Status for files
    /// It has a reduced field set to avoid overwriting non-batch status related fields
    /// </summary>
    public class FileBatchStatus:FileBatch
    {
        public FileBatchStatus() { }
        public FileBatchStatus(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

    }
}
