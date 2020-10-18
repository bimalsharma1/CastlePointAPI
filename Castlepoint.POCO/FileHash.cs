using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class FileHash
    {
        public FileHash() { }
        public FileHash(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string BatchGuid { get; set; }
        public long HashValue { get; set; }
        public string HashType { get; set; }
        public int FileLength { get; set; }
        public DateTime LastModified { get; set; }
    }
}
