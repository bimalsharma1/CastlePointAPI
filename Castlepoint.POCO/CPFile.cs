using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class CPFileMIMEType
    {
        public CPFileMIMEType() { }
        public CPFileMIMEType(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string MIMEType { get; set; }
    }

    public class CPFileSize
    {
        public CPFileSize() { }
        public CPFileSize(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public long SizeInBytes { get; set; }
    }
}
