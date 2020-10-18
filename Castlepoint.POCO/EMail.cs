using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class EMail
    {
        public DateTime SentDate { get; set; }
        public DateTime ReceivedDate { get; set; }
        public List<string> ToRecipients { get; set; }
        public List<string> CCRecipients { get; set; }
        public List<string> BCCRecipients { get; set; }
        public int Priority { get; set; }
        public int Size { get; set; }
        public string Subject { get; set; }
    }

    public class EmailBatchStatus
    {
        public EmailBatchStatus() { }
        public EmailBatchStatus(string partitionKey, string rowKey)
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
