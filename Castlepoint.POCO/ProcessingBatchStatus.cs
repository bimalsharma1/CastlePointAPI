using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class ProcessingBatchStatus
    {
        public ProcessingBatchStatus()
        {
            this.PartitionKey = string.Empty;
            this.RowKey = string.Empty;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ProcessType { get; set; }
        public string SystemUri { get; set; }
        public string RecordUri { get; set; }
        public string ItemUri { get; set; }
    }
}
