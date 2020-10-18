using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class RecordProcessingHistory
    {
        public RecordProcessingHistory() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ProcessingResultAsJson { get; set; }
        public string ProcessResult { get; set; }
    }
}
