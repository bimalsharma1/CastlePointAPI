using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    
        public class RecordSentenceHistory
        {
            public RecordSentenceHistory() { }

            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
        public string RASchemaUri { get; set; }
            public string Function { get; set; }
            public string ClassNo { get; set; }
            public string Activity { get; set; }
            public DateTime Created { get; set; }
            public DateTime LastUpdated { get; set; }
        public DateTime NextSentenceDate { get; set; }
        public DateTime ExpiryDate { get; set; }
        public DateTime LastCategorised { get; set; }
            public Guid LastCategorisedGuid { get; set; }
        public string JsonMatchSummary { get; set; }
    }
}
