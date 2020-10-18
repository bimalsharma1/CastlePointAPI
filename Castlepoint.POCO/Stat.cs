using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class Stat
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string StatsType { get; set; }
        public string JsonStats { get; set; }
    }

    public class RecordStat
    {
        public RecordStat() { }
        public RecordStat(string systemUri, string recordUri)
        {
            this.PartitionKey = systemUri;
            this.RowKey = recordUri;
            this.Stats = "";
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        // GM 20190104 commented out - LastContentsUpdated should only be changed via ProcessFile and ProcessRecord, not stats counting
        //public DateTime LastContentsUpdated { get; set; }
        public string Stats { get; set; }
    }

}
