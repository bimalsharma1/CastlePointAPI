using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class LogLastAction
    {
        public LogLastAction() {
            this.TimeCreatedUTC = DateTime.UtcNow;
                }
        public LogLastAction(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.TimeCreatedUTC = DateTime.UtcNow;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string EventName { get; set; }
        public string LastAction { get; set; }
        public DateTime TimeCreatedUTC { get; set; }
    }
}
