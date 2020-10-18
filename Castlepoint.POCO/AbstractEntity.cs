using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public abstract class AbstractDataEntity
    {

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public string UniqueId { get; set; }
        public long Version { get; set; }
        public string CPStatus { get; set; }
    }
}
