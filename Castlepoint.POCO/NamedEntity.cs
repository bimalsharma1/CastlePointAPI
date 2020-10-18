using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class NamedEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Type { get; set; }
        public string OriginalText { get; set; }
    }
}
