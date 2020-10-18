using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class ManagedService
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public bool Enabled { get; set; }
    }
}
