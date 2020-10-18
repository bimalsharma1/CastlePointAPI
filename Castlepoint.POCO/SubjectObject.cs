using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class SubjectObject
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Subject { get; set; }
        public string Object { get; set; }
        public string Relationship { get; set; }
    }
}
