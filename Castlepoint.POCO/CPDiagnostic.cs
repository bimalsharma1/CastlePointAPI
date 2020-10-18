using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class CPDiagnostic
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Event { get; set; }
        public string ItemUri { get; set; }
        public string MIMEType { get; set; }
        public int NumBytes { get; set; }
        public double TotalMilliseconds { get; set; }
    }
}
