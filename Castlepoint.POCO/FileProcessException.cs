using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class FileProcessException
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime DateProcessed { get; set; }
    }
}
