using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class FileAlert
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string DateAlert { get; set; }
        public string AlertType { get; set; }
    }
}
