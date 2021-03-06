using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

using Microsoft.Extensions.Options;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Net.Http;
using System.Dynamic;

namespace Castlepoint.POCO.Files
{
    public class EOMessage : Files.CPFile
    {
        public EOMessage() { }
        public EOMessage(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

    }

}
