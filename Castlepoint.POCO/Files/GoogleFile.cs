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

using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Drive.v3.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;

namespace Castlepoint.POCO.Files
{
    public class GoogleFile : Files.CPFile
    {
        public GoogleFile() { }
        public GoogleFile(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        
        public string ListId { get; set; }
        
        public long SizeInBytes { get; set; }
        public string UniqueId { get; set; }
        
        public new string Type { get { return "google.document"; } }

        
    }
}
