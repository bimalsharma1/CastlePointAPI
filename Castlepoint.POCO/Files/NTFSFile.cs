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

using System.IO;

namespace Castlepoint.POCO.Files
{
    public class NTFSFile : Files.CPFile
    {
        public NTFSFile() { }
        public NTFSFile(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string Name { get; set; }
        public long SizeInBytes { get; set; }



    }

    internal class FileSecurityEntity : TableEntity
    {
        public string RoleDefinitionName { get; internal set; }
    }

    internal class SPOFileProperties
    {
        public Dictionary<string, string> ListItemAllFields { get; set; }
    }
}
