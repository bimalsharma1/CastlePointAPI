using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.DataFactory
{
    public class DataConfig
    {
        public string ProviderType { get; set; }
        public string ConnectionString { get; set; }
        public string DatabaseName { get; set; }
    }
    public static class ProviderType
    {
        public const string Azure = "azure.tableservice";
        public const string Mongo = "internal.mongodb";
    }

}
