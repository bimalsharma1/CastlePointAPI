using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Castlepoint.POCO;
using Castlepoint.POCO.Config;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Attributes;

namespace Castlepoint.DataFactory
{
    public class MongoFileAlert : POCO.FileAlert
    {

    }

    public class AzureFileAlert : EntityAdapter<POCO.FileAlert>
    {
        public AzureFileAlert() { }
        public AzureFileAlert(POCO.FileAlert o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public static class FileAlert
    {

        public static void AddFileAlert(DataConfig providerConfig, POCO.FileAlert fileAlert, string tableName)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureFileAlert az = new AzureFileAlert(fileAlert);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoFileAlert> collection = Utils.GetMongoCollection<MongoFileAlert>(providerConfig, tableName);
                    MongoFileAlert mongoObject = Utils.ConvertType<MongoFileAlert>(fileAlert);
                    collection.InsertOne(mongoObject);
                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static List<POCO.FileAlert> GetFileAlert(DataConfig providerConfig, string tableName, List<Filter> filters)
        {
            List<POCO.FileAlert> batchstatus = new List<POCO.FileAlert>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureFileAlert> azdata = new List<AzureFileAlert>();
                    AzureTableAdaptor<AzureFileAlert> adaptor = new AzureTableAdaptor<AzureFileAlert>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        batchstatus.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoFileAlert>(providerConfig, tableName);

                    FilterDefinition<MongoFileAlert> filter = Utils.GenerateMongoFilter<MongoFileAlert>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        batchstatus.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return batchstatus;
        }

    }
}
