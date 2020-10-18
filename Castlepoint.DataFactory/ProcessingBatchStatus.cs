using System;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO;

using System.Threading;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using MongoDB.Bson;
using MongoDB.Driver;

using Newtonsoft.Json;

namespace Castlepoint.DataFactory
{
    public static class ProcessingBatchStatus
    {
        private static class AzureTableNames
        {
            internal const string SystemProcessingStatus = "stlpsystemprocessingstatus";
        }

        private static class MongoTableNames
        {
            internal const string SystemProcessingStatus = "systemprocessingstatus";
        }

        public class MongoProcessingBatchStatus : POCO.ProcessingBatchStatus
        {
            public ObjectId _id { get; set; }
        }

        public class AzureProcessingBatchStatus : EntityAdapter<POCO.ProcessingBatchStatus>
        {
            public AzureProcessingBatchStatus() { }
            public AzureProcessingBatchStatus(POCO.ProcessingBatchStatus o) : base(o) { }
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }

        public static string Add(DataConfig providerConfig, POCO.ProcessingBatchStatus processingStatus)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureProcessingBatchStatus az = new AzureProcessingBatchStatus(processingStatus);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SystemProcessingStatus);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    //TODO return the inserted record id/timestamp
                    return string.Empty;


                case "internal.mongodb":
                    IMongoCollection<MongoProcessingBatchStatus> collection = Utils.GetMongoCollection<MongoProcessingBatchStatus>(providerConfig, MongoTableNames.SystemProcessingStatus);
                    MongoProcessingBatchStatus mongoObject = Utils.ConvertType<MongoProcessingBatchStatus>(processingStatus);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return string.Empty;
        }

        public static void Delete(DataConfig providerConfig, POCO.ProcessingBatchStatus processingBatchStatus)
        {

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", processingBatchStatus.PartitionKey, "eq");
            filters.Add(pk);
            Filter rk = new Filter("RowKey", processingBatchStatus.RowKey, "eq");
            filters.Add(rk);
            Filter typekey = new Filter("ProcessType", processingBatchStatus.ProcessType, "eq");
            filters.Add(typekey);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureProcessingBatchStatus az = new AzureProcessingBatchStatus(processingBatchStatus);
                    az.ETag = "*";

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SystemProcessingStatus);
                    TableOperation operation = TableOperation.Delete(az);

                    Task<TableResult> tDelete = table.ExecuteAsync(operation);
                    tDelete.Wait();

                    // Check for "success with no status" code
                    if (tDelete.Result.HttpStatusCode != 204)
                    {
                        // TODO
                        bool isNotDeleted = true;
                    }

                    break;

                case "internal.mongodb":
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Delete the rows                      
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.SystemProcessingStatus);
                    DeleteResult result = collection.DeleteMany(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static List<POCO.ProcessingBatchStatus> GetProcessingStatus(DataConfig providerConfig)
        {
            List<POCO.ProcessingBatchStatus> processingStatus = new List<POCO.ProcessingBatchStatus>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<AzureProcessingBatchStatus> azs = new List<AzureProcessingBatchStatus>();
                    AzureTableAdaptor<AzureProcessingBatchStatus> az = new AzureTableAdaptor<AzureProcessingBatchStatus>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.SystemProcessingStatus);

                    foreach (var doc in azs)
                    {
                        processingStatus.Add(doc.Value);
                    }

                    return processingStatus;


                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoProcessingBatchStatus>(providerConfig, MongoTableNames.SystemProcessingStatus);

                    var documents = collection.Find(new BsonDocument()).ToList();

                    foreach (var doc in documents)
                    {
                        processingStatus.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return processingStatus;
        }
    }
}
