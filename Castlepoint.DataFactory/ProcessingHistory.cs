using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    public class ProcessingHistory
    {
        public class MongoRecordProcessingHistory : POCO.RecordProcessingHistory
        {
            public ObjectId _id { get; set; }
        }

        public class AzureRecordProcessingHistory : EntityAdapter<POCO.RecordProcessingHistory>
        {
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }

        internal static class AzureTableNames
        {
            internal const string RecordProcessingHistory = "stlplogrecordprocessing";
        }

        internal static class MongoTableNames
        {
            internal const string RecordProcessingHistory = "logrecordprocessing";
        }



        public static List<POCO.RecordProcessingHistory> GetForRecord(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordProcessingHistory> ProcessingHistory = new List<POCO.RecordProcessingHistory>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordProcessingHistory> azdata = new List<AzureRecordProcessingHistory>();
                    AzureTableAdaptor<AzureRecordProcessingHistory> adaptor = new AzureTableAdaptor<AzureRecordProcessingHistory>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordProcessingHistory, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        ProcessingHistory.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordProcessingHistory>(providerConfig, MongoTableNames.RecordProcessingHistory);

                    FilterDefinition<MongoRecordProcessingHistory> filter = Utils.GenerateMongoFilter<MongoRecordProcessingHistory>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var logentry in documents)
                    {
                        ProcessingHistory.Add(logentry);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ProcessingHistory;
        }

    }
}
