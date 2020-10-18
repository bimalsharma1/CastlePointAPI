using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    public class SentenceHistory
    {
        public class MongoRecordSentenceHistory : POCO.RecordSentenceHistory
        {
            public ObjectId _id { get; set; }
        }

        public class AzureRecordSentenceHistory : EntityAdapter<POCO.RecordSentenceHistory>
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
            internal const string RecordSentenceHistory = "stlprecordssentencehistory";
        }

        internal static class MongoTableNames
        {
            internal const string RecordSentenceHistory = "recordssentencehistory";
        }



        public static List<POCO.RecordSentenceHistory> GetForRecord(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordSentenceHistory> sentenceHistory = new List<POCO.RecordSentenceHistory>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordSentenceHistory> azdata = new List<AzureRecordSentenceHistory>();
                    AzureTableAdaptor<AzureRecordSentenceHistory> adaptor = new AzureTableAdaptor<AzureRecordSentenceHistory>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordSentenceHistory, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        sentenceHistory.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordSentenceHistory>(providerConfig, MongoTableNames.RecordSentenceHistory);

                    FilterDefinition<MongoRecordSentenceHistory> filter = Utils.GenerateMongoFilter<MongoRecordSentenceHistory>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var logentry in documents)
                    {
                        sentenceHistory.Add(logentry);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return sentenceHistory;
        }

    }
}
