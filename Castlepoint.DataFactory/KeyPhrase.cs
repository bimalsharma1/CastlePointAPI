using System;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO;

using System.Threading;
using System.Threading.Tasks;

using Dasync.Collections;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using MongoDB.Bson;
using MongoDB.Driver;

using Newtonsoft.Json;

namespace Castlepoint.DataFactory
{
    public class MongoRecordAssociationKeyPhraseReverseWord : POCO.RecordAssociationKeyPhraseReverseWord
    {
        public ObjectId _id { get; set; }
    }

    public class AzureRecordAssociationKeyPhraseReverseWord : EntityAdapter<POCO.RecordAssociationKeyPhraseReverseWord>
    {
        public AzureRecordAssociationKeyPhraseReverseWord() { }
        public AzureRecordAssociationKeyPhraseReverseWord(POCO.RecordAssociationKeyPhraseReverseWord o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoRecordAssociationKeyPhraseReverse : POCO.RecordAssociationKeyPhraseReverse
    {
        public ObjectId _id { get; set; }
    }

    public class AzureRecordAssociationKeyPhraseReverse : EntityAdapter<POCO.RecordAssociationKeyPhraseReverse>
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

    public static class KeyPhrase
    {

        public static List<POCO.RecordAssociationKeyPhraseReverse> GetReverseKeyPhrases(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationKeyPhraseReverse> keyphrases = new List<POCO.RecordAssociationKeyPhraseReverse>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationKeyPhraseReverse> azdata = new List<AzureRecordAssociationKeyPhraseReverse>();
                    AzureTableAdaptor<AzureRecordAssociationKeyPhraseReverse> adaptor = new AzureTableAdaptor<AzureRecordAssociationKeyPhraseReverse>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordAssociationKeyPhraseReverse, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyphrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAssociationKeyPhraseReverse>(providerConfig, Record.MongoTableNames.RecordAssociationKeyPhraseReverse);

                    FilterDefinition<MongoRecordAssociationKeyPhraseReverse> filter = Utils.GenerateMongoFilter<MongoRecordAssociationKeyPhraseReverse>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyphrases.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return keyphrases;
        }

        public static List<POCO.RecordAssociationKeyPhraseReverseWord> GetReverseKeyPhraseWords(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;
            List<POCO.RecordAssociationKeyPhraseReverseWord> keyphrases = new List<POCO.RecordAssociationKeyPhraseReverseWord>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    //string combinedFilter = Utils.GenerateAzureFilter(filters);

                    //List<AzureNTFSFile> azdata = new List<AzureNTFSFile>();
                    //AzureTableAdaptor<AzureNTFSFile> adaptor = new AzureTableAdaptor<AzureNTFSFile>();
                    //azdata = adaptor.ReadTableData(providerConfig, NTFS.AzureTableNames.NTFSFiles, combinedFilter);

                    //foreach (var doc in azdata)
                    //{
                    //    files.Add(doc.Value);
                    //}

                    break;
                case "internal.mongodb":

                    var collection = Utils.GetMongoCollection<MongoRecordAssociationKeyPhraseReverseWord>(providerConfig, Record.MongoTableNames.RecordAssociationKeyPhraseReverseWords);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoRecordAssociationKeyPhraseReverseWord> filter = Utils.GenerateMongoFilter<MongoRecordAssociationKeyPhraseReverseWord>(filters);

                    //DEBUG output the filter values
                    //foreach (Castlepoint.DataFactory.Filter debugFilter in filters)
                    //{
                    //    // Output the filter field names and values
                    //    Console.WriteLine("DEBUG filter: " + debugFilter.FieldName + " : " + debugFilter.FieldValue);
                    //}
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyphrases.Add(keyphrase);
                    }

                    // Get the next page id
                    if (documents.Count == rowLimit)
                    {
                        // Set the next page id
                        nextPageId = documents[documents.Count - 1]._id.ToString();
                    }


                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return keyphrases;
        }

        public static List<POCO.RecordAssociationKeyPhraseReverseWord> GetReverseKeyPhraseWords(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationKeyPhraseReverseWord> keyphrases = new List<POCO.RecordAssociationKeyPhraseReverseWord>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationKeyPhraseReverseWord> azdata = new List<AzureRecordAssociationKeyPhraseReverseWord>();
                    AzureTableAdaptor<AzureRecordAssociationKeyPhraseReverseWord> adaptor = new AzureTableAdaptor<AzureRecordAssociationKeyPhraseReverseWord>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordAssociationKeyPhraseReverseWords, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyphrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAssociationKeyPhraseReverseWord>(providerConfig, Record.MongoTableNames.RecordAssociationKeyPhraseReverseWords);

                    FilterDefinition<MongoRecordAssociationKeyPhraseReverseWord> filter = Utils.GenerateMongoFilter<MongoRecordAssociationKeyPhraseReverseWord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyphrases.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return keyphrases;
        }

        public static string AddKeyPhraseReverseWords(DataConfig providerConfig, List<POCO.RecordAssociationKeyPhraseReverseWord> keyPhrases)
        {
            string tableName = "";
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = Record.AzureTableNames.RecordAssociationKeyPhraseReverseWords;

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);

                    keyPhrases.ParallelForEachAsync(
                    async kp =>
                    {
                        AzureRecordAssociationKeyPhraseReverseWord az = new AzureRecordAssociationKeyPhraseReverseWord(kp);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        TableResult update = await table.ExecuteAsync(operation);
                    }, 6);

                    //foreach (POCO.RecordAssociationKeyPhraseReverseWord kp in keyPhrases)
                    //{
                    //    AzureRecordAssociationKeyPhraseReverseWord az = new AzureRecordAssociationKeyPhraseReverseWord(kp);
                    //    TableOperation operation = TableOperation.InsertOrReplace(az);

                    //    Task tUpdate = table.ExecuteAsync(operation);
                    //    tUpdate.Wait();
                    //}

                    break;

                case "internal.mongodb":

                    tableName = Record.MongoTableNames.RecordAssociationKeyPhraseReverseWords;


                    IMongoCollection<MongoRecordAssociationKeyPhraseReverseWord> collection = Utils.GetMongoCollection<MongoRecordAssociationKeyPhraseReverseWord>(providerConfig, tableName);

                    var operationList = new List<WriteModel<MongoRecordAssociationKeyPhraseReverseWord>>();
                    foreach (POCO.RecordAssociationKeyPhraseReverseWord kp in keyPhrases)
                    {
                        // Convert to mongo-compatible object
                        MongoRecordAssociationKeyPhraseReverseWord mongoObject = Utils.ConvertType<MongoRecordAssociationKeyPhraseReverseWord>(kp);

                        // Create the filter for the upsert
                        FilterDefinition<MongoRecordAssociationKeyPhraseReverseWord> filter = Builders<MongoRecordAssociationKeyPhraseReverseWord>.Filter.Eq(x => x.PartitionKey, kp.PartitionKey) &
                            Builders<MongoRecordAssociationKeyPhraseReverseWord>.Filter.Eq(x => x.RowKey, kp.RowKey);

                        UpdateDefinition<MongoRecordAssociationKeyPhraseReverseWord> updateDefinition = new UpdateDefinitionBuilder<MongoRecordAssociationKeyPhraseReverseWord>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition
                            .SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("WordNumber", mongoObject.WordNumber)
                            .Set("TotalWords", mongoObject.TotalWords);

                        UpdateOneModel<MongoRecordAssociationKeyPhraseReverseWord> update = new UpdateOneModel<MongoRecordAssociationKeyPhraseReverseWord>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);

                    }

                    if (operationList.Count > 0)
                    {
                        collection.BulkWrite(operationList);
                    }

                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

    }

}
