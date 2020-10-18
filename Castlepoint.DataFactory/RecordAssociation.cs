using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;

using Dasync.Collections;

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
    class MongoRecordAuthorityMatchStatus : POCO.RecordAuthorityMatchStatus
    {
        public ObjectId _id { get; set; }
    }

    class AzureRecordAuthorityMatchStatus : EntityAdapter<POCO.RecordAuthorityMatchStatus>
    {
        public AzureRecordAuthorityMatchStatus() { }
        public AzureRecordAuthorityMatchStatus(POCO.RecordAuthorityMatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoRecordAssociationToRecord : POCO.RecordAssociationToRecord
    {
        public ObjectId _id { get; set; }
    }

    class AzureRecordAssociationToRecord : EntityAdapter<POCO.RecordAssociationToRecord>
    {
        public AzureRecordAssociationToRecord() { }
        public AzureRecordAssociationToRecord(POCO.RecordAssociationToRecord o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoKeyPhraseCount : POCO.RecordAssociationKeyPhraseCount
    {
        public ObjectId _id { get; set; }

    }
    class AzureKeyPhraseCount : EntityAdapter<POCO.RecordAssociationKeyPhraseCount>
    {
        public AzureKeyPhraseCount() { }
        public AzureKeyPhraseCount(POCO.RecordAssociationKeyPhraseCount o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoNamedEntityCount : POCO.RecordAssociationNamedEntityCount
    {
        public ObjectId _id { get; set; }

    }
    class AzureNamedEntityCount : EntityAdapter<POCO.RecordAssociationNamedEntityCount>
    {
        public AzureNamedEntityCount() { }
        public AzureNamedEntityCount(POCO.RecordAssociationNamedEntityCount o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoKeyPhrase : POCO.RecordAssociationKeyPhrase
    {
        public ObjectId _id { get; set; }

    }
    class AzureKeyPhrase : EntityAdapter<POCO.RecordAssociationKeyPhrase>
    {
        public AzureKeyPhrase() { }
        public AzureKeyPhrase(POCO.RecordAssociationKeyPhrase o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureOntologyTermMatch : EntityAdapter<POCO.OntologyTermMatch>
    {
        public AzureOntologyTermMatch() { }
        public AzureOntologyTermMatch(POCO.OntologyTermMatch o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureOntologyTermMatchReverse : EntityAdapter<POCO.OntologyTermMatchReverse>
    {
        public AzureOntologyTermMatchReverse() { }
        public AzureOntologyTermMatchReverse(POCO.OntologyTermMatchReverse o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureOntologyMatchStatus : EntityAdapter<POCO.OntologyMatchStatus>
    {
        public AzureOntologyMatchStatus() { }
        public AzureOntologyMatchStatus(POCO.OntologyMatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoOntologyMatchStatus : POCO.OntologyMatchStatus
    {
        public ObjectId _id { get; set; }
    }

    class MongoOntologyTermMatch : POCO.OntologyTermMatch
    {
        public ObjectId _id { get; set; }

    }
    class MongoOntologyTermMatchReverse : POCO.OntologyTermMatchReverse
    {
        public ObjectId _id { get; set; }

    }
    class MongoKeyPhraseReverse : POCO.RecordAssociationKeyPhraseReverse
    {
        public ObjectId _id { get; set; }

    }
    class AzureFileMetadata : EntityAdapter<POCO.RecordAssociationFileMetadata>
    {
        public AzureFileMetadata() { }
        public AzureFileMetadata(POCO.RecordAssociationFileMetadata o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoFileMetadata : POCO.RecordAssociationFileMetadata
    {
        public ObjectId _id { get; set; }

    }

    class AzureKeyPhraseReverse : EntityAdapter<POCO.RecordAssociationKeyPhraseReverse>
    {
        public AzureKeyPhraseReverse() { }
        public AzureKeyPhraseReverse(POCO.RecordAssociationKeyPhraseReverse o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoSubjectObject : POCO.RecordAssociationSubjectObject
    {
        public ObjectId _id { get; set; }

    }
    class AzureSubjectObject : EntityAdapter<POCO.RecordAssociationSubjectObject>
    {
        public AzureSubjectObject() { }
        public AzureSubjectObject(POCO.RecordAssociationSubjectObject o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureNamedEntity : EntityAdapter<POCO.RecordAssociationNamedEntity>
    {
        public AzureNamedEntity() { }
        public AzureNamedEntity(POCO.RecordAssociationNamedEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureNamedEntityReverse : EntityAdapter<POCO.RecordAssociationNamedEntityReverse>
    {
        public AzureNamedEntityReverse() { }
        public AzureNamedEntityReverse(POCO.RecordAssociationNamedEntityReverse o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoNamedEntity : POCO.RecordAssociationNamedEntity
    {
        public ObjectId _id { get; set; }

    }
    class MongoNamedEntityReverse : POCO.RecordAssociationNamedEntityReverse
    {
        public ObjectId _id { get; set; }

    }

    class MongoRecordAssociationKeyPhrase : POCO.RecordAssociationKeyPhrase
    {
        public ObjectId _id { get; set; }
    }

    class MongoRecordAssociationNamedEntity : POCO.RecordAssociationNamedEntity
    {
        public ObjectId _id { get; set; }
    }

    class AzureRecordAssociationKeyPhrase : EntityAdapter<POCO.RecordAssociationKeyPhrase>
    {
        public AzureRecordAssociationKeyPhrase() { }
        public AzureRecordAssociationKeyPhrase(POCO.RecordAssociationKeyPhrase o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureRecordAssociationNamedEntity : EntityAdapter<POCO.RecordAssociationNamedEntity>
    {
        public AzureRecordAssociationNamedEntity() { }
        public AzureRecordAssociationNamedEntity(POCO.RecordAssociationNamedEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoRecordAssociationNamedEntityReverse : POCO.RecordAssociationNamedEntityReverse
    {
        public ObjectId _id { get; set; }
    }

    class AzureRecordAssociationNamedEntityReverse : EntityAdapter<POCO.RecordAssociationNamedEntityReverse>
    {
        public AzureRecordAssociationNamedEntityReverse() { }
        public AzureRecordAssociationNamedEntityReverse(POCO.RecordAssociationNamedEntityReverse o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoRecordAssociation : POCO.RecordAssociation
    {

    }

    public static class RecordAssociation
    {
        internal static class AzureTableNames
        {
            internal const string RecordAssociationToRecord = "stlprecordassociationtorecord";
            internal const string RecordAssociationKeyphrases = "stlprecordassociationkeyphrases";
            internal const string RecordAssociationKeyphrasesFilename = "stlprecordassociationkeyphrasesfilename";
            internal const string RecordAssociationFileMetadata = "stlprecordassociationfilemetadata";
            internal const string RecordAssociationKeyphraseCount = "stlprecordassociationkeyphrasecount";
            internal const string RecordAssociationKeyphrasesReverse = "stlprecordassociationkeyphrasesreverse";
            internal const string RecordAssociationSubjectObject = "stlprecordassociationsubjectobject";
            internal const string RecordAssociationKeyphraseCountFilename = "stlprecordassociationkeyphrasecountfilename";
            internal const string RecordAssociationKeyphrasesReverseFilename = "stlprecordassociationkeyphrasesreversefilename";
            internal const string RecordAssociationSubjectObjectFilename = "stlprecordassociationsubjectobjectfilename";
            internal const string RecordAssociationNamedEntity = "stlprecordassociationnamedentity";
            internal const string RecordAssociationNamedEntityFilename = "stlprecordassociationnamedentityfilename";
            internal const string RecordAssociationNamedEntityReverse = "stlprecordassociationnamedentityreverse";
            internal const string RecordAssociationNamedEntityReverseFilename = "stlprecordassociationnamedentityreversefilename";
            internal const string RecordAssociationNamedEntityCount = "stlprecordassociationnamedentitycount";
            internal const string RecordAssociationNamedEntityCountFilename = "stlprecordassociationnamedentitycountfilename";
            internal const string RecordAssociationOntologyMatch = "stlprecordassociationontologymatch";
            internal const string RecordAssociationOntologyMatchReverse = "stlprecordassociationontologymatchreverse";
        }

        internal static class MongoTableNames
        {
            internal const string RecordAssociationToRecord = "recordassociationtorecord";
            internal const string RecordAssociationKeyphrases = "recordassociationkeyphrases";
            internal const string RecordAssociationKeyphrasesFilename = "recordassociationkeyphrasesfilename";
            internal const string RecordAssociationFileMetadata = "recordassociationfilemetadata";
            internal const string RecordAssociationKeyphraseCount = "recordassociationkeyphrasecount";
            internal const string RecordAssociationKeyphrasesReverse = "recordassociationkeyphrasesreverse";
            internal const string RecordAssociationSubjectObjectFilename = "recordassociationsubjectobject";
            internal const string RecordAssociationKeyphraseCountFilename = "recordassociationkeyphrasecountfilename";
            internal const string RecordAssociationKeyphrasesReverseFilename = "recordassociationkeyphrasesreversefilename";
            internal const string RecordAssociationSubjectObject = "recordassociationsubjectobjectfilename";
            internal const string RecordAssociationNamedEntity = "recordassociationnamedentity";
            internal const string RecordAssociationNamedEntityFilename = "recordassociationnamedentityfilename";
            internal const string RecordAssociationNamedEntityReverse = "recordassociationnamedentityreverse";
            internal const string RecordAssociationNamedEntityReverseFilename = "recordassociationnamedentityreversefilename";
            internal const string RecordAssociationNamedEntityCount = "recordassociationnamedentitycount";
            internal const string RecordAssociationNamedEntityCountFilename = "recordassociationnamedentitycountfilename";
            internal const string RecordAssociationOntologyMatch = "recordassociationontologymatch";
            internal const string RecordAssociationOntologyMatchReverse = "recordassociationontologymatchreverse";
        }

        public static List<POCO.RecordAssociationKeyPhrase> GetKeyPhrases(DataConfig providerConfig, POCO.RecordToRecordAssociation recassoc)
        {
            // Create a filter for the record association primary key
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", recassoc.RowKey, "eq");
            filters.Add(pkfilt);

            return GetKeyPhrases(providerConfig, filters);
        }

        public static List<POCO.RecordAssociationKeyPhrase> GetKeyPhrases(DataConfig providerConfig, POCO.RecordAssociation recassoc)
        {
            // Create a filter for the record association primary key
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", recassoc.RowKey, "eq");
            filters.Add(pkfilt);

            return GetKeyPhrases(providerConfig, filters);
        }

        public static List<POCO.RecordAssociationKeyPhrase> GetKeyPhrases(DataConfig providerConfig, List<Filter> filters)
        {
            return GetKeyPhrases(providerConfig, filters, "body");
        }

        public static List<POCO.RecordAssociationFileMetadata> GetFileMetadata(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;

            List<POCO.RecordAssociationFileMetadata> filemeta = new List<POCO.RecordAssociationFileMetadata>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":


                    string combinedFilter = Utils.GenerateAzureFilter(filters);
                    TableContinuationToken thisPageToken = null;
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        thisPageToken = Newtonsoft.Json.JsonConvert.DeserializeObject<TableContinuationToken>(thisPageId);
                    }
                    TableContinuationToken nextPageToken = null;
                    List<AzureFileMetadata> azdata = new List<AzureFileMetadata>();
                    AzureTableAdaptor<AzureFileMetadata> adaptor = new AzureTableAdaptor<AzureFileMetadata>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, AzureTableNames.RecordAssociationFileMetadata, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

                    foreach (var doc in azdata)
                    {
                        filemeta.Add(doc.Value);
                    }

                    // Check if there is a next page token available
                    if (nextPageToken != null)
                    {
                        nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                    }

                    break;





                    //string combinedFilter = Utils.GenerateAzureFilter(filters);

                    //List<AzureFileMetadata> azdata = new List<AzureFileMetadata>();
                    //AzureTableAdaptor<AzureFileMetadata> adaptor = new AzureTableAdaptor<AzureFileMetadata>();
                    //azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAssociationFileMetadata, combinedFilter);

                    //foreach (var doc in azdata)
                    //{
                    //    filemeta.Add(doc.Value);
                    //}

                    break;
                case "internal.mongodb":

                    var collection = Utils.GetMongoCollection<MongoFileMetadata>(providerConfig, MongoTableNames.RecordAssociationFileMetadata);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoFileMetadata> filter = Utils.GenerateMongoFilter<MongoFileMetadata>(filters);

                    //DEBUG output the filter values
                    //foreach (Castlepoint.DataFactory.Filter debugFilter in filters)
                    //{
                    //    // Output the filter field names and values
                    //    Console.WriteLine("DEBUG filter: " + debugFilter.FieldName + " : " + debugFilter.FieldValue);
                    //}
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();


                    foreach (var doc in documents)
                    {
                        filemeta.Add(doc);
                    }

                    // Get the next page id
                    if (documents.Count == rowLimit)
                    {
                        // Set the next page id
                        nextPageId = documents[documents.Count - 1]._id.ToString();
                    }





                    //var collection = Utils.GetMongoCollection<MongoFileMetadata>(providerConfig, MongoTableNames.RecordAssociationFileMetadata);

                    //FilterDefinition<MongoFileMetadata> filter = Utils.GenerateMongoFilter<MongoFileMetadata>(filters);

                    //var documents = collection.Find(filter).ToList();

                    //foreach (var keyphrase in documents)
                    //{
                    //    filemeta.Add(keyphrase);
                    //}

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return filemeta;
        }

        public static List<POCO.RecordAssociationKeyPhrase> GetKeyPhrases(DataConfig providerConfig, List<Filter> filters, string keyPhraseLocation)
        {
            List<POCO.RecordAssociationKeyPhrase> keyphrases = new List<POCO.RecordAssociationKeyPhrase>();

            string tableName = string.Empty;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphrasesFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphrases;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationKeyPhrase> azdata = new List<AzureRecordAssociationKeyPhrase>();
                    AzureTableAdaptor<AzureRecordAssociationKeyPhrase> adaptor = new AzureTableAdaptor<AzureRecordAssociationKeyPhrase>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyphrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphrasesFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphrases;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }


                    var collection = Utils.GetMongoCollection<MongoRecordAssociationKeyPhrase>(providerConfig, tableName);

                    FilterDefinition<MongoRecordAssociationKeyPhrase> filter = Utils.GenerateMongoFilter<MongoRecordAssociationKeyPhrase>(filters);

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

        public static List<POCO.RecordAssociationNamedEntity> GetNamedEntities(DataConfig providerConfig, POCO.RecordToRecordAssociation recassoc)
        {
            // Create a filter for the record association primary key
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", recassoc.RowKey, "eq");
            filters.Add(pkfilt);

            return GetNamedEntities(providerConfig, filters);
        }

        public static List<POCO.RecordAssociationNamedEntity> GetNamedEntities(DataConfig providerConfig, POCO.RecordAssociation recassoc)
        {
            // Create a filter for the record association primary key
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", recassoc.RowKey, "eq");
            filters.Add(pkfilt);

            return GetNamedEntities(providerConfig, filters);
        }

        public static List<POCO.RecordAssociationNamedEntity> GetNamedEntities(DataConfig providerConfig, List<Filter> filters)
        {
            return GetNamedEntities(providerConfig, filters, "body");
        }

        public static List<POCO.RecordAssociationNamedEntity> GetNamedEntities(DataConfig providerConfig, List<Filter> filters, string keyPhraseLocation)
        {
            List<POCO.RecordAssociationNamedEntity> keyphrases = new List<POCO.RecordAssociationNamedEntity>();

            string tableName = string.Empty;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntityFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntity;
                                break;
                            }
                        default:
                            throw new ApplicationException("Named entity location not recognised: " + keyPhraseLocation);
                    }

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationNamedEntity> azdata = new List<AzureRecordAssociationNamedEntity>();
                    AzureTableAdaptor<AzureRecordAssociationNamedEntity> adaptor = new AzureTableAdaptor<AzureRecordAssociationNamedEntity>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyphrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntityFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntity;
                                break;
                            }
                        default:
                            throw new ApplicationException("Named entity location not recognised: " + keyPhraseLocation);
                    }


                    var collection = Utils.GetMongoCollection<MongoRecordAssociationNamedEntity>(providerConfig, tableName);

                    FilterDefinition<MongoRecordAssociationNamedEntity> filter = Utils.GenerateMongoFilter<MongoRecordAssociationNamedEntity>(filters);

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
        public static string AddOntologyMatchReverse(DataConfig providerConfig, List<POCO.OntologyTermMatchReverse> reverseTerms)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationOntologyMatchReverse);
                    foreach (POCO.OntologyTermMatchReverse ne in reverseTerms)
                    {
                        AzureOntologyTermMatchReverse az = new AzureOntologyTermMatchReverse(ne);
                        TableOperation operation = TableOperation.InsertOrReplace(az);

                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoOntologyTermMatchReverse> collection = Utils.GetMongoCollection<MongoOntologyTermMatchReverse>(providerConfig, MongoTableNames.RecordAssociationOntologyMatchReverse);

                    var operationList = new List<WriteModel<MongoOntologyTermMatchReverse>>();
                    foreach (POCO.OntologyTermMatchReverse ne in reverseTerms)
                    {
                        // Convert to mongo-compatible object
                        MongoOntologyTermMatchReverse mongoObject = Utils.ConvertType<MongoOntologyTermMatchReverse>(ne);

                        // Create the filter for the upsert
                        FilterDefinition<MongoOntologyTermMatchReverse> filter = Builders<MongoOntologyTermMatchReverse>.Filter.Eq(x => x.PartitionKey, ne.PartitionKey) &
                            Builders<MongoOntologyTermMatchReverse>.Filter.Eq(x => x.RowKey, ne.RowKey);

                        UpdateDefinition<MongoOntologyTermMatchReverse> updateDefinition = new UpdateDefinitionBuilder<MongoOntologyTermMatchReverse>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition
                            .SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("MatchSource", mongoObject.MatchSource)
                            .Set("MatchType", mongoObject.MatchType)
                            .Set("ParentTerm", mongoObject.ParentTerm)
                            .Set("OntologyUri", mongoObject.OntologyUri)
                            .Set("TermRowKey", mongoObject.TermRowKey)
                            .Set("RecordAssociation", mongoObject.RecordAssociation)
                            .Set("Term", mongoObject.Term)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoOntologyTermMatchReverse> update = new UpdateOneModel<MongoOntologyTermMatchReverse>(filter, updateDefinition) { IsUpsert = true };
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

        public static string AddOntologyMatch(DataConfig dataConfig, List<OntologyTermMatch> termMatchesForRecAssoc)
        {
            // Check if there are any keyphrases to add
            if (termMatchesForRecAssoc.Count == 0)
            {
                return string.Empty;
            }

            string tableName = "";
            switch (dataConfig.ProviderType)
            {
                case "azure.tableservice":
                    tableName = AzureTableNames.RecordAssociationOntologyMatch;

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(dataConfig, tableName);
                    foreach (POCO.OntologyTermMatch termmatch in termMatchesForRecAssoc)
                    {
                        AzureOntologyTermMatch az = new AzureOntologyTermMatch(termmatch);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);

                        //Task tUpdate = table.ExecuteAsync(operation);
                        //tUpdate.Wait();
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":

                    tableName = MongoTableNames.RecordAssociationOntologyMatch;

                    IMongoCollection<MongoOntologyTermMatch> collection = Utils.GetMongoCollection<MongoOntologyTermMatch>(dataConfig, tableName);

                    var operationList = new List<WriteModel<MongoOntologyTermMatch>>();
                    foreach (POCO.OntologyTermMatch termmatch in termMatchesForRecAssoc)
                    {
                        // Convert to mongo-compatible object
                        MongoOntologyTermMatch mongoObject = Utils.ConvertType<MongoOntologyTermMatch>(termmatch);

                        // Create the filter for the upsert
                        FilterDefinition<MongoOntologyTermMatch> filter = Builders<MongoOntologyTermMatch>.Filter.Eq(x => x.PartitionKey, termmatch.PartitionKey) &
                            Builders<MongoOntologyTermMatch>.Filter.Eq(x => x.RowKey, termmatch.RowKey);

                        UpdateDefinition<MongoOntologyTermMatch> updateDefinition = new UpdateDefinitionBuilder<MongoOntologyTermMatch>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("MatchSource", mongoObject.MatchSource)
                            .Set("MatchType", mongoObject.MatchType)
                            .Set("ParentTerm", mongoObject.ParentTerm)
                            .Set("OntologyUri", mongoObject.OntologyUri)
                            .Set("TermRowKey", mongoObject.TermRowKey)
                            .Set("RecordAssociation", mongoObject.RecordAssociation)
                            .Set("Term", mongoObject.Term)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoOntologyTermMatch> update = new UpdateOneModel<MongoOntologyTermMatch>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);

                    }

                    if (operationList.Count > 0)
                    {
                        collection.BulkWrite(operationList);
                    }

                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + dataConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.RecordAssociationKeyPhraseCount> GetKeyPhraseCounts(DataConfig providerConfig, List<Filter> filters, string keyPhraseLocation)
        {
            List<POCO.RecordAssociationKeyPhraseCount> keyphrases = new List<POCO.RecordAssociationKeyPhraseCount>();

            string tableName = string.Empty;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphraseCountFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphraseCount;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureKeyPhraseCount> azdata = new List<AzureKeyPhraseCount>();
                    AzureTableAdaptor<AzureKeyPhraseCount> adaptor = new AzureTableAdaptor<AzureKeyPhraseCount>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyphrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphraseCountFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphraseCount;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }


                    var collection = Utils.GetMongoCollection<MongoKeyPhraseCount>(providerConfig, tableName);

                    FilterDefinition<MongoKeyPhraseCount> filter = Utils.GenerateMongoFilter<MongoKeyPhraseCount>(filters);

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

        public static string AddKeyPhraseCount(DataConfig providerConfig, List<POCO.RecordAssociationKeyPhraseCount> keyPhraseCounts, string keyPhraseLocation)
        {
            string tableName = "";
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphraseCountFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphraseCount;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    foreach (POCO.RecordAssociationKeyPhraseCount kp in keyPhraseCounts)
                    {
                        AzureKeyPhraseCount az = new AzureKeyPhraseCount(kp);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphraseCountFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphraseCount;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }
                    IMongoCollection<MongoKeyPhraseCount> collection = Utils.GetMongoCollection<MongoKeyPhraseCount>(providerConfig, tableName);

                    var operationList = new List<WriteModel<MongoKeyPhraseCount>>();
                    foreach (POCO.RecordAssociationKeyPhraseCount kp in keyPhraseCounts)
                    {
                        // Convert to mongo-compatible object
                        MongoKeyPhraseCount mongoObject = Utils.ConvertType<MongoKeyPhraseCount>(kp);

                        // Create the filter for the upsert
                        FilterDefinition<MongoKeyPhraseCount> filter = Builders<MongoKeyPhraseCount>.Filter.Eq(x => x.PartitionKey, kp.PartitionKey) &
                            Builders<MongoKeyPhraseCount>.Filter.Eq(x => x.RowKey, kp.RowKey);

                        UpdateDefinition<MongoKeyPhraseCount> updateDefinition = new UpdateDefinitionBuilder<MongoKeyPhraseCount>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("KeyPhraseCount", mongoObject.KeyPhraseCount)
                            .Set("KeyPhraseLocation", mongoObject.KeyPhraseLocation)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);


                        UpdateOneModel<MongoKeyPhraseCount> update = new UpdateOneModel<MongoKeyPhraseCount>(filter, updateDefinition) { IsUpsert = true };
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

        public static string AddKeyPhrase(DataConfig providerConfig, List<POCO.RecordAssociationKeyPhrase> keyPhrases, string keyPhraseLocation)
        {
            // Check if there are any keyphrases to add
            if (keyPhrases.Count == 0)
            {
                return string.Empty;
            }

            string tableName = "";
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphrasesFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphrases;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    foreach (POCO.RecordAssociationKeyPhrase kp in keyPhrases)
                    {
                        AzureKeyPhrase az = new AzureKeyPhrase(kp);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);

                        //Task tUpdate = table.ExecuteAsync(operation);
                        //tUpdate.Wait();
                    }

                    Utils.AzureBatchExecute(table, ops);


                    break;

                case "internal.mongodb":

                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphrasesFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphrases;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }

                    IMongoCollection<MongoKeyPhrase> collection = Utils.GetMongoCollection<MongoKeyPhrase>(providerConfig, tableName);

                    var operationList = new List<WriteModel<MongoKeyPhrase>>();
                    foreach (POCO.RecordAssociationKeyPhrase kp in keyPhrases)
                    {
                        // Convert to mongo-compatible object
                        MongoKeyPhrase mongoObject = Utils.ConvertType<MongoKeyPhrase>(kp);

                        // Create the filter for the upsert
                        FilterDefinition<MongoKeyPhrase> filter = Builders<MongoKeyPhrase>.Filter.Eq(x => x.PartitionKey, kp.PartitionKey) &
                            Builders<MongoKeyPhrase>.Filter.Eq(x => x.RowKey, kp.RowKey);

                        UpdateDefinition<MongoKeyPhrase> updateDefinition = new UpdateDefinitionBuilder<MongoKeyPhrase>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("KeyPhraseLocation", mongoObject.KeyPhraseLocation)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoKeyPhrase> update = new UpdateOneModel<MongoKeyPhrase>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);

                    }

                    string bulkResultErrors = string.Empty;
                    if (operationList.Count > 0)
                    {
                        BulkWriteResult<MongoKeyPhrase> bulkResult = null;
                        try

                        {
                            bulkResult = collection.BulkWrite(operationList);
                        }
                        catch (Exception ex)
                        {
                            bulkResultErrors += "Bulk write error (" + ex.Message + ")";
                            if (ex.InnerException != null)
                            {
                                bulkResultErrors += " Inner: " + ex.InnerException.ToString();
                            }
                        }
                    }

                    return bulkResultErrors;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddKeyPhraseReverse(DataConfig providerConfig, List<POCO.RecordAssociationKeyPhraseReverse> keyPhrases, string keyPhraseLocation)
        {
            string tableName = "";
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphrasesReverseFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationKeyphrasesReverse;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }
                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);

                    keyPhrases.ParallelForEachAsync(
                    async kp =>
                    {
                        AzureKeyPhraseReverse az = new AzureKeyPhraseReverse(kp);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        TableResult result = await table.ExecuteAsync(operation);
                    }, 6);

                    //foreach (POCO.RecordAssociationKeyPhraseReverse kp in keyPhrases)
                    //{
                    //    AzureKeyPhraseReverse az = new AzureKeyPhraseReverse(kp);
                    //    TableOperation operation = TableOperation.InsertOrReplace(az);

                    //    Task tUpdate = table.ExecuteAsync(operation);
                    //    tUpdate.Wait();
                    //}


                    break;

                case "internal.mongodb":

                    switch (keyPhraseLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphrasesReverseFilename;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationKeyphrasesReverse;
                                break;
                            }
                        default:
                            throw new ApplicationException("Key phrase location not recognised: " + keyPhraseLocation);
                    }

                    IMongoCollection<MongoKeyPhraseReverse> collection = Utils.GetMongoCollection<MongoKeyPhraseReverse>(providerConfig, tableName);

                    var operationList = new List<WriteModel<MongoKeyPhraseReverse>>();
                    foreach (POCO.RecordAssociationKeyPhraseReverse kp in keyPhrases)
                    {
                        // Convert to mongo-compatible object
                        MongoKeyPhraseReverse mongoObject = Utils.ConvertType<MongoKeyPhraseReverse>(kp);

                        // Create the filter for the upsert
                        FilterDefinition<MongoKeyPhraseReverse> filter = Builders<MongoKeyPhraseReverse>.Filter.Eq(x => x.PartitionKey, kp.PartitionKey) &
                            Builders<MongoKeyPhraseReverse>.Filter.Eq(x => x.RowKey, kp.RowKey);

                        UpdateDefinition<MongoKeyPhraseReverse> updateDefinition = new UpdateDefinitionBuilder<MongoKeyPhraseReverse>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition
                            .SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("KeyPhraseLocation", mongoObject.KeyPhraseLocation)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoKeyPhraseReverse> update = new UpdateOneModel<MongoKeyPhraseReverse>(filter, updateDefinition) { IsUpsert = true };
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

        public static List<POCO.RecordAssociationKeyPhraseReverse> GetByReverseKeyPhrases(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            List<POCO.RecordAssociationKeyPhraseReverse> keyphrases = new List<POCO.RecordAssociationKeyPhraseReverse>();

            // Default next page id to "no more data" (empty string)
            nextPageId = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string azureFilter = Utils.GenerateAzureOrFilter(filters);

                    TableContinuationToken thisPageToken = null;
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        thisPageToken = Newtonsoft.Json.JsonConvert.DeserializeObject<TableContinuationToken>(thisPageId);
                    }
                    TableContinuationToken nextPageToken = null;

                    List<AzureRecordAssociationKeyPhraseReverse> azdata = new List<AzureRecordAssociationKeyPhraseReverse>();
                    AzureTableAdaptor<AzureRecordAssociationKeyPhraseReverse> adaptor = new AzureTableAdaptor<AzureRecordAssociationKeyPhraseReverse>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, Record.AzureTableNames.RecordAssociationKeyPhraseReverse, azureFilter, rowLimit, thisPageToken, out nextPageToken);

                    foreach (var keyphrase in azdata)
                    {
                        keyphrases.Add(keyphrase.Value);
                    }

                    // Check if there is a next page token available
                    if (nextPageToken != null)
                    {
                        nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoKeyPhraseReverse>(providerConfig, Record.MongoTableNames.RecordAssociationKeyPhraseReverse);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoKeyPhraseReverse> mongoFilter = Utils.GenerateOrMongoFilter<MongoKeyPhraseReverse>(filters);

                    var documents = collection.Find(mongoFilter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();
                    //var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyphrases.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Now get the record association details from the key phrase we matched
            //List<POCO.RecordToRecordAssociationWithMatchInfo> recordAssocs = new List<POCO.RecordToRecordAssociationWithMatchInfo>();
            //foreach(POCO.RecordAssociationKeyPhraseReverse kpreverse in keyphrases)
            //{
            //    // Create a filter to look up the record association
            //    List<DataFactory.Filter> recordassociationFilters = new List<Filter>();
            //    DataFactory.Filter f = new Filter("RowKey", kpreverse.RowKey, "eq");
            //    recordassociationFilters.Add(f);

            //    List<POCO.RecordToRecordAssociation> listr = new List<POCO.RecordToRecordAssociation>();
            //    listr = Record.GetRecordToRecordsAssociations(providerConfig, recordassociationFilters);
            //    foreach(POCO.RecordToRecordAssociation r in listr)
            //    {
            //        string j = JsonConvert.SerializeObject(r);
            //        POCO.RecordToRecordAssociationWithMatchInfo rmi = JsonConvert.DeserializeObject<POCO.RecordToRecordAssociationWithMatchInfo>(j);
            //        rmi.KeyPhrase = kpreverse.PartitionKey;
            //        // Add any data to the returning list
            //        recordAssocs.Add(rmi);
            //    }
            //}

            return keyphrases;
        }

        public static List<POCO.RecordAssociationNamedEntityReverse> GetByReverseNamedEntity(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationNamedEntityReverse> namedentities = new List<POCO.RecordAssociationNamedEntityReverse>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string azureFilter = Utils.GenerateAzureOrFilter(filters);

                    List<AzureRecordAssociationNamedEntityReverse> azdata = new List<AzureRecordAssociationNamedEntityReverse>();
                    AzureTableAdaptor<AzureRecordAssociationNamedEntityReverse> adaptor = new AzureTableAdaptor<AzureRecordAssociationNamedEntityReverse>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAssociationNamedEntityReverse, azureFilter);

                    foreach (var o in azdata)
                    {
                        namedentities.Add(o.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAssociationNamedEntityReverse>(providerConfig, MongoTableNames.RecordAssociationNamedEntityReverse);

                    FilterDefinition<MongoRecordAssociationNamedEntityReverse> mongoFilter = Utils.GenerateOrMongoFilter<MongoRecordAssociationNamedEntityReverse>(filters);

                    var documents = collection.Find(mongoFilter).ToList();

                    foreach (var o in documents)
                    {
                        namedentities.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //// Now get the record association details from the key phrase we matched
            //List<POCO.RecordToRecordAssociationWithMatchInfo> recordAssocs = new List<POCO.RecordToRecordAssociationWithMatchInfo>();
            //foreach (POCO.RecordAssociationNamedEntityReverse kpreverse in keyphrases)
            //{
            //    // Create a filter to look up the record association
            //    List<DataFactory.Filter> recordassociationFilters = new List<Filter>();
            //    DataFactory.Filter f = new Filter("RowKey", kpreverse.RowKey, "eq");
            //    recordassociationFilters.Add(f);

            //    List<POCO.RecordToRecordAssociation> listr = new List<POCO.RecordToRecordAssociation>();
            //    listr = Record.GetRecordToRecordsAssociations(providerConfig, recordassociationFilters);
            //    foreach (POCO.RecordToRecordAssociation r in listr)
            //    {
            //        string j = JsonConvert.SerializeObject(r);
            //        POCO.RecordToRecordAssociationWithMatchInfo rmi = JsonConvert.DeserializeObject<POCO.RecordToRecordAssociationWithMatchInfo>(j);
            //        rmi.KeyPhrase = kpreverse.PartitionKey;
            //        // Add any data to the returning list
            //        recordAssocs.Add(rmi);
            //    }
            //}

            return namedentities;
        }

        public static List<POCO.RecordToRecordAssociationWithMatchInfo> GetByReverseOntologyTerm(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.OntologyTermMatchReverse> ontologyterms = new List<POCO.OntologyTermMatchReverse>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyTermMatchReverse> azdata = new List<AzureOntologyTermMatchReverse>();
                    AzureTableAdaptor<AzureOntologyTermMatchReverse> adaptor = new AzureTableAdaptor<AzureOntologyTermMatchReverse>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAssociationOntologyMatchReverse, combinedFilter);

                    foreach (var o in azdata)
                    {
                        ontologyterms.Add(o.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyTermMatchReverse>(providerConfig, MongoTableNames.RecordAssociationOntologyMatchReverse);

                    FilterDefinition<MongoOntologyTermMatchReverse> filter = Utils.GenerateMongoFilter<MongoOntologyTermMatchReverse>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var o in documents)
                    {
                        ontologyterms.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Now get the record association details from the key phrase we matched
            List<POCO.RecordToRecordAssociationWithMatchInfo> recordAssocs = new List<POCO.RecordToRecordAssociationWithMatchInfo>();
            foreach (POCO.OntologyTermMatchReverse term in ontologyterms)
            {
                // Create a filter to look up the record association
                List<DataFactory.Filter> recordassociationFilters = new List<Filter>();
                DataFactory.Filter f = new Filter("RowKey", term.RowKey, "eq");
                recordassociationFilters.Add(f);

                List<POCO.RecordToRecordAssociation> listr = new List<POCO.RecordToRecordAssociation>();
                listr = Record.GetRecordToRecordsAssociations(providerConfig, recordassociationFilters);
                foreach (POCO.RecordToRecordAssociation r in listr)
                {
                    string j = JsonConvert.SerializeObject(r);
                    POCO.RecordToRecordAssociationWithMatchInfo rmi = JsonConvert.DeserializeObject<POCO.RecordToRecordAssociationWithMatchInfo>(j);
                    rmi.OntologyTerm = term.Term;
                    // Add any data to the returning list
                    recordAssocs.Add(rmi);
                }
            }

            return recordAssocs;
        }

        public static List<POCO.RecordAssociationToRecord> GetRecord(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationToRecord> recordassoctorecord = new List<POCO.RecordAssociationToRecord>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationToRecord> azdata = new List<AzureRecordAssociationToRecord>();
                    AzureTableAdaptor<AzureRecordAssociationToRecord> adaptor = new AzureTableAdaptor<AzureRecordAssociationToRecord>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAssociationToRecord, combinedFilter);

                    foreach (var o in azdata)
                    {
                        recordassoctorecord.Add(o.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAssociationToRecord>(providerConfig, MongoTableNames.RecordAssociationToRecord);

                    FilterDefinition<MongoRecordAssociationToRecord> filter = Utils.GenerateMongoFilter<MongoRecordAssociationToRecord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var o in documents)
                    {
                        recordassoctorecord.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return recordassoctorecord;
        }

        public static List<POCO.OntologyTermMatch> GetByOntologyTerm(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.OntologyTermMatch> ontologyterms = new List<POCO.OntologyTermMatch>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyTermMatch> azdata = new List<AzureOntologyTermMatch>();
                    AzureTableAdaptor<AzureOntologyTermMatch> adaptor = new AzureTableAdaptor<AzureOntologyTermMatch>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAssociationOntologyMatch, combinedFilter);

                    foreach (var o in azdata)
                    {
                        ontologyterms.Add(o.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyTermMatch>(providerConfig, MongoTableNames.RecordAssociationOntologyMatch);

                    FilterDefinition<MongoOntologyTermMatch> filter = Utils.GenerateMongoFilter<MongoOntologyTermMatch>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var o in documents)
                    {
                        ontologyterms.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //// Now get the record association details from the key phrase we matched
            //List<POCO.RecordToRecordAssociationWithMatchInfo> recordAssocs = new List<POCO.RecordToRecordAssociationWithMatchInfo>();
            //foreach (POCO.OntologyTermMatch term in ontologyterms)
            //{
            //    // Create a filter to look up the record association
            //    List<DataFactory.Filter> recordassociationFilters = new List<Filter>();
            //    DataFactory.Filter f = new Filter("RowKey", term.RecordAssociation, "eq");
            //    recordassociationFilters.Add(f);

            //    List<POCO.RecordToRecordAssociation> listr = new List<POCO.RecordToRecordAssociation>();
            //    listr = Record.GetRecordToRecordsAssociations(providerConfig, recordassociationFilters);
            //    foreach (POCO.RecordToRecordAssociation r in listr)
            //    {
            //        string j = JsonConvert.SerializeObject(r);
            //        POCO.RecordToRecordAssociationWithMatchInfo rmi = JsonConvert.DeserializeObject<POCO.RecordToRecordAssociationWithMatchInfo>(j);
            //        rmi.OntologyTerm = term.Term;
            //        // Add any data to the returning list
            //        recordAssocs.Add(rmi);
            //    }
            //}

            return ontologyterms;
        }

        public static string AddSubjectObject(DataConfig providerConfig, List<POCO.RecordAssociationSubjectObject> subjectObjects)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<TableOperation> ops = new List<TableOperation>();
                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationSubjectObject);
                    foreach (POCO.RecordAssociationSubjectObject kp in subjectObjects)
                    {
                        AzureSubjectObject az = new AzureSubjectObject(kp);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);

                        //Task tUpdate = table.ExecuteAsync(operation);
                        //tUpdate.Wait();
                    }

                    Utils.AzureBatchExecute(table, ops);

                    //CloudTable table = Utils.GetCloudTable(cpConfig, "stlpRecordAssociationKeyPhrase", _logger);

                    //int batchCount = 0;
                    //var batchOp = new TableBatchOperation();
                    //List<string> rowKeys = new List<string>();

                    //foreach (string key in keyPhraseCount.Keys)
                    //{

                    //    string cleanRowKey = Utils.CleanTableKey(key);

                    //    // Check if we need to run the batch
                    //    if (batchCount >= 100)
                    //    {

                    //        try
                    //        {
                    //            _logger.LogDebug("Batch count: " + batchCount.ToString() + "," + batchOp.Count.ToString());
                    //            // Reset tracking variables
                    //            batchCount = 0;
                    //            rowKeys = new List<string>();

                    //            Task tBatch = table.ExecuteBatchAsync(batchOp);
                    //            tBatch.Wait();

                    //            // Reset the batch operation
                    //            batchOp = new TableBatchOperation();
                    //        }
                    //        catch (StorageException ex)
                    //        {
                    //            var requestInformation = ex.RequestInformation;
                    //            _logger.LogWarning("ERR http status msg: " + requestInformation.HttpStatusMessage);

                    //            // Reset the batch operation
                    //            batchOp = new TableBatchOperation();
                    //            batchCount = 0;
                    //            rowKeys = new List<string>();
                    //        }

                    //    }
                    //    if (key != null && key != "")
                    //    {
                    //        if (!rowKeys.Contains(cleanRowKey))
                    //        {
                    //            batchCount++;

                    //            // Add the keyphrase to the tracking rowkeys list
                    //            rowKeys.Add(cleanRowKey);

                    //            // Save the keyphrase, replacing any existing one    
                    //            RecordAssociationKeyPhrase kpCountEntity = new RecordAssociationKeyPhrase(cleanPartitionKey, cleanRowKey);
                    //            kpCountEntity.KeyPhraseCount = keyPhraseCount[key];
                    //            TableOperation insertOperation = TableOperation.InsertOrReplace(kpCountEntity);
                    //            batchOp.Add(insertOperation);
                    //        }

                    //    }



                    //}


                    //// Check if any batch operations that haven't been run
                    //if (batchOp.Count > 0)
                    //{
                    //    try
                    //    {
                    //        _logger.LogDebug("Batch remainder: " + batchOp.Count.ToString());
                    //        Task tBatch = table.ExecuteBatchAsync(batchOp);
                    //        tBatch.Wait();
                    //    }
                    //    catch (StorageException ex)
                    //    {
                    //        var requestInformation = ex.RequestInformation;
                    //        _logger.LogWarning("ERR http status msg: " + requestInformation.HttpStatusMessage);

                    //        // Reset the batch operation
                    //        batchOp = new TableBatchOperation();
                    //        batchCount = 0;
                    //        rowKeys = new List<string>();
                    //    }
                    //    catch (Exception ex1)
                    //    {
                    //        _logger.LogError("ERR http status msg: " + ex1.Message);
                    //    }

                    //}

                    break;

                case "internal.mongodb":
                    //foreach (POCO.RecordAssociationSubjectObject soCount in subjectObjects)
                    //{
                    //    IMongoCollection<MongoSubjectObject> collection = Utils.GetMongoCollection<MongoSubjectObject>(providerConfig, "recordassociationsubjectobject");
                    //    MongoSubjectObject mongoObject = Utils.ConvertType<MongoSubjectObject>(soCount);

                    //    // Create the upsert filter
                    //    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    //    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", soCount.PartitionKey, "eq");
                    //    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", soCount.RowKey, "eq");
                    //    filters.Add(pkFilter);
                    //    filters.Add(rkFilter);
                    //    FilterDefinition<MongoSubjectObject> filter = Utils.GenerateMongoFilter<MongoSubjectObject>(filters);

                    //    // Create the upsert options
                    //    MongoDB.Driver.UpdateOptions options = new UpdateOptions();
                    //    options.IsUpsert = true;

                    //    // Upsert
                    //    collection.ReplaceOne(filter, mongoObject, options);
                    //}



                    IMongoCollection<MongoSubjectObject> collection = Utils.GetMongoCollection<MongoSubjectObject>(providerConfig, MongoTableNames.RecordAssociationSubjectObject);

                    var operationList = new List<WriteModel<MongoSubjectObject>>();
                    foreach (POCO.RecordAssociationSubjectObject subjectobject in subjectObjects)
                    {
                        // Convert to mongo-compatible object
                        MongoSubjectObject mongoObject = Utils.ConvertType<MongoSubjectObject>(subjectobject);

                        // Create the filter for the upsert
                        FilterDefinition<MongoSubjectObject> filter = Builders<MongoSubjectObject>.Filter.Eq(x => x.PartitionKey, subjectobject.PartitionKey) &
                            Builders<MongoSubjectObject>.Filter.Eq(x => x.RowKey, subjectobject.RowKey);

                        UpdateDefinition<MongoSubjectObject> updateDefinition = new UpdateDefinitionBuilder<MongoSubjectObject>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("Object", mongoObject.Object)
                            .Set("Relationship", mongoObject.Relationship)
                            .Set("Subject", mongoObject.Subject);

                        UpdateOneModel<MongoSubjectObject> update = new UpdateOneModel<MongoSubjectObject>(filter, updateDefinition) { IsUpsert = true };
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

        public static string AddNamedEntityForBody(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntity> namedEntities)
        {
            return AddNamedEntity(providerConfig, namedEntities, "body");
        }
        public static string AddNamedEntityForFilename(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntity> namedEntities)
        {
            return AddNamedEntity(providerConfig, namedEntities, "filename");
        }

        public static string AddNamedEntity(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntity> namedEntities, string from)
        {
            // Check if there are any named entities to add
            if (namedEntities.Count == 0)
            {
                return string.Empty;
            }

            string tableName = "";
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    switch (from)
                    {
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntity;
                                break;
                            }
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntityFilename;
                                break;
                            }
                        default:
                            throw new ApplicationException("named entity source not found: " + from);
                    }

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    List<TableOperation> ops = new List<TableOperation>();
                    foreach (POCO.RecordAssociationNamedEntity ne in namedEntities)
                    {
                        AzureNamedEntity az = new AzureNamedEntity(ne);
                        TableOperation operation = TableOperation.InsertOrReplace(az);

                        ops.Add(operation);

                    }

                    Utils.AzureBatchExecute(table, ops);


                    break;

                case "internal.mongodb":

                    switch (from)
                    {
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntity;
                                break;
                            }
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntityFilename;
                                break;
                            }
                        default:
                            throw new ApplicationException("named entity source not found: " + from);
                    }
                    IMongoCollection<MongoNamedEntity> collection = Utils.GetMongoCollection<MongoNamedEntity>(providerConfig, tableName);

                    var operationList = new List<WriteModel<MongoNamedEntity>>();
                    foreach (POCO.RecordAssociationNamedEntity ne in namedEntities)
                    {
                        // Convert to mongo-compatible object
                        MongoNamedEntity mongoObject = Utils.ConvertType<MongoNamedEntity>(ne);

                        // Create the filter for the upsert
                        FilterDefinition<MongoNamedEntity> filter = Builders<MongoNamedEntity>.Filter.Eq(x => x.PartitionKey, ne.PartitionKey) &
                            Builders<MongoNamedEntity>.Filter.Eq(x => x.RowKey, ne.RowKey);

                        UpdateDefinition<MongoNamedEntity> updateDefinition = new UpdateDefinitionBuilder<MongoNamedEntity>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition
                            .SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("OriginalText", mongoObject.OriginalText)
                            .Set("Type", mongoObject.Type)
                            .Set("NamedEntityLocation", mongoObject.NamedEntityLocation)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoNamedEntity> update = new UpdateOneModel<MongoNamedEntity>(filter, updateDefinition) { IsUpsert = true };
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

        public static string AddNamedEntityReverseForBody(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntityReverse> namedEntities)
        {
            return AddNamedEntityReverse(providerConfig, namedEntities, "body");
        }
        public static string AddNamedEntityReverseForFilename(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntityReverse> namedEntities)
        {
            return AddNamedEntityReverse(providerConfig, namedEntities, "filename");
        }
        public static string AddNamedEntityReverse(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntityReverse> namedEntities, string from)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationNamedEntityReverse);
                    foreach (POCO.RecordAssociationNamedEntityReverse ne in namedEntities)
                    {
                        AzureNamedEntityReverse az = new AzureNamedEntityReverse(ne);
                        TableOperation operation = TableOperation.InsertOrReplace(az);

                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoNamedEntityReverse> collection = Utils.GetMongoCollection<MongoNamedEntityReverse>(providerConfig, MongoTableNames.RecordAssociationNamedEntityReverse);

                    var operationList = new List<WriteModel<MongoNamedEntityReverse>>();
                    foreach (POCO.RecordAssociationNamedEntityReverse ne in namedEntities)
                    {
                        // Convert to mongo-compatible object
                        MongoNamedEntityReverse mongoObject = Utils.ConvertType<MongoNamedEntityReverse>(ne);

                        // Create the filter for the upsert
                        FilterDefinition<MongoNamedEntityReverse> filter = Builders<MongoNamedEntityReverse>.Filter.Eq(x => x.PartitionKey, ne.PartitionKey) &
                            Builders<MongoNamedEntityReverse>.Filter.Eq(x => x.RowKey, ne.RowKey);

                        UpdateDefinition<MongoNamedEntityReverse> updateDefinition = new UpdateDefinitionBuilder<MongoNamedEntityReverse>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition
                            .SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("OriginalText", mongoObject.OriginalText)
                            .Set("Type", mongoObject.Type)
                            .Set("NamedEntityLocation", mongoObject.NamedEntityLocation)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoNamedEntityReverse> update = new UpdateOneModel<MongoNamedEntityReverse>(filter, updateDefinition) { IsUpsert = true };
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

        public static string AddNamedEntityCountForFilename(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntityCount> namedEntities)
        {
            return AddNamedEntityCount(providerConfig, namedEntities, "filename");
        }
        public static string AddNamedEntityCountForBody(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntityCount> namedEntities)
        {
            return AddNamedEntityCount(providerConfig, namedEntities, "body");
        }

        public static string AddNamedEntityCount(DataConfig providerConfig, List<POCO.RecordAssociationNamedEntityCount> namedEntities, string from)
        {
            string tableName = "";

            // Check if there are any entities to add
            if (namedEntities.Count == 0)
            {
                return string.Empty;
            }

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    switch (from)
                    {
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntityCount;
                                break;
                            }
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntityCountFilename;
                                break;
                            }
                        default:
                            throw new ApplicationException("unable to determine Named Entity source: " + from);
                    }
                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    List<TableOperation> ops = new List<TableOperation>();

                    foreach (POCO.RecordAssociationNamedEntityCount ne in namedEntities)
                    {
                        AzureNamedEntityCount az = new AzureNamedEntityCount(ne);
                        TableOperation operation = TableOperation.InsertOrReplace(az);

                        ops.Add(operation);

                    }

                    // Make sure there are operations to write
                    if (ops.Count > 0)
                    {
                        Utils.AzureBatchExecute(table, ops);
                    }

                    break;

                case "internal.mongodb":
                    switch (from)
                    {
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntityCount;
                                break;
                            }
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntityCountFilename;
                                break;
                            }
                        default:
                            throw new ApplicationException("unable to determine Named Entity source: " + from);
                    }
                    IMongoCollection<MongoNamedEntityCount> collection = Utils.GetMongoCollection<MongoNamedEntityCount>(providerConfig, tableName);

                    var operationList = new List<WriteModel<MongoNamedEntityCount>>();
                    foreach (POCO.RecordAssociationNamedEntityCount ne in namedEntities)
                    {
                        // Convert to mongo-compatible object
                        MongoNamedEntityCount mongoObject = Utils.ConvertType<MongoNamedEntityCount>(ne);

                        // Create the filter for the upsert
                        FilterDefinition<MongoNamedEntityCount> filter = Builders<MongoNamedEntityCount>.Filter.Eq(x => x.PartitionKey, ne.PartitionKey) &
                            Builders<MongoNamedEntityCount>.Filter.Eq(x => x.RowKey, ne.RowKey);

                        UpdateDefinition<MongoNamedEntityCount> updateDefinition = new UpdateDefinitionBuilder<MongoNamedEntityCount>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition
                            .SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("OriginalText", mongoObject.OriginalText)
                            .Set("Type", mongoObject.Type)
                            .Set("NamedEntityCount", mongoObject.NamedEntityCount)
                            .Set("NamedEntityLocation", mongoObject.NamedEntityLocation)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoNamedEntityCount> update = new UpdateOneModel<MongoNamedEntityCount>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);

                    }

                    // Make sure there are operations to write
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

        public static string AddFileMetadata(DataConfig providerConfig, List<RecordAssociationFileMetadata> fileMeta)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationFileMetadata);
                    foreach (POCO.RecordAssociationFileMetadata meta in fileMeta)
                    {
                        AzureFileMetadata az = new AzureFileMetadata(meta);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoFileMetadata> collection = Utils.GetMongoCollection<MongoFileMetadata>(providerConfig, MongoTableNames.RecordAssociationFileMetadata);

                    var operationList = new List<WriteModel<MongoFileMetadata>>();
                    foreach (POCO.RecordAssociationFileMetadata fm in fileMeta)
                    {
                        // Convert to mongo-compatible object
                        MongoFileMetadata mongoObject = Utils.ConvertType<MongoFileMetadata>(fm);

                        // Create the filter for the upsert
                        FilterDefinition<MongoFileMetadata> filter = Builders<MongoFileMetadata>.Filter.Eq(x => x.PartitionKey, fm.PartitionKey) &
                            Builders<MongoFileMetadata>.Filter.Eq(x => x.RowKey, fm.RowKey);

                        UpdateDefinition<MongoFileMetadata> updateDefinition = new UpdateDefinitionBuilder<MongoFileMetadata>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("FieldName", mongoObject.FieldName)
                            .Set("FieldValue", mongoObject.FieldValue)
                            .Set("SystemUri", mongoObject.SystemUri)
                            .Set("SystemType", mongoObject.SystemType);

                        UpdateOneModel<MongoFileMetadata> update = new UpdateOneModel<MongoFileMetadata>(filter, updateDefinition) { IsUpsert = true };
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

        public static RecordAuthorityMatchStatus GetRecordAuthorityMatchStatus(DataConfig providerConfig, RecordToRecordAssociation recordAssocEntity)
        {
            List<POCO.RecordAuthorityMatchStatus> matchstatus = new List<POCO.RecordAuthorityMatchStatus>();

            List<DataFactory.Filter> filters = new List<Filter>();
            DataFactory.Filter pkfilt = new Filter("PartitionKey", recordAssocEntity.PartitionKey, "eq");
            filters.Add(pkfilt);
            DataFactory.Filter rkfilt = new Filter("RowKey", recordAssocEntity.RowKey, "eq");
            filters.Add(rkfilt);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAuthorityMatchStatus> azdata = new List<AzureRecordAuthorityMatchStatus>();
                    AzureTableAdaptor<AzureRecordAuthorityMatchStatus> adaptor = new AzureTableAdaptor<AzureRecordAuthorityMatchStatus>();
                    azdata = adaptor.ReadTableData(providerConfig, TableNames.Azure.RecordAssociation.RecordAssociationMatchStatus, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        matchstatus.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":

                    var collection = Utils.GetMongoCollection<MongoRecordAuthorityMatchStatus>(providerConfig, TableNames.Mongo.RecordAssociation.RecordAssociationMatchStatus);

                    FilterDefinition<MongoRecordAuthorityMatchStatus> filter = Utils.GenerateMongoFilter<MongoRecordAuthorityMatchStatus>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        matchstatus.Add(keyphrase);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            if (matchstatus.Count > 0)
            {
                return matchstatus[0];
            }
            else
            {
                return null;
            }
        }

        public static void AddRecordAuthorityMatchStatus(DataConfig providerConfig, POCO.RecordToRecordAssociation recordAssoc, string matchStatus)
        {
            POCO.RecordAuthorityMatchStatus status = new RecordAuthorityMatchStatus(recordAssoc.PartitionKey, recordAssoc.RowKey, matchStatus);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordAuthorityMatchStatus az = new AzureRecordAuthorityMatchStatus(status);

                    CloudTable table = Utils.GetCloudTable(providerConfig, TableNames.Azure.RecordAssociation.RecordAssociationMatchStatus);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordAuthorityMatchStatus> collection = Utils.GetMongoCollection<MongoRecordAuthorityMatchStatus>(providerConfig, TableNames.Mongo.RecordAssociation.RecordAssociationMatchStatus);
                    MongoRecordAuthorityMatchStatus mongoObject = Utils.ConvertType<MongoRecordAuthorityMatchStatus>(status);
                    collection.InsertOne(mongoObject);
                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }
    }
}
