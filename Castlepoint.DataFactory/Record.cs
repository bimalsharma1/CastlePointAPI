using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SysNet = System.Net;

using Castlepoint.POCO;

using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Logging;

namespace Castlepoint.DataFactory
{
    class MongoRecord:POCO.Record
    {
        public ObjectId _id { get; set; }
    }

    class MongoRecordToRecordAssociation : POCO.RecordToRecordAssociation
    {
        public ObjectId _id { get; set; }
    }

    class MongoRecordAuthorityMatchResult : POCO.RecordAuthorityMatchResult
    {
        public ObjectId _id { get; set; }
    }

    class MongoRecordChangeFlag : POCO.RecordChangeFlag
    {
        public ObjectId _id { get; set; }
    }

    class MongoKeyPhraseToRecord : POCO.KeyPhraseToRecordLookup
    {
        public ObjectId _id { get; set; }
    }

    class MongoMetadataToRecord : POCO.MetadataToRecordLookup
    {
        public ObjectId _id { get; set; }
    }

    class MongoNamedEntityToRecord : POCO.NamedEntityToRecordLookup
    {
        public ObjectId _id { get; set; }
    }

    class MongoRecordStatUpdate : POCO.RecordStat { }
    class MongoRecordCategorisedUpdate : POCO.RecordCategorised { }

    class AzureRecordAuthorityMatchResult : EntityAdapter<POCO.RecordAuthorityMatchResult>
    {
        public AzureRecordAuthorityMatchResult() { }
        public AzureRecordAuthorityMatchResult(POCO.RecordAuthorityMatchResult o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureRecord : EntityAdapter<POCO.Record>
    {
        public AzureRecord() { }
        public AzureRecord(POCO.Record record):base(record) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureKeyPhraseToRecord : EntityAdapter<POCO.KeyPhraseToRecordLookup>
    {
        public AzureKeyPhraseToRecord() { }
        public AzureKeyPhraseToRecord(POCO.KeyPhraseToRecordLookup o):base(o) { }

        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureMetadataToRecord : EntityAdapter<POCO.MetadataToRecordLookup>
    {
        public AzureMetadataToRecord() { }
        public AzureMetadataToRecord(POCO.MetadataToRecordLookup o) : base(o) { }

        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureNamedEntityToRecord : EntityAdapter<POCO.NamedEntityToRecordLookup>
    {
        public AzureNamedEntityToRecord() { }
        public AzureNamedEntityToRecord(POCO.NamedEntityToRecordLookup o) : base(o) { }

        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureRecordToRecordAssociation : EntityAdapter<POCO.RecordToRecordAssociation>
    {
        public AzureRecordToRecordAssociation() { }
        public AzureRecordToRecordAssociation(POCO.RecordToRecordAssociation o):base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureRecordIdUpdate : EntityAdapter<POCO.RecordIdUpdate>
    {
        public AzureRecordIdUpdate() { }
        public AzureRecordIdUpdate(POCO.RecordIdUpdate o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoRecordIdUpdate : POCO.RecordIdUpdate { }

    class AzureRecordCreationDate : EntityAdapter<POCO.RecordCreationDate>
    {
        public AzureRecordCreationDate() { }
        public AzureRecordCreationDate(POCO.RecordCreationDate o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoRecordCreationDate : POCO.RecordIdUpdate { }

    class MongoRecordClassification : POCO.RecordClassificationEntity { }

    class AzureRecordClassification : EntityAdapter<POCO.RecordClassificationEntity>
    {
        public AzureRecordClassification() { }
        public AzureRecordClassification(POCO.RecordClassificationEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }



    class AzureRecordChangeFlag : EntityAdapter<POCO.RecordChangeFlag>
    {
        public AzureRecordChangeFlag(RecordChangeFlag r) : base(r) { }

        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }

    }
    class AzureRecordStat : EntityAdapter<POCO.RecordStat>
    {
        public AzureRecordStat(RecordStat r) : base(r) { }

        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }

    }

    class AzureRecordCategorised : EntityAdapter<POCO.RecordCategorised>
    {
        public AzureRecordCategorised(RecordCategorised r) : base(r) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class Record
    {
        internal class AzureTableNames
        {
            internal const string Record = "stlprecords";
            internal const string RecordAllowList = "stlprecordsallowlist";
            internal const string RecordToRecordAssociation = "stlprecordtorecordassociation";
            internal const string RecordKeyPhrases = "stlprecordkeyphrases";
            internal const string RecordNamedEntities = "stlprecordnamedentities";
            internal const string RecordAssociationSubjectObject = "stlprecordassociationsubjectobject";
            internal const string RecordAssociationKeyPhraseReverse = "stlprecordassociationkeyphrasesreverse";
            internal const string RecordAssociationKeyPhraseReverseWords = "stlprecordassociationkeyphrasesreversewords";
            internal const string RecordAssociationKeyPhrase = "stlprecordassociationkeyphrases";
            internal const string RecordAssociationNamedEntity = "stlprecordassociationnamedentity";
            internal const string RecordsSentenceHistory = "stlprecordssentencehistory";
            internal const string RecordsSentenceAuthorityMatch = "stlprecordssentenceauthorityhistory";
        }

        internal class MongoTableNames
        {
            internal const string Record = "records";
            internal const string RecordAllowList = "recordsallowlist";
            internal const string RecordToRecordAssociation = "recordtorecordassociation";
            internal const string RecordKeyPhrases = "recordkeyphrases";
            internal const string RecordNamedEntities = "recordnamedentities";
            internal const string RecordAssociationSubjectObject = "recordassociationsubjectobject";
            internal const string RecordAssociationKeyPhraseReverse = "recordassociationkeyphrasesreverse";
            internal const string RecordAssociationKeyPhraseReverseWords = "recordassociationkeyphrasesreversewords";
            internal const string RecordAssociationKeyPhrase = "recordassociationkeyphrases";
            internal const string RecordAssociationNamedEntity = "recordassociationnamedentity";
            internal const string RecordsSentenceHistory = "recordssentencehistory";
            internal const string RecordsSentenceAuthorityMatch = "recordssentenceauthorityhistory";
        }

        public static string AddRecord(DataConfig providerConfig, POCO.Record record)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureRecord az = new AzureRecord(record);

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();
                    
                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecord> collection = Utils.GetMongoCollection<MongoRecord>(providerConfig, "records");
                    MongoRecord mongoObject = Utils.ConvertType<MongoRecord>(record);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddKeyPhrase(DataConfig providerConfig, POCO.KeyPhraseToRecordLookup kpToRecord)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    try
                    {
                        AzureKeyPhraseToRecord az = new AzureKeyPhraseToRecord(kpToRecord);

                        CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordKeyPhrases);
                        TableOperation operation = TableOperation.InsertOrMerge(az);
                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }
                    catch (Microsoft.WindowsAzure.Storage.StorageException ex)
                    {
                        var requestInformation = ex.RequestInformation;
                        Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

                        // get more details about the exception 
                        var information = requestInformation.ExtendedErrorInformation;
                        // if you have aditional information, you can use it for your logs
                        if (information != null)
                        {
                            var errorCode = information.ErrorCode;
                            var errMessage = string.Format("({0}) {1}",
                            errorCode,
                            information.ErrorMessage);
                        }
                    }

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoKeyPhraseToRecord> collection = Utils.GetMongoCollection<MongoKeyPhraseToRecord>(providerConfig, MongoTableNames.RecordKeyPhrases);
                    MongoKeyPhraseToRecord mongoObject = Utils.ConvertType<MongoKeyPhraseToRecord>(kpToRecord);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddMetadata(DataConfig providerConfig, POCO.MetadataToRecordLookup metaToRecord)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    try
                    {
                        AzureMetadataToRecord az = new AzureMetadataToRecord(metaToRecord);

                        CloudTable table = Utils.GetCloudTable(providerConfig, TableNames.Azure.Record.RecordToMetadata);
                        TableOperation operation = TableOperation.InsertOrMerge(az);
                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }
                    catch (Microsoft.WindowsAzure.Storage.StorageException ex)
                    {
                        var requestInformation = ex.RequestInformation;
                        Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

                        // get more details about the exception 
                        var information = requestInformation.ExtendedErrorInformation;
                        // if you have aditional information, you can use it for your logs
                        if (information != null)
                        {
                            var errorCode = information.ErrorCode;
                            var errMessage = string.Format("({0}) {1}",
                            errorCode,
                            information.ErrorMessage);
                        }
                    }

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoMetadataToRecord> collection = Utils.GetMongoCollection<MongoMetadataToRecord>(providerConfig, TableNames.Mongo.Record.RecordToMetadata);
                    MongoMetadataToRecord mongoObject = Utils.ConvertType<MongoMetadataToRecord>(metaToRecord);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.Record> GetRecordsAllowList(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.Record> records = new List<POCO.Record>();

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecord> azdata = new List<AzureRecord>();
                    AzureTableAdaptor<AzureRecord> adaptor = new AzureTableAdaptor<AzureRecord>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordAllowList, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        records.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:
                    var collection = Utils.GetMongoCollection<MongoRecord>(providerConfig, Record.MongoTableNames.RecordAllowList);

                    FilterDefinition<MongoRecord> filter = Utils.GenerateMongoFilter<MongoRecord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var record in documents)
                    {
                        records.Add(record);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return records;
        }

        public static string AddNamedEntity(DataConfig providerConfig, POCO.NamedEntityToRecordLookup neToRecord)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    try
                    {
                        AzureNamedEntityToRecord az = new AzureNamedEntityToRecord(neToRecord);

                        CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordNamedEntities);
                        TableOperation operation = TableOperation.InsertOrMerge(az);
                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }
                    catch (AggregateException exAgg)
                    {
                        if (exAgg.InnerException != null && exAgg.InnerException is Microsoft.WindowsAzure.Storage.StorageException)
                        {
                            var innerException = (Microsoft.WindowsAzure.Storage.StorageException)exAgg.InnerException;
                            var requestInformation = innerException.RequestInformation;
                            Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

                            // get more details about the exception 
                            var information = requestInformation.ExtendedErrorInformation;
                            // if you have aditional information, you can use it for your logs
                            if (information != null)
                            {
                                var errorCode = information.ErrorCode;
                                var errMessage = string.Format("({0}) {1}",
                                errorCode,
                                information.ErrorMessage);
                            }
                        }
                    }
                    catch (Microsoft.WindowsAzure.Storage.StorageException ex)
                    {
                        var requestInformation = ex.RequestInformation;
                        Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

                        // get more details about the exception 
                        var information = requestInformation.ExtendedErrorInformation;
                        // if you have aditional information, you can use it for your logs
                        if (information != null)
                        {
                            var errorCode = information.ErrorCode;
                            var errMessage = string.Format("({0}) {1}",
                            errorCode,
                            information.ErrorMessage);
                        }
                    }

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoNamedEntityToRecord> collection = Utils.GetMongoCollection<MongoNamedEntityToRecord>(providerConfig, MongoTableNames.RecordNamedEntities);
                    MongoNamedEntityToRecord mongoObject = Utils.ConvertType<MongoNamedEntityToRecord>(neToRecord);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddRecordToRecordAssociation(DataConfig providerConfig, POCO.RecordToRecordAssociation recordToRecordAssociation)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordToRecordAssociation az = new AzureRecordToRecordAssociation(recordToRecordAssociation);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordToRecordAssociation);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordToRecordAssociation> collection = Utils.GetMongoCollection<MongoRecordToRecordAssociation>(providerConfig, MongoTableNames.RecordToRecordAssociation);
                    MongoRecordToRecordAssociation mongoObject = Utils.ConvertType<MongoRecordToRecordAssociation>(recordToRecordAssociation);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.Record> GetRecords (DataConfig providerConfig, POCO.System system)
        {
            // Create a filters object
            string partitionKey = system.PartitionKey;
            if (!partitionKey.EndsWith("|")) { partitionKey += "|"; }

            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", partitionKey, "eq");
            filters.Add(pkFilter);

            return GetRecords(providerConfig, filters);
        }

        public static List<POCO.Record> GetRecords(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.Record> records = new List<POCO.Record>();

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecord> azdata = new List<AzureRecord>();
                    AzureTableAdaptor<AzureRecord> adaptor = new AzureTableAdaptor<AzureRecord>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.Record, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        records.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:
                    var collection = Utils.GetMongoCollection<MongoRecord>(providerConfig, Record.MongoTableNames.Record);

                    FilterDefinition<MongoRecord> filter = Utils.GenerateMongoFilter<MongoRecord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var record in documents)
                    {
                        records.Add(record);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return records;

        }

        public static List<POCO.RecordAuthorityMatchStatus> GetRecordMatchStatus(DataConfig providerConfig, POCO.Record record)
        {
            string partitionKey = record.RowKey;

            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", partitionKey, "eq");
            filters.Add(pkFilter);

            return Record.GetRecordMatchStatus(providerConfig, filters);
        }


        public static List<POCO.RecordAuthorityMatchStatus> GetRecordMatchStatus(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAuthorityMatchStatus> matchStatus = new List<RecordAuthorityMatchStatus>();

            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAuthorityMatchStatus> azdata = new List<AzureRecordAuthorityMatchStatus>();
                    AzureTableAdaptor<AzureRecordAuthorityMatchStatus> adaptor = new AzureTableAdaptor<AzureRecordAuthorityMatchStatus>();
                    azdata = adaptor.ReadTableData(providerConfig, TableNames.Azure.RecordAssociation.RecordAssociationMatchStatus, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        matchStatus.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAuthorityMatchStatus>(providerConfig, TableNames.Mongo.RecordAssociation.RecordAssociationMatchStatus);

                    FilterDefinition<MongoRecordAuthorityMatchStatus> filter = Utils.GenerateMongoFilter<MongoRecordAuthorityMatchStatus>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        matchStatus.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return matchStatus;
        }

        public static List<POCO.NamedEntityToRecordLookup> GetRecordNamedEntitys(DataConfig providerConfig, POCO.Record record)
        {
            string partitionKey = record.RowKey;

            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", partitionKey, "eq");
            filters.Add(pkFilter);

            return Record.GetRecordNamedEntitys(providerConfig, filters);
        }

        public static List<POCO.NamedEntityToRecordLookup> GetRecordNamedEntitys(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.NamedEntityToRecordLookup> namedEntitys = new List<NamedEntityToRecordLookup>();

            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureNamedEntityToRecord> azdata = new List<AzureNamedEntityToRecord>();
                    AzureTableAdaptor<AzureNamedEntityToRecord> adaptor = new AzureTableAdaptor<AzureNamedEntityToRecord>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordNamedEntities, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        namedEntitys.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoNamedEntityToRecord>(providerConfig, Record.MongoTableNames.RecordNamedEntities);
                    
                    FilterDefinition<MongoNamedEntityToRecord> filter = Utils.GenerateMongoFilter<MongoNamedEntityToRecord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        namedEntitys.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return namedEntitys;
        }

        public static List<POCO.KeyPhraseToRecordLookup> GetRecordKeyPhrases(DataConfig providerConfig, POCO.Record record)
        {
            string partitionKey = record.RowKey;

            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", partitionKey, "eq");
            filters.Add(pkFilter);

            return Record.GetRecordKeyPhrases(providerConfig, filters);
        }

        public static List<POCO.KeyPhraseToRecordLookup> GetRecordKeyPhrases(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.KeyPhraseToRecordLookup> keyPhrases = new List<KeyPhraseToRecordLookup>();

            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureKeyPhraseToRecord> azdata = new List<AzureKeyPhraseToRecord>();
                    AzureTableAdaptor<AzureKeyPhraseToRecord> adaptor = new AzureTableAdaptor<AzureKeyPhraseToRecord>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordKeyPhrases, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyPhrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoKeyPhraseToRecord>(providerConfig, Record.MongoTableNames.RecordKeyPhrases);

                    FilterDefinition<MongoKeyPhraseToRecord> filter = Utils.GenerateMongoFilter<MongoKeyPhraseToRecord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyPhrases.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return keyPhrases;
        }

        public static List<POCO.RecordAssociationKeyPhrase> GetRecordAssociationKeyPhrases(DataConfig providerConfig, POCO.Record record)
        {
            List<RecordAssociationKeyPhrase> kpToRecord = new List<RecordAssociationKeyPhrase>();

            string partitionKey = "";

            switch (record.Type)
            {
                case "Basecamp.Project":
                    throw new NotImplementedException();
                    //partitionKey = BasecampProject.GetBasecampBucketUrl(record.RowKey);
                    break;
                default:
                    partitionKey = record.RowKey;
                    break;
            }
            partitionKey = Utils.CleanTableKey(partitionKey);

            // Get all the record associations for this record
            List<RecordToRecordAssociation> recordAssocs = GetRecordToRecordsAssociations(providerConfig, record);
            foreach (RecordToRecordAssociation recassoc in recordAssocs)
            {
                // Create the filter from the Record
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter pk = new DataFactory.Filter("PartitionKey", recassoc.RowKey, "eq");
                filters.Add(pk);

                List<RecordAssociationKeyPhrase> recordAssocKeyPhrases = Record.GetRecordAssociationKeyPhrases(providerConfig, filters);

                kpToRecord.AddRange(recordAssocKeyPhrases);
            }

            return kpToRecord;
        }

        public static int CountRecordAssociationKeyPhrases(DataConfig providerConfig, POCO.Record record)
        {
            int totalKeyPhrases = 0;

            string partitionKey = "";

            switch (record.Type)
            {
                case "Basecamp.Project":
                    throw new NotImplementedException();
                    //partitionKey = BasecampProject.GetBasecampBucketUrl(record.RowKey);
                    break;
                default:
                    partitionKey = record.RowKey;
                    break;
            }
            partitionKey = Utils.CleanTableKey(partitionKey);

            // Get all the record associations for this record
            List<RecordToRecordAssociation> recordAssocs = GetRecordToRecordsAssociations(providerConfig, record);
            foreach (RecordToRecordAssociation recassoc in recordAssocs)
            {
                // Create the filter from the Record
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter pk = new DataFactory.Filter("PartitionKey", recassoc.RowKey, "eq");
                filters.Add(pk);

                // Get the Rec Assoc key phrases
                List<RecordAssociationKeyPhrase> recordAssocKeyPhrases = Record.GetRecordAssociationKeyPhrases(providerConfig, filters);

                totalKeyPhrases += recordAssocKeyPhrases.Count;
            }

            return totalKeyPhrases;
        }

        public static int CountRecordAssociationNamedEntities(DataConfig providerConfig, POCO.Record record)
        {
            int totalNamedEnt = 0;

            string partitionKey = "";

            switch (record.Type)
            {
                case "Basecamp.Project":
                    throw new NotImplementedException();
                    //partitionKey = BasecampProject.GetBasecampBucketUrl(record.RowKey);
                    break;
                default:
                    partitionKey = record.RowKey;
                    break;
            }
            partitionKey = Utils.CleanTableKey(partitionKey);

            // Get all the record associations for this record
            List<RecordToRecordAssociation> recordAssocs = GetRecordToRecordsAssociations(providerConfig, record);
            foreach (RecordToRecordAssociation recassoc in recordAssocs)
            {
                // Create the filter from the Record
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter pk = new DataFactory.Filter("PartitionKey", recassoc.RowKey, "eq");
                filters.Add(pk);

                // Get the Rec Assoc key phrases
                List<RecordAssociationNamedEntity> recordAssocKeyNamedEnt = Record.GetRecordAssociationNamedEntitys(providerConfig, filters);

                totalNamedEnt += recordAssocKeyNamedEnt.Count;
            }

            return totalNamedEnt;
        }

        public static int CountRecordAssociationSubjectObject(DataConfig providerConfig, POCO.Record record)
        {
            int totalSubjectObject = 0;

            string partitionKey = "";

            switch (record.Type)
            {
                case "Basecamp.Project":
                    throw new NotImplementedException();
                    //partitionKey = BasecampProject.GetBasecampBucketUrl(record.RowKey);
                    break;
                default:
                    partitionKey = record.RowKey;
                    break;
            }
            partitionKey = Utils.CleanTableKey(partitionKey);

            // Get all the record associations for this record
            List<RecordToRecordAssociation> recordAssocs = GetRecordToRecordsAssociations(providerConfig, record);
            foreach (RecordToRecordAssociation recassoc in recordAssocs)
            {
                // Create the filter from the Record
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter pk = new DataFactory.Filter("PartitionKey", recassoc.RowKey, "eq");
                filters.Add(pk);

                // Get the Rec Assoc key phrases
                List<RecordAssociationSubjectObject> recordAssocKeySubjectObject = Record.GetRecordAssociationSubjectObject(providerConfig, filters);

                totalSubjectObject += recordAssocKeySubjectObject.Count;
            }

            return totalSubjectObject;
        }

        public static List<POCO.RecordAssociationKeyPhrase> GetRecordAssociationKeyPhrases(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationKeyPhrase> keyPhrases = new List<RecordAssociationKeyPhrase>();



            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationKeyPhrase> azdata = new List<AzureRecordAssociationKeyPhrase>();
                    AzureTableAdaptor<AzureRecordAssociationKeyPhrase> adaptor = new AzureTableAdaptor<AzureRecordAssociationKeyPhrase>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordAssociationKeyPhrase, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyPhrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAssociationKeyPhrase>(providerConfig, MongoTableNames.RecordAssociationKeyPhrase);

                    FilterDefinition<MongoRecordAssociationKeyPhrase> filter = Utils.GenerateMongoFilter<MongoRecordAssociationKeyPhrase>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyPhrases.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return keyPhrases;
        }

        public static bool UpdateRecordId(DataConfig providerConfig, POCO.System system, POCO.Record record, ILogger logger)
        {
            POCO.RecordIdUpdate recordIdUpdate = new RecordIdUpdate(record);
            recordIdUpdate.RecordId = record.RecordId;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordIdUpdate az = new AzureRecordIdUpdate(recordIdUpdate);

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoRecordIdUpdate> collection = Utils.GetMongoCollection<MongoRecordIdUpdate>(providerConfig, Record.MongoTableNames.Record);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(record.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(record.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoRecordIdUpdate> filter = Utils.GenerateMongoFilter<MongoRecordIdUpdate>(filters);

                    var update = Builders<MongoRecordIdUpdate>.Update
                        .SetOnInsert("PartitionKey", record.PartitionKey)
                        .SetOnInsert("RowKey", record.RowKey)
                        .Set("RecordId", record.RecordId);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return true;
        }

        public static bool UpdateCreationDate(DataConfig providerConfig, POCO.System system, POCO.Record record, DateTime firstCreatedDate)
        {
            POCO.RecordCreationDate recordCreation = new RecordCreationDate(record);
            recordCreation.Created = firstCreatedDate;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordCreationDate az = new AzureRecordCreationDate(recordCreation);

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoRecordCreationDate> collection = Utils.GetMongoCollection<MongoRecordCreationDate>(providerConfig, Record.MongoTableNames.Record);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(recordCreation.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(recordCreation.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoRecordCreationDate> filter = Utils.GenerateMongoFilter<MongoRecordCreationDate>(filters);

                    var update = Builders<MongoRecordCreationDate>.Update
                        .SetOnInsert("PartitionKey", recordCreation.PartitionKey)
                        .SetOnInsert("RowKey", recordCreation.RowKey)
                        .Set("Created", recordCreation.Created);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return true;
        }

        public static List<POCO.RecordAssociationNamedEntity> GetRecordAssociationNamedEntitys(DataConfig providerConfig, POCO.Record record)
        {
            string partitionKey = "";

            switch (record.Type)
            {
                case "Basecamp.Project":
                    partitionKey = BasecampProject.GetBasecampBucketUrl(record.RowKey);
                    break;
                default:
                    partitionKey = record.RowKey;
                    break;
            }
            partitionKey = Utils.CleanTableKey(partitionKey);

            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkGEFilter = new DataFactory.Filter("PartitionKey", partitionKey, "ge");
            DataFactory.Filter pkLTFilter = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(partitionKey), "lt");
            filters.Add(pkGEFilter);
            filters.Add(pkLTFilter);

            return Record.GetRecordAssociationNamedEntitys(providerConfig, filters);
        }

        public static void DeleteKeyphraseMatch(DataConfig providerConfig, POCO.Record record, RecordToRecordAssociation recordAssocEntity)
        {

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pk);
            string rowkey = string.Empty;
            if (!recordAssocEntity.RowKey.EndsWith("|"))
            { rowkey = recordAssocEntity.RowKey + "|"; }
            else { rowkey = recordAssocEntity.RowKey; }
            Filter rk = new Filter("RowKey", rowkey, "ge");
            filters.Add(rk);
            rk = new Filter("RowKey", Utils.GetLessThanFilter(rowkey), "lt");
            filters.Add(rk);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Get a list of entities to delete
                    List<KeyPhraseToRecordLookup> keyphraseToRecord = GetRecordKeyPhrases(providerConfig, filters);

                    //TODO better way for bulk delete
                    foreach (KeyPhraseToRecordLookup l in keyphraseToRecord)
                    {
                        AzureKeyPhraseToRecordLookup az = new AzureKeyPhraseToRecordLookup(l);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.RecordKeyPhrases);
                        TableOperation operation = TableOperation.Delete(az);

                        Task<TableResult> tDelete = table.ExecuteAsync(operation);
                        tDelete.Wait();

                        // Check for "success with no status" code
                        if (tDelete.Result.HttpStatusCode != 204)
                        {
                            // TODO
                            bool isNotDeleted = true;
                        }

                    }

                    break;

                case "internal.mongodb":
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Delete the rows                      
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, Record.MongoTableNames.RecordKeyPhrases);
                    DeleteResult result = collection.DeleteMany(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void DeleteNamedEntityMatch(DataConfig providerConfig, POCO.Record record, RecordToRecordAssociation recordAssocEntity)
        {

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pk);
            Filter rk = new Filter("RowKey", recordAssocEntity.RowKey, "ge");
            filters.Add(rk);
            rk = new Filter("RowKey", Utils.GetLessThanFilter(recordAssocEntity.RowKey), "lt");
            filters.Add(rk);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Get a list of entities to delete
                    List<NamedEntityToRecordLookup> namedentityToRecord = GetRecordNamedEntitys(providerConfig, filters);

                    //TODO better way for bulk delete
                    foreach (NamedEntityToRecordLookup l in namedentityToRecord)
                    {
                        AzureNamedEntityToRecord az = new AzureNamedEntityToRecord(l);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.RecordNamedEntities);
                        TableOperation operation = TableOperation.Delete(az);

                        Task<TableResult> tDelete = table.ExecuteAsync(operation);
                        tDelete.Wait();

                        // Check for "success with no status" code
                        if (tDelete.Result.HttpStatusCode != 204)
                        {
                            // TODO
                            bool isNotDeleted = true;
                        }

                    }

                    break;

                case "internal.mongodb":
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Delete the rows                      
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, Record.MongoTableNames.RecordNamedEntities);
                    DeleteResult result = collection.DeleteMany(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static List<POCO.RecordAssociationNamedEntity> GetRecordAssociationNamedEntitys(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationNamedEntity> namedEntitys = new List<RecordAssociationNamedEntity>();

            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureNamedEntity> azdata = new List<AzureNamedEntity>();
                    AzureTableAdaptor<AzureNamedEntity> adaptor = new AzureTableAdaptor<AzureNamedEntity>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordAssociationNamedEntity, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        namedEntitys.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoNamedEntity>(providerConfig, MongoTableNames.RecordAssociationNamedEntity);

                    FilterDefinition<MongoNamedEntity> filter = Utils.GenerateMongoFilter<MongoNamedEntity>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var namedentity in documents)
                    {
                        namedEntitys.Add(namedentity);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return namedEntitys;
        }

        public static List<POCO.RecordAssociationSubjectObject> GetRecordAssociationSubjectObject(DataConfig providerConfig, POCO.Record record)
        {

            string partitionKey = "";

            switch (record.Type)
            {
                case "Basecamp.Project":
                    partitionKey = BasecampProject.GetBasecampBucketUrl(record.RowKey);
                    break;
                default:
                    partitionKey = record.RowKey;
                    break;
            }
            partitionKey = Utils.CleanTableKey(partitionKey);

            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkGEFilter = new DataFactory.Filter("PartitionKey", partitionKey, "ge");
            DataFactory.Filter pkLTFilter = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(partitionKey), "lt");
            filters.Add(pkGEFilter);
            filters.Add(pkLTFilter);

            return Record.GetRecordAssociationSubjectObject(providerConfig, filters);

        }

        public static List<POCO.RecordAssociationSubjectObject> GetRecordAssociationSubjectObject(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAssociationSubjectObject> subjectObjects = new List<POCO.RecordAssociationSubjectObject>();

            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSubjectObject> azdata = new List<AzureSubjectObject>();
                    AzureTableAdaptor<AzureSubjectObject> adaptor = new AzureTableAdaptor<AzureSubjectObject>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordAssociationSubjectObject, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        subjectObjects.Add(doc.Value);
                    }
                    

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSubjectObject>(providerConfig, Record.MongoTableNames.RecordAssociationSubjectObject);

                    FilterDefinition<MongoSubjectObject> filter = Utils.GenerateMongoFilter<MongoSubjectObject>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var subjectobject in documents)
                    {
                        subjectObjects.Add(subjectobject);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return subjectObjects;
        }

        public static List<POCO.KeyPhraseToRecordLookup> GetKeyPhrases(DataConfig providerConfig, POCO.Record record)
        {
            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pkFilter);

            return Record.GetKeyPhrases(providerConfig, filters);
        }

        public static List<POCO.KeyPhraseToRecordLookup> GetKeyPhrases(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.KeyPhraseToRecordLookup> keyPhrases = new List<KeyPhraseToRecordLookup>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureKeyPhraseToRecord> azdata = new List<AzureKeyPhraseToRecord>();
                    AzureTableAdaptor<AzureKeyPhraseToRecord> adaptor = new AzureTableAdaptor<AzureKeyPhraseToRecord>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordKeyPhrases, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        keyPhrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoKeyPhraseToRecord>(providerConfig, Record.MongoTableNames.RecordKeyPhrases);

                    FilterDefinition<MongoKeyPhraseToRecord> filter = Utils.GenerateMongoFilter<MongoKeyPhraseToRecord>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        keyPhrases.Add(keyphrase);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return keyPhrases;
        }

        public static List<POCO.RecordToRecordAssociation> GetRecordToRecordsAssociations(DataConfig providerConfig, POCO.Record record)
        {
            // Create the filter from the Record
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pkFilter);

            return Record.GetRecordToRecordsAssociations(providerConfig, filters);
        }

        public static List<POCO.RecordToRecordAssociation> GetRecordToRecordsAssociations(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordToRecordAssociation> recordToRecordAssociations = new List<POCO.RecordToRecordAssociation>();


            switch (providerConfig.ProviderType)
            {

                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordToRecordAssociation> azdata = new List<AzureRecordToRecordAssociation>();
                    AzureTableAdaptor<AzureRecordToRecordAssociation> adaptor = new AzureTableAdaptor<AzureRecordToRecordAssociation>();
                    azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.RecordToRecordAssociation, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        recordToRecordAssociations.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordToRecordAssociation>(providerConfig, Record.MongoTableNames.RecordToRecordAssociation);

                    FilterDefinition<MongoRecordToRecordAssociation> filter = Utils.GenerateMongoFilter<MongoRecordToRecordAssociation>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var record in documents)
                    {
                        recordToRecordAssociations.Add(record);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return recordToRecordAssociations;

        }

        public static bool ResetMatchStatus(DataConfig providerConfig, POCO.Record record)
        {
            bool isRecordMatchStatusReset = false;

            long rowsDeleted = DeleteRecordMatchStatus(providerConfig, record);

            isRecordMatchStatusReset = (rowsDeleted >= 0);

            return isRecordMatchStatusReset;
        }

        public static bool ForceChangeFlag(DataConfig providerConfig, POCO.Record record)
        {
            bool isRecordChangeFlagReset = false;

            string resetGuid = "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF";

            isRecordChangeFlagReset = SetRecordChangeFlag(providerConfig, record, resetGuid);

            return isRecordChangeFlagReset;
        }

        public static bool SetRecordChangeFlag(DataConfig providerConfig, POCO.Record record, string changeFlagId)
        {
            bool isChangeFlagged = false;

            // Create a change flag
            POCO.RecordChangeFlag changeFlag = new RecordChangeFlag(record.PartitionKey, record.RowKey);
            changeFlag.ChangeFlagId = changeFlagId;
            // Check if we need to reset the Last Categorised Guid as well (in case it was flagged for force processing)
            if (changeFlag.ChangeFlagId == string.Empty) { changeFlag.LastCategorisedGuid = ""; }

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);

                    // Check if we are setting the change flag, or resetting it to string.empty
                    // This handles the case where we can SET it multiple times, but a reset only occurs if the previous ChangeTagId matches the current one
                    string eTag = string.Empty;
                    if (changeFlagId == string.Empty)
                    {
                        // Get the ETag for the Record direct from Azure storage
                        List<DataFactory.Filter> azfilters = new List<Filter>();
                        DataFactory.Filter azpkFilter = new DataFactory.Filter("PartitionKey", changeFlag.PartitionKey, "eq");
                        DataFactory.Filter azrkFilter = new DataFactory.Filter("RowKey", changeFlag.RowKey, "eq");
                        DataFactory.Filter azflagFilter = new DataFactory.Filter("ChangeFlagId", record.ChangeFlagId, "eq");
                        azfilters.Add(azpkFilter);
                        azfilters.Add(azrkFilter);
                        azfilters.Add(azflagFilter);

                        string azureRecordFilter = Utils.GenerateAzureFilter(azfilters);

                        // Get the Record data
                        List<AzureRecord> azdata = new List<AzureRecord>();
                        AzureTableAdaptor<AzureRecord> adaptor = new AzureTableAdaptor<AzureRecord>();
                        azdata = adaptor.ReadTableData(providerConfig, Record.AzureTableNames.Record, azureRecordFilter);
                        if (azdata.Count == 0)
                        {
                            // Record not found - change flag id has likely(?) been changed already by another process
                            return true;
                        }

                        if (azdata.Count > 0)
                        {
                            eTag = azdata[0].ETag;
                        }
                    }

                    // Update the record, using the eTag for consistency if required
                    AzureRecordChangeFlag az = new AzureRecordChangeFlag(changeFlag);
                    if (eTag != string.Empty)
                    {
                        az.ETag = eTag;
                    }
                    else
                    {
                        az.ETag = "*";
                    }

                    // Call the merge operation
                    TableOperation operation = TableOperation.Merge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordChangeFlag> collection = Utils.GetMongoCollection<MongoRecordChangeFlag>(providerConfig, Record.MongoTableNames.Record);
                    MongoRecordChangeFlag mongoObject = Utils.ConvertType<MongoRecordChangeFlag>(changeFlag);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(changeFlag.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(changeFlag.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);

                    // Only check the previous ChangeFlagId when we are trying to reset the ChangeFlagId
                    if (changeFlagId == string.Empty)
                    {
                        // NOTE - original record.ChangeFlagId is used to check if the record has already been updated by another process
                        DataFactory.Filter flagFilter = new DataFactory.Filter("ChangeFlagId", record.ChangeFlagId, "eq");
                        filters.Add(flagFilter);
                    }

                    FilterDefinition<MongoRecordChangeFlag> filter = Utils.GenerateMongoFilter<MongoRecordChangeFlag>(filters);

                    //string updateParam = "{$set: {LastContentsUpdated: '" + recordStat.LastContentsUpdated.ToString(Utils.ISODateFormat) + "', Stats: '" + recordStat.Stats + "'}}";
                    string updateParam = "{$set: {ChangeFlagId: '" + changeFlag.ChangeFlagId + "',LastCategorisedGuid: '" + changeFlag.LastCategorisedGuid + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, updateDoc);

                    isChangeFlagged = true;

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return isChangeFlagged;
        }

        public static void UpdateRecordStatistics(DataConfig providerConfig, POCO.RecordStat recordStat)
        {

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordStat az = new AzureRecordStat(recordStat);

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordStatUpdate> collection = Utils.GetMongoCollection<MongoRecordStatUpdate>(providerConfig, Record.MongoTableNames.Record);
                    MongoRecordStatUpdate mongoObject = Utils.ConvertType<MongoRecordStatUpdate>(recordStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(recordStat.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(recordStat.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);

                    FilterDefinition<MongoRecordStatUpdate> filter = Utils.GenerateMongoFilter<MongoRecordStatUpdate>(filters);

                    //string updateParam = "{$set: {LastContentsUpdated: '" + recordStat.LastContentsUpdated.ToString(Utils.ISODateFormat) + "', Stats: '" + recordStat.Stats + "'}}";
                    string updateParam = "{$set: {Stats: '" + recordStat.Stats + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update the batch status
                    collection.UpdateOne(filter, updateDoc);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void ResetRecordCategorised(DataConfig providerConfig, POCO.RecordCategorised recordCat)
        {

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordCategorised az = new AzureRecordCategorised(recordCat);

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordCategorisedUpdate> collection = Utils.GetMongoCollection<MongoRecordCategorisedUpdate>(providerConfig, Record.MongoTableNames.Record);
                    MongoRecordCategorisedUpdate mongoObject = Utils.ConvertType<MongoRecordCategorisedUpdate>(recordCat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(recordCat.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(recordCat.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);

                    FilterDefinition<MongoRecordCategorisedUpdate> filter = Utils.GenerateMongoFilter<MongoRecordCategorisedUpdate>(filters);

                    string updateParam = "{$set: {LastCategorisedGuid: '" + recordCat.LastCategorisedGuid + "}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update the batch status
                    collection.UpdateOne(filter, updateDoc);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static long UpdateRecordClassification(DataConfig providerConfig, RecordClassificationEntity rEntityClassification)
        {
            long rowsUpdated = 0;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordClassification az = new AzureRecordClassification(rEntityClassification);

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task<TableResult> tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoRecordClassification> collection = Utils.GetMongoCollection<MongoRecordClassification>(providerConfig, Record.MongoTableNames.Record);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(rEntityClassification.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(rEntityClassification.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoRecordClassification> filter = Utils.GenerateMongoFilter<MongoRecordClassification>(filters);

                    var update = Builders<MongoRecordClassification>.Update
                        .SetOnInsert("PartitionKey", rEntityClassification.PartitionKey)
                        .SetOnInsert("RowKey", rEntityClassification.RowKey)
                        .Set("Activity", rEntityClassification.Activity)
                        .Set("ClassNo", rEntityClassification.ClassNo)
                        .Set("Created", rEntityClassification.Created)
                        .Set("ExpiryDate", rEntityClassification.ExpiryDate)
                        .Set("RASchemaUri", rEntityClassification.RASchemaUri)
                        .Set("Function", rEntityClassification.Function)
                        .Set("ItemUri", rEntityClassification.ItemUri)
                        .Set("LastCategorised", rEntityClassification.LastCategorised)
                        .Set("LastCategorisedGuid", rEntityClassification.LastCategorisedGuid)
                        .Set("LastUpdated", rEntityClassification.LastUpdated)
                        .Set("NextSentenceDate", rEntityClassification.NextSentenceDate)
                        .Set("SourceUri", rEntityClassification.SourceUri)
                        .Set("JsonMatchSummary", rEntityClassification.JsonMatchSummary);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    // Check if a result is available
                    if (result.IsModifiedCountAvailable)
                    {
                        rowsUpdated = result.ModifiedCount;
                    }

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return rowsUpdated;

        }


        public static void InsertRecordClassificationHistory(DataConfig providerConfig, RecordClassificationEntity rEntityClassification, DateTime classificationDate)
        {

            // Add the classificate datetime to the rowkey to differentiate it from other classification history entries
            string rowKey = Utils.CleanTableKey(rEntityClassification.RowKey + "|" + classificationDate.ToUniversalTime().ToString(Utils.ISODateFormat));
            
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordClassification az = new AzureRecordClassification(rEntityClassification);
                    az.RowKey = rowKey;

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.RecordsSentenceHistory);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoRecordClassification> collection = Utils.GetMongoCollection<MongoRecordClassification>(providerConfig, Record.MongoTableNames.RecordsSentenceHistory);

                    MongoRecordClassification mongoObject = Utils.ConvertType<MongoRecordClassification>(rEntityClassification);
                    mongoObject.RowKey = rowKey;
                    collection.InsertOne(mongoObject);

                    //// Create the update filter
                    //List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    //DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(rEntityClassification.PartitionKey), "eq");
                    //string rowKey = Utils.CleanTableKey(rEntityClassification.RowKey + "|" + DateTime.UtcNow.ToString(Utils.ISODateFormat));
                    //DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", rowKey, "eq");
                    //filters.Add(pkFilter);
                    //filters.Add(rkFilter);
                    //FilterDefinition<MongoRecordClassification> filter = Utils.GenerateMongoFilter<MongoRecordClassification>(filters);

                    //var update = Builders<MongoRecordClassification>.Update
                    //    .SetOnInsert("PartitionKey", rEntityClassification.PartitionKey)
                    //    .SetOnInsert("RowKey", rEntityClassification.RowKey)
                    //    .Set("Activity", rEntityClassification.Activity)
                    //    .Set("ClassNo", rEntityClassification.ClassNo)
                    //    .Set("Created", rEntityClassification.Created)
                    //    .Set("ExpiryDate", rEntityClassification.ExpiryDate)
                    //    .Set("Function", rEntityClassification.Function)
                    //    .Set("ItemUri", rEntityClassification.ItemUri)
                    //    .Set("LastCategorised", rEntityClassification.LastCategorised)
                    //    .Set("LastCategorisedGuid", rEntityClassification.LastCategorisedGuid)
                    //    .Set("LastUpdated", rEntityClassification.LastUpdated)
                    //    .Set("NextSentenceDate", rEntityClassification.NextSentenceDate)
                    //    .Set("SourceUri", rEntityClassification.SourceUri);

                    //// Update the batch status
                    //UpdateResult result = collection.UpdateOne(filter, update);
                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;

        }

        public static void DeleteRecordAuthorityMatches(DataConfig providerConfig, POCO.Record record)
        {

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pk);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Get a list of keyphrases to delete
                    List<KeyPhraseToRecordLookup> keyphraseToRecord = GetRecordKeyPhrases(providerConfig, filters);

                    //TODO better way for bulk delete
                    foreach (KeyPhraseToRecordLookup l in keyphraseToRecord)
                    {
                        AzureKeyPhraseToRecordLookup az = new AzureKeyPhraseToRecordLookup(l);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.RecordKeyPhrases);
                        TableOperation operation = TableOperation.Delete(az);

                        Task<TableResult> tDelete = table.ExecuteAsync(operation);
                        tDelete.Wait();

                        // Check for "success with no status" code
                        if (tDelete.Result.HttpStatusCode != 204)
                        {
                            // TODO
                            bool isNotDeleted = true;
                        }

                    }

                    // Get a list of named entities to delete
                    List<NamedEntityToRecordLookup> namedentityToRecord = GetRecordNamedEntitys(providerConfig, filters);

                    //TODO better way for bulk delete
                    foreach (NamedEntityToRecordLookup l in namedentityToRecord)
                    {
                        AzureNamedEntityToRecord az = new AzureNamedEntityToRecord(l);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.RecordNamedEntities);
                        TableOperation operation = TableOperation.Delete(az);

                        Task<TableResult> tDelete = table.ExecuteAsync(operation);
                        tDelete.Wait();

                        // Check for "success with no status" code
                        if (tDelete.Result.HttpStatusCode != 204)
                        {
                            // TODO
                            bool isNotDeleted = true;
                        }

                    }

                    break;

                case "internal.mongodb":
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Delete the key phrase rows                      
                    IMongoCollection<BsonDocument> collectionKP = Utils.GetMongoCollection<BsonDocument>(providerConfig, Record.MongoTableNames.RecordKeyPhrases);
                    DeleteResult resultKP = collectionKP.DeleteMany(filter);

                    // Delete the named entity rows                      
                    IMongoCollection<BsonDocument> collectionNE = Utils.GetMongoCollection<BsonDocument>(providerConfig, Record.MongoTableNames.RecordNamedEntities);
                    DeleteResult resultNE = collectionNE.DeleteMany(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static long DeleteRecordMatchStatus(DataConfig providerConfig, POCO.Record record)
        {
            long rowsDeleted = 0;

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pk);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Get a list of record association match entries to delete
                    List<RecordAuthorityMatchStatus> matchStatus = DataFactory.Record.GetRecordMatchStatus(providerConfig, filters);

                    //TODO better way for bulk delete
                    foreach (RecordAuthorityMatchStatus l in matchStatus)
                    {
                        AzureRecordAuthorityMatchStatus az = new AzureRecordAuthorityMatchStatus(l);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, TableNames.Azure.RecordAssociation.RecordAssociationMatchStatus);
                        TableOperation operation = TableOperation.Delete(az);

                        Task<TableResult> tDelete = table.ExecuteAsync(operation);
                        tDelete.Wait();

                        // Check for "success with no status" code
                        if (tDelete.Result.HttpStatusCode != 204)
                        {
                            // TODO
                            bool isNotDeleted = true;
                        }
                        else
                        {
                            rowsDeleted++;
                        }

                    }

                    break;

                case "internal.mongodb":
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Delete the record association match status rows                      
                    IMongoCollection<BsonDocument> collectionKP = Utils.GetMongoCollection<BsonDocument>(providerConfig, TableNames.Mongo.RecordAssociation.RecordAssociationMatchStatus);
                    DeleteResult resultKP = collectionKP.DeleteMany(filter);
                    rowsDeleted = resultKP.DeletedCount;

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return rowsDeleted;
        }

        public static string AddRecordAuthorityMatchGroups(DataConfig providerConfig, POCO.Record record, List<POCO.RecordAuthorityMatchResult> raMatchesGrouped, DateTime classificationDate)
        {

            string partitionKey = record.PartitionKey;
            string rowKey = Utils.CleanTableKey(record.RowKey + "|" + classificationDate.ToString(Utils.ISODateFormat));

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    foreach(POCO.RecordAuthorityMatchResult result in raMatchesGrouped)
                    {
                        AzureRecordAuthorityMatchResult az = new AzureRecordAuthorityMatchResult(result);
                        az.PartitionKey = partitionKey;
                        az.RowKey = rowKey;

                        CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordsSentenceAuthorityMatch);
                        TableOperation operation = TableOperation.InsertOrMerge(az);
                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }

                    break;

                case "internal.mongodb":
                    foreach (POCO.RecordAuthorityMatchResult result in raMatchesGrouped)
                    {
                        IMongoCollection<MongoRecordAuthorityMatchResult> collection = Utils.GetMongoCollection<MongoRecordAuthorityMatchResult>(providerConfig, MongoTableNames.RecordsSentenceAuthorityMatch);
                        MongoRecordAuthorityMatchResult mongoObject = Utils.ConvertType<MongoRecordAuthorityMatchResult>(result);
                        mongoObject.PartitionKey = partitionKey;
                        mongoObject.RowKey = rowKey;
                        collection.InsertOne(mongoObject);
                    }

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static void DeleteRecord(DataConfig providerConfig, POCO.Record rec)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecord az = new AzureRecord(rec);
                    az.ETag = "*";

                    CloudTable table = Utils.GetCloudTable(providerConfig, Record.AzureTableNames.Record);
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

                    // Create the delete filter
                    List<Filter> deleteFilter = new List<Filter>();
                    Filter pkfilter = new Filter("PartitionKey", rec.PartitionKey, "eq");
                    Filter rkfilter = new Filter("RowKey", rec.RowKey, "eq");
                    deleteFilter.Add(pkfilter);
                    deleteFilter.Add(rkfilter);

                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(deleteFilter);

                    // Delete the row                       
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, Record.MongoTableNames.Record);
                    MongoRecord mongoObject = Utils.ConvertType<MongoRecord>(rec);
                    DeleteResult result = collection.DeleteOne(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;

        }
    }
}
