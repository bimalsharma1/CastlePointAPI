using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Castlepoint.DataFactory
{
    public class MongoRecordAuthorityMatch : POCO.RecordAuthorityMatch
    {

    }
    class AzureRecordAuthorityMatch : EntityAdapter<POCO.RecordAuthorityMatch>
    {
        public AzureRecordAuthorityMatch() { }
        public AzureRecordAuthorityMatch(POCO.RecordAuthorityMatch o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class MongoRecordAuthorityFilter : POCO.RecordAuthorityFilter
    {
        public ObjectId _id { get; set; }

    }
    class AzureRecordAuthorityFilter : EntityAdapter<POCO.RecordAuthorityFilter>
    {
        public AzureRecordAuthorityFilter() { }
        public AzureRecordAuthorityFilter(POCO.RecordAuthorityFilter o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class MongoRecordAuthorityKeyPhrase : POCO.RecordAuthorityKeyPhrase
    {
        public ObjectId _id { get; set; }
    }
    class AzureRecordAuthorityKeyPhrase : EntityAdapter<POCO.RecordAuthorityKeyPhrase>
    {
        public AzureRecordAuthorityKeyPhrase() { }
        public AzureRecordAuthorityKeyPhrase(POCO.RecordAuthorityKeyPhrase o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class MongoKeyPhraseToItemLookup : POCO.KeyPhraseToItemLookup
    {

    }
    class AzureKeyPhraseToItemLookup : EntityAdapter<POCO.KeyPhraseToItemLookup>
    {
        public AzureKeyPhraseToItemLookup() { }
        public AzureKeyPhraseToItemLookup(POCO.KeyPhraseToItemLookup o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class MongoKeyPhraseToRecordLookup : POCO.KeyPhraseToRecordLookup
    {
        public ObjectId _id { get; set; }
    }
    class AzureKeyPhraseToRecordLookup : EntityAdapter<POCO.KeyPhraseToRecordLookup>
    {
        public AzureKeyPhraseToRecordLookup() { }
        public AzureKeyPhraseToRecordLookup(POCO.KeyPhraseToRecordLookup o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class MongoRecordAuthorityFunctionActivityEntry : POCO.RecordAuthorityFunctionActivityEntry
    {
        public ObjectId _id { get; set; }
    }
    class AzureRecordAuthorityFunctionActivityEntry : EntityAdapter<POCO.RecordAuthorityFunctionActivityEntry>
    {
        public AzureRecordAuthorityFunctionActivityEntry() { }
        public AzureRecordAuthorityFunctionActivityEntry(POCO.RecordAuthorityFunctionActivityEntry o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public static class RecordAuthority
    {
        internal class AzureTableNames
        {
            internal const string RecordAuthority = "stlprecordauthority";
            internal const string RecordsAssignedRecordsAuthority = "stlprecordsassignedrecordsauthority";
            internal const string SystemsAssignedRecordsAuthority = "stlpsystemsassignedrecordsauthority";
            internal const string RecordAuthorityKeyphrases = "stlprecordauthoritykeyphrases";
            internal const string RecordAssociations = "stlprecordassociations";
            internal const string RecordKeyphrases = "stlprecordkeyphrases";
            internal const string RecordFilenameKeyphrases = "stlprecordfilenamekeyphrases";
            internal const string RecordAuthorityKeyphraseToFile = "stlprecordauthoritykeyphrasetofile";
            internal const string FileToRecordAuthorityKeyphrase = "stlpfiletorecordauthoritykeyphrase";
            internal const string RecordAuthorityFilenameKeyphraseToFile = "stlprecordauthorityfilenamekeyphrasetofile";
            internal const string FileToRecordAuthorityFilenameKeyphrase = "stlpfiletorecordauthorityfilenamekeyphrase";
        }

        internal class MongoTableNames
        {
            internal const string RecordAuthority = "recordauthority";
            internal const string RecordsAssignedRecordsAuthority = "recordsassignedrecordsauthority";
            internal const string SystemsAssignedRecordsAuthority = "systemsassignedrecordsauthority";
            internal const string RecordAuthorityKeyphrases = "recordauthoritykeyphrases";
            internal const string RecordAssociations = "recordassociations";
            internal const string RecordKeyphrases = "recordkeyphrases";
            internal const string RecordFilenameKeyphrases = "recordfilenamekeyphrases";
            internal const string RecordAuthorityKeyphraseToFile = "recordauthoritykeyphrasetofile";
            internal const string FileToRecordAuthorityKeyphrase = "filetorecordauthoritykeyphrase";
            internal const string RecordAuthorityFilenameKeyphraseToFile = "recordauthorityfilenamekeyphrasetofile";
            internal const string FileToRecordAuthorityFilenameKeyphrase = "filetorecordauthorityfilenamekeyphrase";
        }
        public static List<POCO.RecordAuthorityKeyPhrase> GetKeyPhrases(DataConfig providerConfig, List<POCO.RecordAuthorityFilter> raFilters)
        {
            List<POCO.RecordAuthorityKeyPhrase> keyphrases = new List<POCO.RecordAuthorityKeyPhrase>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedAzureFilter = string.Empty;

                    foreach (POCO.RecordAuthorityFilter raFilter in raFilters)
                    {
                        List<Filter> currentFilters = new List<Filter>();
                        if (raFilter.RASchemaUri!=null && raFilter.RASchemaUri != "")
                        {
                            Filter raschemaFilter = new Filter("PartitionKey", raFilter.RASchemaUri, "eq");
                            currentFilters.Add(raschemaFilter);
                        }
                        if (raFilter.Function!=null && raFilter.Function != "")
                        {
                            Filter functionFilter = new Filter("Function", raFilter.Function, "eq");
                            currentFilters.Add(functionFilter);
                        }
                        if (raFilter.Class!=null && raFilter.Class != "")
                        {
                            Filter classFilter = new Filter("Class", raFilter.Class, "eq");
                            currentFilters.Add(classFilter);
                        }

                        // AND the current filters
                        string andFilter = Utils.GenerateAzureFilter(currentFilters);

                        // OR with the combined filter
                        if (combinedAzureFilter == string.Empty) { combinedAzureFilter = andFilter; }
                        else { combinedAzureFilter = TableQuery.CombineFilters(combinedAzureFilter, TableOperators.Or, andFilter); }

                    }
                    List<AzureRecordAuthorityKeyPhrase> azdata = new List<AzureRecordAuthorityKeyPhrase>();
                    AzureTableAdaptor<AzureRecordAuthorityKeyPhrase> adaptor = new AzureTableAdaptor<AzureRecordAuthorityKeyPhrase>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAuthorityKeyphrases, combinedAzureFilter);

                    foreach (var doc in azdata)
                    {
                        keyphrases.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAuthorityKeyPhrase>(providerConfig, MongoTableNames.RecordAuthorityKeyphrases);

                    FilterDefinition<MongoRecordAuthorityKeyPhrase> combinedMongoFilter = null;

                    foreach (POCO.RecordAuthorityFilter raFilter in raFilters)
                    {
                        List<Filter> currentFilters = new List<Filter>();
                        if (raFilter.RASchemaUri != null && raFilter.RASchemaUri != "")
                        {
                            Filter raschemaFilter = new Filter("PartitionKey", raFilter.RASchemaUri, "eq");
                            currentFilters.Add(raschemaFilter);
                        }
                        if (raFilter.Function != null && raFilter.Function != "")
                        {
                            Filter functionFilter = new Filter("Function", raFilter.Function, "eq");
                            currentFilters.Add(functionFilter);
                        }
                        if (raFilter.Class != null && raFilter.Class!= "")
                        {
                            Filter classFilter = new Filter("Class", raFilter.Class, "eq");
                            currentFilters.Add(classFilter);
                        }

                        // AND the current filters
                        FilterDefinition<MongoRecordAuthorityKeyPhrase> andFilter = Utils.GenerateMongoFilter<MongoRecordAuthorityKeyPhrase>(currentFilters);

                        // OR with the combined filter
                        if (combinedMongoFilter==null) { combinedMongoFilter = andFilter; }
                        else { combinedMongoFilter = combinedMongoFilter | andFilter; }

                    }

                    var documents = collection.Find(combinedMongoFilter).ToList();

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

        public static List<POCO.RecordAuthorityFilter> GetAssignedForRecord(DataConfig providerConfig, POCO.Record record)
        {
            List<POCO.RecordAuthorityFilter> assignedrecordauthoritys = new List<POCO.RecordAuthorityFilter>();

            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", record.RowKey, "eq");
            filters.Add(pkFilter);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAuthorityFilter> azdata = new List<AzureRecordAuthorityFilter>();
                    AzureTableAdaptor<AzureRecordAuthorityFilter> adaptor = new AzureTableAdaptor<AzureRecordAuthorityFilter>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordsAssignedRecordsAuthority, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        assignedrecordauthoritys.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAuthorityFilter>(providerConfig, MongoTableNames.RecordsAssignedRecordsAuthority);

                    FilterDefinition<MongoRecordAuthorityFilter> filter = Utils.GenerateMongoFilter<MongoRecordAuthorityFilter>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var assignedra in documents)
                    {
                        assignedrecordauthoritys.Add(assignedra);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return assignedrecordauthoritys;
        }

        public static int AssignToSystem(DataConfig providerConfig, POCO.RecordAuthorityFilter authFilter)
        {
            int numRows = 0;

            // Check if this exists already
            List<POCO.RecordAuthorityFilter> rafilts = GetAuthorityFilters(providerConfig, authFilter);
            if (rafilts.Count>0)
            {
                return numRows;
            }

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureRecordAuthorityFilter az = new AzureRecordAuthorityFilter(authFilter);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SystemsAssignedRecordsAuthority);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    //TODO return the inserted record id/timestamp
                    return numRows;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordAuthorityFilter> collection = Utils.GetMongoCollection<MongoRecordAuthorityFilter>(providerConfig, MongoTableNames.SystemsAssignedRecordsAuthority);
                    MongoRecordAuthorityFilter mongoObject = Utils.ConvertType<MongoRecordAuthorityFilter>(authFilter);
                    collection.InsertOne(mongoObject);
                    return numRows;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return numRows;
        }

        public static List<POCO.RecordAuthorityFilter> GetAuthorityFilters(DataConfig providerConfig, POCO.RecordAuthorityFilter rafilt)
        {
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", rafilt.PartitionKey, "eq");
            filters.Add(pkFilter);
            Filter rkFilter = new Filter("RowKey", rafilt.RowKey, "eq");
            filters.Add(rkFilter);

            return GetAssignedForSystem(providerConfig, filters);
        }

        public static List<POCO.RecordAuthorityFilter> GetAssignedForSystem(DataConfig providerConfig, POCO.System system)
        {
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", system.PartitionKey, "eq");
            filters.Add(pkFilter);

            return GetAssignedForSystem(providerConfig, filters);
        }

        public static List<POCO.RecordAuthorityFilter> GetAssignedForSystem(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.RecordAuthorityFilter> assignedrecordauthoritys = new List<POCO.RecordAuthorityFilter>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAuthorityFilter> azdata = new List<AzureRecordAuthorityFilter>();
                    AzureTableAdaptor<AzureRecordAuthorityFilter> adaptor = new AzureTableAdaptor<AzureRecordAuthorityFilter>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SystemsAssignedRecordsAuthority, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        assignedrecordauthoritys.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAuthorityFilter>(providerConfig, MongoTableNames.SystemsAssignedRecordsAuthority);

                    FilterDefinition<MongoRecordAuthorityFilter> filter = Utils.GenerateMongoFilter<MongoRecordAuthorityFilter>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var assignedra in documents)
                    {
                        assignedrecordauthoritys.Add(assignedra);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return assignedrecordauthoritys;
        }

        public static List<POCO.RecordAuthorityFunctionActivityEntry> GetEntries(DataConfig providerConfig, List<POCO.RecordAuthorityFilter> raFilters)
        {
            List<POCO.RecordAuthorityFunctionActivityEntry> raitems = new List<POCO.RecordAuthorityFunctionActivityEntry>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":


                    string combinedAzureFilter = string.Empty;

                    foreach (POCO.RecordAuthorityFilter raFilter in raFilters)
                    {
                        List<Filter> currentFilters = new List<Filter>();
                        if (raFilter.RASchemaUri!=null && raFilter.RASchemaUri != "")
                        {
                            Filter raschemaFilter = new Filter("PartitionKey", raFilter.RASchemaUri, "eq");
                            currentFilters.Add(raschemaFilter);
                        }
                        if (raFilter.Function!=null && raFilter.Function != "")
                        {
                            Filter functionFilter = new Filter("Function", raFilter.Function, "eq");
                            currentFilters.Add(functionFilter);
                        }
                        if (raFilter.Class!=null && raFilter.Class != "")
                        {
                            Filter classFilter = new Filter("Class", raFilter.Class, "eq");
                            currentFilters.Add(classFilter);
                        }

                        // AND the current filters
                        string andFilter = Utils.GenerateAzureFilter(currentFilters);

                        // OR with the combined filter
                        if (combinedAzureFilter == string.Empty) { combinedAzureFilter = andFilter; }
                        else { combinedAzureFilter = TableQuery.CombineFilters(combinedAzureFilter, TableOperators.Or, andFilter); }

                    }

                    //if (combinedAzureFilter == null || combinedAzureFilter==string.Empty)
                    //{
                    //    throw new InvalidOperationException("Schema filter has not been provided. Cannot retrieve Record Authority information.");
                    //}

                    List<AzureRecordAuthorityFunctionActivityEntry> azdata = new List<AzureRecordAuthorityFunctionActivityEntry>();
                    AzureTableAdaptor<AzureRecordAuthorityFunctionActivityEntry> adaptor = new AzureTableAdaptor<AzureRecordAuthorityFunctionActivityEntry>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.RecordAuthority, combinedAzureFilter);

                    foreach (var doc in azdata)
                    {
                        raitems.Add(doc.Value);
                    }

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRecordAuthorityFunctionActivityEntry>(providerConfig, MongoTableNames.RecordAuthority);

                    FilterDefinition<MongoRecordAuthorityFunctionActivityEntry> combinedFilter = null;
                    List<Filter> filters = new List<Filter>();
                    foreach(POCO.RecordAuthorityFilter raf in raFilters)
                    {
                        List<Filter> thisFilter = new List<Filter>();
                        if (!string.IsNullOrEmpty(raf.RASchemaUri))
                        {
                            Filter fschema = new Filter("PartitionKey", raf.RASchemaUri, "eq");
                            thisFilter.Add(fschema);
                        }
                        if (!string.IsNullOrEmpty(raf.Function))
                        {
                            Filter ffunction = new Filter("Function", raf.Function, "eq");
                            thisFilter.Add(ffunction);
                        }
                        if (!string.IsNullOrEmpty(raf.Class))
                        {
                            Filter fclass = new Filter("Class", raf.Class, "eq");
                            thisFilter.Add(fclass);
                        }

                        // AND the filters
                        FilterDefinition<MongoRecordAuthorityFunctionActivityEntry> andFilter = Utils.GenerateMongoFilter<MongoRecordAuthorityFunctionActivityEntry>(thisFilter);

                        // OR with the combined filter
                        if (combinedFilter==null) { combinedFilter = andFilter; }
                        else { combinedFilter = combinedFilter | andFilter; }
                    }

                    if (combinedFilter == null)
                    {
                        // Set to blank filter
                        combinedFilter = Utils.GenerateMongoFilter<MongoRecordAuthorityFunctionActivityEntry>(filters);
                        //throw new InvalidOperationException("Schema filter has not been provided. Cannot retrieve Record Authority information.");
                    }

                    var documents = collection.Find(combinedFilter).ToList();

                    foreach (var raitem in documents)
                    {
                        raitems.Add(raitem);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return raitems;
        }


        public static string UpsertKeyPhraseToItemLookup(DataConfig providerConfig, POCO.KeyPhraseToItemLookup itemLookup, string locationInFile)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch(locationInFile)
                    {
                        case "body":
                            tableName = AzureTableNames.RecordAuthorityKeyphraseToFile;
                            break;
                        case "filename":
                            tableName = AzureTableNames.RecordAuthorityFilenameKeyphraseToFile;
                            break;
                        default:
                            throw new ApplicationException("UpsertKeyPhraseToItemLookup: location of keyphrase in file is invalid.");
                    }

                    AzureKeyPhraseToItemLookup az = new AzureKeyPhraseToItemLookup(itemLookup);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    return string.Empty;

                case "internal.mongodb":

                    switch (locationInFile)
                    {
                        case "body":
                            tableName = MongoTableNames.RecordAuthorityKeyphraseToFile;
                            break;
                        case "filename":
                            tableName = MongoTableNames.RecordAuthorityFilenameKeyphraseToFile;
                            break;
                        default:
                            throw new ApplicationException("UpsertKeyPhraseToItemLookup: location of keyphrase in file is invalid.");
                    }

                    IMongoCollection<MongoKeyPhraseToItemLookup> collection = Utils.GetMongoCollection<MongoKeyPhraseToItemLookup>(providerConfig, tableName);
                        MongoKeyPhraseToItemLookup mongoObject = Utils.ConvertType<MongoKeyPhraseToItemLookup>(itemLookup);

                        // Create the upsert filter
                        List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", itemLookup.PartitionKey, "eq");
                        DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", itemLookup.RowKey, "eq");
                        filters.Add(pkFilter);
                        filters.Add(rkFilter);
                        FilterDefinition<MongoKeyPhraseToItemLookup> filter = Utils.GenerateMongoFilter<MongoKeyPhraseToItemLookup>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);

                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

        }

        public static string UpsertKeyPhraseToRecordLookup(DataConfig providerConfig, POCO.KeyPhraseToRecordLookup recordLookup, string locationInFile)
        {

            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch (locationInFile)
                    {
                        case "body":
                            tableName = AzureTableNames.RecordKeyphrases;
                            break;
                        case "filename":
                            tableName = AzureTableNames.RecordFilenameKeyphrases;
                            break;
                        default:
                            throw new ApplicationException("UpsertKeyPhraseToItemLookup: location of keyphrase in file is invalid.");
                    }

                    AzureKeyPhraseToRecordLookup az = new AzureKeyPhraseToRecordLookup(recordLookup);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    return string.Empty;

                case "internal.mongodb":
                    switch (locationInFile)
                    {
                        case "body":
                            tableName = MongoTableNames.RecordKeyphrases;
                            break;
                        case "filename":
                            tableName = MongoTableNames.RecordFilenameKeyphrases;
                            break;
                        default:
                            throw new ApplicationException("UpsertKeyPhraseToItemLookup: location of keyphrase in file is invalid.");
                    }

                    IMongoCollection<MongoKeyPhraseToRecordLookup> collection = Utils.GetMongoCollection<MongoKeyPhraseToRecordLookup>(providerConfig, tableName);
                    MongoKeyPhraseToRecordLookup mongoObject = Utils.ConvertType<MongoKeyPhraseToRecordLookup>(recordLookup);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", recordLookup.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", recordLookup.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoKeyPhraseToRecordLookup> filter = Utils.GenerateMongoFilter<MongoKeyPhraseToRecordLookup>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);

                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

        }

        public static string UpdateRecordAuthorityMatch(DataConfig providerConfig, POCO.RecordAuthorityMatch raMatch)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordAuthorityMatch az = new AzureRecordAuthorityMatch(raMatch);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociations);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordAuthorityMatch> collection = Utils.GetMongoCollection<MongoRecordAuthorityMatch>(providerConfig, MongoTableNames.RecordAssociations );
                    MongoRecordAuthorityMatch mongoObject = Utils.ConvertType<MongoRecordAuthorityMatch>(raMatch);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(raMatch.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(raMatch.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoRecordAuthorityMatch> filter = Utils.GenerateMongoFilter<MongoRecordAuthorityMatch>(filters);

                    // Replace current document
                    collection.ReplaceOne(filter, mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static long DeleteKeyPhraseToFile(DataConfig providerConfig, List<Filter> filters)
        {
            // Validate the filter
            if (filters==null ||  filters.Count==0)
            {
                throw new InvalidOperationException("No filters were provided. Cannot delete data without a valid filter.");
            }

            long numEntriesDeleted = 0;

            // Check the data provider to use
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileToRecordAuthorityKeyphrase);

                    TableQuery<TableEntity> query = new TableQuery<TableEntity>().Where(combinedFilter);

                    List<TableEntity> recordsToDelete = new List<TableEntity>();

                    TableContinuationToken token = null;

                    var runningQuery = new TableQuery<TableEntity>()
                    {
                        FilterString = query.FilterString,
                        SelectColumns = query.SelectColumns
                    };

                    do
                    {
                        runningQuery.TakeCount = query.TakeCount - recordsToDelete.Count;

                        // Add the entries to the recordsToDelete list
                        Task<TableQuerySegment<TableEntity>> tSeg = table.ExecuteQuerySegmentedAsync<TableEntity>(runningQuery, token);
                        tSeg.Wait();
                        token = tSeg.Result.ContinuationToken;
                        recordsToDelete.AddRange(tSeg.Result);

                        // Create a batch of records to delete
                        TableBatchOperation batch = new TableBatchOperation();
                        foreach (TableEntity entity in recordsToDelete)
                        {
                            batch.Add(TableOperation.Delete(entity));

                            if (batch.Count == 100)
                            {
                                numEntriesDeleted += batch.Count;
                                Task tBatchDelete = table.ExecuteBatchAsync(batch);
                                tBatchDelete.Wait();
                                batch = new TableBatchOperation();
                            }

                        }
                        if (batch.Count > 0)
                        {
                            numEntriesDeleted += batch.Count;
                            Task tBatchDelete = table.ExecuteBatchAsync(batch);
                            tBatchDelete.Wait();
                        }

                    } while (token != null && (query.TakeCount == null || recordsToDelete.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

                    break;

                case "internal.mongodb":
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.FileToRecordAuthorityKeyphrase);

                    // Create the delete filter
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Replace current document
                    DeleteResult result = collection.DeleteMany(filter);

                    numEntriesDeleted = result.DeletedCount;
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return numEntriesDeleted;
        }
    }
}
