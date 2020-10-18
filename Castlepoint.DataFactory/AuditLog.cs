using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Logging;
using Castlepoint.POCO.O365;

namespace Castlepoint.DataFactory.O365
{
    class MongoAuditLogEntryFactorsUpdate:POCO.O365.AuditLogEntryFactorsUpdate
    { 
        public ObjectId _id { get; set; }
    }

    class AzureAuditLogEntryFactorsUpdate : EntityAdapter<POCO.O365.AuditLogEntryFactorsUpdate>
    {
        public AzureAuditLogEntryFactorsUpdate() { }
        public AzureAuditLogEntryFactorsUpdate(POCO.O365.AuditLogEntryFactorsUpdate o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoAuditLogEntry :POCO.O365.AuditLogEntry
    {
        public ObjectId _id { get; set; }
    }

    class AzureAuditLogEntry:EntityAdapter<POCO.O365.AuditLogEntry>
    {
        public AzureAuditLogEntry() { }
        public AzureAuditLogEntry(POCO.O365.AuditLogEntry o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class MongoAuditLogEntryv1 : POCO.O365.AuditLogEntryv1
    {
        public ObjectId _id { get; set; }
    }

    class AzureAuditLogEntryv1 : EntityAdapter<POCO.O365.AuditLogEntryv1>
    {
        public AzureAuditLogEntryv1() { }
        public AzureAuditLogEntryv1(POCO.O365.AuditLogEntryv1 o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class AuditLog
    {
        internal class AzureTableNames
        {
            internal const string O365AuditLogEntry = "stlpo365spoauditlogentry";
            internal const string O365AuditLogEntryActionable = "stlpo365spoauditlogentryactionanableevents";
            internal const string O365AuditLogEntryActionableByUser = "stlpo365spoauditlogentryactionanableeventsbyuser";
            internal const string O365AuditLogEntryActionableByDay = "stlpo365spoauditlogentryactionanableeventsbyday";
        }

        internal class MongoTableNames
        {
            internal const string O365AuditLogEntry = "o365spoauditlogentry";
            internal const string O365AuditLogEntryActionable = "o365spoauditlogentryactionanableevents";
            internal const string O365AuditLogEntryActionableByUser = "o365spoauditlogentryactionanableeventsbyuser";
            internal const string O365AuditLogEntryActionableByDay = "o365spoauditlogentryactionanableeventsbyday";
        }

        public static string AddLogEntry(DataConfig providerConfig, POCO.O365.AuditLogEntry logEntry)
        {
            DateTime logEntryDate;
            bool isDateValid = DateTime.TryParse(logEntry.CreationTime, out logEntryDate);

            string tableSuffix = logEntryDate.ToString(Utils.TableSuffixDateFormatYM);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureAuditLogEntry az = new AzureAuditLogEntry(logEntry);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.O365AuditLogEntry + tableSuffix);

                    TableOperation insertReplace = TableOperation.InsertOrReplace(az);

                    // Execute the insert operation. 
                    Task tResult = table.ExecuteAsync(insertReplace);
                    tResult.Wait();
                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoAuditLogEntry> collection = Utils.GetMongoCollection<MongoAuditLogEntry>(providerConfig, MongoTableNames.O365AuditLogEntry + tableSuffix);
                    MongoAuditLogEntry mongoObject = Utils.ConvertType<MongoAuditLogEntry>(logEntry);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.O365.AuditLogEntry> GetActionableEvents(DataConfig providerConfig, string byType, List<DataFactory.Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            List<POCO.O365.AuditLogEntry> auditEntries = new List<POCO.O365.AuditLogEntry>();

            // Default next page id to "no more data" (empty string)
            nextPageId = string.Empty;

            //List<DataFactory.Filter> filters = new List<Filter>();
            //DataFactory.Filter f = new Filter("Operation", "FileDeleted", "eq");
            //filters.Add(f);
            //f = new Filter("Operation", "FileRestored", "eq");
            //filters.Add(f);
            //f = new Filter("Operation", "FileRenamed", "eq");
            //filters.Add(f);
            //f = new Filter("Operation", "FileMoved", "eq");
            //filters.Add(f);

            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch (byType)
                    {
                        case "byuser":
                            tableName = AzureTableNames.O365AuditLogEntryActionableByUser;
                            break;
                        case "bydate":
                            tableName = AzureTableNames.O365AuditLogEntryActionableByDay;
                            break;
                        case "byfile":
                            tableName = AzureTableNames.O365AuditLogEntryActionable;
                            break;
                        default:
                            // Default to File
                            tableName = AzureTableNames.O365AuditLogEntryActionable;
                            break;
                    }

                    string combinedFilter = Utils.GenerateAzureFilter(filters);
                    TableContinuationToken thisPageToken = null;
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        thisPageToken = Newtonsoft.Json.JsonConvert.DeserializeObject<TableContinuationToken>(thisPageId);
                    }
                    TableContinuationToken nextPageToken = null;
                    List<AzureAuditLogEntry> azdata = new List<AzureAuditLogEntry>();
                    AzureTableAdaptor<AzureAuditLogEntry> adaptor = new AzureTableAdaptor<AzureAuditLogEntry>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, tableName, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

                    foreach (var doc in azdata)
                    {
                        auditEntries.Add(doc.Value);
                    }

                    // Check if there is a next page token available
                    if (nextPageToken != null)
                    {
                        nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                    }

                    break;
                case "internal.mongodb":

                    switch (byType)
                    {
                        case "byuser":
                            tableName = MongoTableNames.O365AuditLogEntryActionableByUser;
                            break;
                        case "bydate":
                            tableName = MongoTableNames.O365AuditLogEntryActionableByDay;
                            break;
                        case "byfile":
                            tableName = MongoTableNames.O365AuditLogEntryActionable;
                            break;
                        default:
                            tableName = MongoTableNames.O365AuditLogEntryActionable;
                            break;
                    }

                    var collection = Utils.GetMongoCollection<MongoAuditLogEntry>(providerConfig, tableName);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoAuditLogEntry> filter = Utils.GenerateMongoFilter<MongoAuditLogEntry>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();

                    foreach (var logentry in documents)
                    {
                        auditEntries.Add(logentry);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return auditEntries;
        }

        public static List<POCO.O365.AuditLogEntry> GetAuditEvents(DataConfig providerConfig, string tableName, List<DataFactory.Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            List<POCO.O365.AuditLogEntry> auditEntries = new List<POCO.O365.AuditLogEntry>();

            // Default next page id to "no more data" (empty string)
            nextPageId = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Prefix with stlp
                    tableName = "stlp" + tableName;

                    string combinedFilter = Utils.GenerateAzureFilter(filters);
                    TableContinuationToken thisPageToken = null;
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        thisPageToken = Newtonsoft.Json.JsonConvert.DeserializeObject<TableContinuationToken>(thisPageId);
                    }
                    TableContinuationToken nextPageToken = null;
                    List<AzureAuditLogEntry> azdata = new List<AzureAuditLogEntry>();
                    AzureTableAdaptor<AzureAuditLogEntry> adaptor = new AzureTableAdaptor<AzureAuditLogEntry>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, tableName, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

                    foreach (var doc in azdata)
                    {
                        auditEntries.Add(doc.Value);
                    }

                    // Check if there is a next page token available
                    if (nextPageToken != null)
                    {
                        nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                    }

                    break;
                case "internal.mongodb":

                    var collection = Utils.GetMongoCollection<MongoAuditLogEntry>(providerConfig, tableName);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoAuditLogEntry> filter = Utils.GenerateMongoFilter<MongoAuditLogEntry>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();

                    foreach (var logentry in documents)
                    {
                        auditEntries.Add(logentry);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return auditEntries;
        }

        public static List<POCO.O365.AuditLogEntryv1> GetAuditEventsv1(DataConfig providerConfig, string tableName, List<DataFactory.Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            List<POCO.O365.AuditLogEntryv1> auditEntries = new List<POCO.O365.AuditLogEntryv1>();

            // Default next page id to "no more data" (empty string)
            nextPageId = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Prefix with stlp
                    tableName = "stlp" + tableName;

                    string combinedFilter = Utils.GenerateAzureFilter(filters);
                    TableContinuationToken thisPageToken = null;
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        thisPageToken = Newtonsoft.Json.JsonConvert.DeserializeObject<TableContinuationToken>(thisPageId);
                    }
                    TableContinuationToken nextPageToken = null;
                    List<AzureAuditLogEntryv1> azdata = new List<AzureAuditLogEntryv1>();
                    AzureTableAdaptor<AzureAuditLogEntryv1> adaptor = new AzureTableAdaptor<AzureAuditLogEntryv1>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, tableName, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

                    foreach (var doc in azdata)
                    {
                        auditEntries.Add(doc.Value);
                    }

                    // Check if there is a next page token available
                    if (nextPageToken != null)
                    {
                        nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                    }

                    break;
                case "internal.mongodb":

                    var collection = Utils.GetMongoCollection<MongoAuditLogEntryv1>(providerConfig, tableName);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoAuditLogEntryv1> filter = Utils.GenerateMongoFilter<MongoAuditLogEntryv1>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();

                    foreach (var logentry in documents)
                    {
                        auditEntries.Add(logentry);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return auditEntries;
        }

        public static List<POCO.O365.AuditLogEntry> GetForItem(DataConfig providerConfig, List<Filter> filters, string tableSuffix)
        {
            List<POCO.O365.AuditLogEntry> auditEntries = new List<POCO.O365.AuditLogEntry>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureAuditLogEntry> azdata = new List<AzureAuditLogEntry>();
                    AzureTableAdaptor<AzureAuditLogEntry> adaptor = new AzureTableAdaptor<AzureAuditLogEntry>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.O365AuditLogEntry + tableSuffix, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        auditEntries.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoAuditLogEntry>(providerConfig, MongoTableNames.O365AuditLogEntry + "201903");

                    FilterDefinition<MongoAuditLogEntry> filter = Utils.GenerateMongoFilter<MongoAuditLogEntry>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var logentry in documents)
                    {
                        auditEntries.Add(logentry);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return auditEntries;
        }

        public static List<POCO.O365.AuditLogEntry> GetActionableEventsForItem(DataConfig providerConfig, List<Filter> filters, string tableSuffix)
        {
            List<POCO.O365.AuditLogEntry> auditEntries = GetForItem(providerConfig, filters, tableSuffix);

            // Return only audit entries that are actionable
            int totalEntries = auditEntries.Count - 1;
            for (int i = totalEntries; i >= 0; i--)
            {
                switch (auditEntries[i].Operation.ToLower())
                {
                    case "filedeleted":
                        {
                            // Keep this record
                            break;
                        }
                    case "filerestored":
                        {
                            // Keep this record
                            break;
                        }
                    default:
                        {
                            // Remove the entry
                            auditEntries.RemoveAt(i);
                            break;
                        }
                }
            }

            return auditEntries;
        }

        public static string AddAuditableEventLogEntry(DataConfig providerConfig, AuditLogEntry logEntry, string auditLookupType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    switch (auditLookupType)
                    {
                        case "byuser":
                            {
                                tableName = AzureTableNames.O365AuditLogEntryActionableByUser;
                                break;
                            }
                        case "byday":
                            {
                                tableName = AzureTableNames.O365AuditLogEntryActionableByDay;
                                break;
                            }
                        default:
                            {
                                tableName = AzureTableNames.O365AuditLogEntryActionable;
                                break;
                            }
                    }

                    AzureAuditLogEntry az = new AzureAuditLogEntry(logEntry);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);

                    TableOperation insertReplace = TableOperation.InsertOrReplace(az);

                    // Execute the insert operation. 
                    Task tResult = table.ExecuteAsync(insertReplace);
                    tResult.Wait();
                    break;

                case "internal.mongodb":

                    switch (auditLookupType)
                    {
                        case "byuser":
                            {
                                tableName = MongoTableNames.O365AuditLogEntryActionableByUser;
                                break;
                            }
                        case "byday":
                            {
                                tableName = MongoTableNames.O365AuditLogEntryActionableByDay;
                                break;
                            }
                        default:
                            {
                                tableName = MongoTableNames.O365AuditLogEntryActionable;
                                break;
                            }
                    }

                    IMongoCollection<MongoAuditLogEntry> collection = Utils.GetMongoCollection<MongoAuditLogEntry>(providerConfig, tableName);
                    MongoAuditLogEntry mongoObject = Utils.ConvertType<MongoAuditLogEntry>(logEntry);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string UpdateAuditableEventFactors(DataConfig providerConfig, AuditLogEntry logEntry, string factors, string auditLookupType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                // AZURE
                case "azure.tableservice":
                    switch (auditLookupType)
                    {
                        case "byuser":
                            {
                                tableName = AzureTableNames.O365AuditLogEntryActionableByUser;
                                break;
                            }
                        case "byday":
                            {
                                tableName = AzureTableNames.O365AuditLogEntryActionableByDay;
                                break;
                            }
                        default:
                            {
                                tableName = AzureTableNames.O365AuditLogEntryActionable;
                                break;
                            }
                    }

                    // Create an factors update object matching our LogEntry
                    POCO.O365.AuditLogEntryFactorsUpdate factorsObject = new AuditLogEntryFactorsUpdate();
                    factorsObject.PartitionKey = logEntry.PartitionKey;
                    factorsObject.RowKey = logEntry.RowKey;
                    factorsObject.Factors = factors;

                    AzureAuditLogEntryFactorsUpdate az = new AzureAuditLogEntryFactorsUpdate(factorsObject);
                    az.ETag = "*";

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);

                    TableOperation merge = TableOperation.InsertOrMerge(az);

                    // Execute the insert operation. 
                    Task tResult = table.ExecuteAsync(merge);
                    tResult.Wait();
                    break;

                // MONGO
                case "internal.mongodb":

                    switch (auditLookupType)
                    {
                        case "byuser":
                            {
                                tableName = MongoTableNames.O365AuditLogEntryActionableByUser;
                                break;
                            }
                        case "byday":
                            {
                                tableName = MongoTableNames.O365AuditLogEntryActionableByDay;
                                break;
                            }
                        default:
                            {
                                tableName = MongoTableNames.O365AuditLogEntryActionable;
                                break;
                            }
                    }

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(logEntry.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(logEntry.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoAuditLogEntryFactorsUpdate> filter = Utils.GenerateMongoFilter<MongoAuditLogEntryFactorsUpdate>(filters);

                    IMongoCollection<MongoAuditLogEntryFactorsUpdate> collection = Utils.GetMongoCollection<MongoAuditLogEntryFactorsUpdate>(providerConfig, tableName);

                    var update = Builders<MongoAuditLogEntryFactorsUpdate>.Update
                                    .Set("Factors", logEntry.Factors);


                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static void DeleteLogEntry(DataConfig providerConfig, string auditTableName, AuditLogEntry entry)
        {

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", entry.PartitionKey, "eq");
            filters.Add(pk);
            Filter rk = new Filter("RowKey", entry.RowKey, "eq");
            filters.Add(rk);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    auditTableName = "stlp" + auditTableName;

                        AzureAuditLogEntry az = new AzureAuditLogEntry(entry);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, auditTableName);
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
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, auditTableName);
                    DeleteResult result = collection.DeleteMany(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

    }
}
