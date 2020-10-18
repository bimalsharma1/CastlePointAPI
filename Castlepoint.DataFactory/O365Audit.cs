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
    //public class AzureAuditLogEntryImport: EntityAdapter<POCO.O365.AuditLogEntryImport>
    //{
    //    public AzureAuditLogEntryImport() { }
    //    public AzureAuditLogEntryImport(POCO.O365.AuditLogEntryImport o) : base(o) { }
    //    protected override string BuildPartitionKey()
    //    {
    //        return this.Value.PartitionKey;
    //    }

    //    protected override string BuildRowKey()
    //    {
    //        return this.Value.RowKey;
    //    }
    //}
    //public class MongoAuditLogEntryImport: POCO.O365.AuditLogEntryImport
    //{
    //    public ObjectId _id { get; set; }
    //}

    public class AzureAuditContentEntity: EntityAdapter<POCO.O365.AuditContentEntity>
    {
        public AzureAuditContentEntity() { }
        public AzureAuditContentEntity(POCO.O365.AuditContentEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class AzureAuditContentEntityUpdateStatus: EntityAdapter<POCO.O365.AuditContentEntityUpdateStatus>
    {
        public AzureAuditContentEntityUpdateStatus() { }
        public AzureAuditContentEntityUpdateStatus(POCO.O365.AuditContentEntityUpdateStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoAuditContentEntity:POCO.O365.AuditContentEntity
    {
        public ObjectId _id { get; set; }
    }

    public class MongoAuditContentEntityUpdateStatus: POCO.O365.AuditContentEntityUpdateStatus
    {
        public ObjectId _id { get; set; }
    }

    public static class Audit
    {
        private static class AzureTableNames
        {
            internal const string AuditContentFiles = "stlpo365auditcontentfiles";
        }
        private static class MongoTableNames
        {
            internal const string AuditContentFiles = "o365auditcontentfiles";
        }

        public static string UpdateAuditContentFileProcessStatus(DataConfig providerConfig, POCO.O365.AuditContentEntity contentFile)
        {
            switch (providerConfig.ProviderType)
            {
                // AZURE
                case "azure.tableservice":


                    // Create an update object
                    POCO.O365.AuditContentEntityUpdateStatus updateStatus = new AuditContentEntityUpdateStatus();
                    updateStatus.PartitionKey = contentFile.PartitionKey;
                    updateStatus.RowKey = contentFile.RowKey;
                    updateStatus.ProcessStatus = contentFile.ProcessStatus;

                    AzureAuditContentEntityUpdateStatus az = new AzureAuditContentEntityUpdateStatus(updateStatus);
                    az.ETag = "*";

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.AuditContentFiles);

                    TableOperation merge = TableOperation.InsertOrMerge(az);

                    // Execute the insert operation. 
                    Task tResult = table.ExecuteAsync(merge);
                    tResult.Wait();
                    break;

                // MONGO
                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(contentFile.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(contentFile.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoAuditContentEntityUpdateStatus> filter = Utils.GenerateMongoFilter<MongoAuditContentEntityUpdateStatus>(filters);

                    IMongoCollection<MongoAuditContentEntityUpdateStatus> collection = Utils.GetMongoCollection<MongoAuditContentEntityUpdateStatus>(providerConfig, MongoTableNames.AuditContentFiles);

                    var update = Builders<MongoAuditContentEntityUpdateStatus>.Update
                                    .Set("ProcessStatus", contentFile.ProcessStatus);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        //public static bool UpdateContentFileCompleted(POCO.Config.DbConnectionConfig cpConfig, POCO.O365.AuditContentEntity auditContentEntity, string accessToken, ILogger _logger)
        //{
        //    bool isRecordUpdated = false;

        //    // Validate the audit file
        //    if (auditContentEntity == null || auditContentEntity.contentId == null)
        //    {
        //        string logLine = "GetAuditLogs: Audit content entity null, cannot update audit entity";
        //        _logger.LogWarning(logLine);
        //        //Utils.LogErrorToStorageAsync(cpConfig, Guid.NewGuid().ToString(), "Warning", logLine, _logger);
        //        return isRecordUpdated;
        //    }

        //    _logger.LogDebug("GetAuditLogs: Add O365 audit file: " + auditContentEntity.contentId);

        //    auditContentEntity.ProcessStatus = "complete";

        //    try
        //    {
        //        CloudTable table = Utils.GetCloudTable(cpConfig, "stlpo365auditcontentfiles", _logger);

        //        //log.Info("Creating record entity");

        //        // Create the TableOperation that inserts or merges the entry. 
        //        //log.Verbose("Creating table operation");
        //        TableOperation insertMergeOperation = TableOperation.InsertOrMerge(auditContentEntity);

        //        // Execute the insert operation. 
        //        //log.Verbose("Executing table operation");
        //        Task tResult = table.ExecuteAsync(insertMergeOperation);
        //        tResult.Wait();

        //    }
        //    catch (Microsoft.WindowsAzure.Storage.StorageException ex)
        //    {
        //        var requestInformation = ex.RequestInformation;
        //        //Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

        //        // get more details about the exception 
        //        var information = requestInformation.ExtendedErrorInformation;
        //        // if you have aditional information, you can use it for your logs
        //        if (information != null)
        //        {
        //            var errorCode = information.ErrorCode;
        //            var errMessage = string.Format("GetAuditLogs: ({0}) {1}",
        //            errorCode,
        //            information.ErrorMessage);
        //            //    var errDetails = information
        //            //.AdditionalDetails
        //            //.Aggregate("", (s, pair) =>
        //            //{
        //            //    return s + string.Format("{0}={1},", pair.Key, pair.Value);
        //            //});
        //            _logger.LogError(errMessage);
        //        }

        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError("GetAuditLogs: AddO365AuditContent - Error updating entry in table: " + ex.Message);
        //    }

        //    isRecordUpdated = true;

        //    return isRecordUpdated;
        //}

        //public static string AddAuditEntry(DataConfig providerConfig, List<POCO.O365.AuditLogEntryImport> auditLogEntries, ILogger _logger)
        //{
        //    switch (providerConfig.ProviderType)
        //    {
        //        case "azure.tableservice":
        //            CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.AuditContentFiles);

        //            foreach(POCO.O365.AuditLogEntryImport audimp in auditLogEntries)
        //            {
        //                AzureAuditLogEntryImport az = new AzureAuditLogEntryImport(audimp);

        //                TableOperation operation = TableOperation.InsertOrMerge(az);
        //                Task tUpdate = table.ExecuteAsync(operation);
        //                tUpdate.Wait();
        //            }

        //            //TODO return the inserted record id/timestamp
        //            return string.Empty;


        //        case "internal.mongodb":
        //            IMongoCollection<MongoOntology> collection = Utils.GetMongoCollection<MongoOntology>(providerConfig, MongoTableNames.AuditContentFiles);
        //            MongoOntology mongoObject = Utils.ConvertType<MongoOntology>(ontology);
        //            collection.InsertOne(mongoObject);
        //            return string.Empty;

        //        default:
        //            throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
        //    }

        //    return string.Empty;
        //}



        public static string AddAuditContentFile(DataConfig providerConfig, AuditContentEntity auditContentFile, ILogger _logger)
        {
            string partitionKey = Utils.CleanTableKey(auditContentFile.contentId);
            string rowKey = Utils.CleanTableKey(auditContentFile.contentCreated);
            auditContentFile.PartitionKey = partitionKey;
            auditContentFile.RowKey = rowKey;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.AuditContentFiles);

                    AzureAuditContentEntity az = new AzureAuditContentEntity(auditContentFile);

                        TableOperation operation = TableOperation.InsertOrMerge(az);
                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();

                    //TODO return the inserted record id/timestamp
                    return string.Empty;


                case "internal.mongodb":
                    IMongoCollection<MongoAuditContentEntity> collection = Utils.GetMongoCollection<MongoAuditContentEntity>(providerConfig, MongoTableNames.AuditContentFiles);
                    MongoAuditContentEntity mongoObject = Utils.ConvertType<MongoAuditContentEntity>(auditContentFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return string.Empty;
        }

        //private static bool AddO365AuditContent(POCO.Config.DbConnectionConfig cpConfig, POCO.O365.AuditContentEntity auditContentFile, ILogger _logger)
        //{
        //    bool isRecordAdded = false;

        //    _logger.LogDebug("GetAuditLogs: Add O365 audit file: " + auditContentFile.contentId);

        //    // Set the table entity information
        //    string partitionKey = Utils.CleanTableKey(auditContentFile.contentId);
        //    string rowKey = Utils.CleanTableKey(auditContentFile.contentCreated);
        //    auditContentFile.PartitionKey = partitionKey;
        //    auditContentFile.RowKey = rowKey;

        //    try
        //    {
        //        CloudTable table = Utils.GetCloudTable(cpConfig, "stlpo365auditcontentfiles", _logger);

        //        //log.Info("Creating record entity");

        //        // Create the TableOperation that inserts or merges the entry. 
        //        //log.Verbose("Creating table operation");
        //        TableOperation insertMergeOperation = TableOperation.InsertOrMerge(auditContentFile);

        //        // Execute the insert operation. 
        //        //log.Verbose("Executing table operation");
        //        Task tResult = table.ExecuteAsync(insertMergeOperation);
        //        tResult.Wait();

        //    }
        //    catch (Microsoft.WindowsAzure.Storage.StorageException ex)
        //    {
        //        var requestInformation = ex.RequestInformation;
        //        //Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

        //        // get more details about the exception 
        //        var information = requestInformation.ExtendedErrorInformation;
        //        // if you have aditional information, you can use it for your logs
        //        if (information != null)
        //        {
        //            var errorCode = information.ErrorCode;
        //            var errMessage = string.Format("({0}) {1}",
        //            errorCode,
        //            information.ErrorMessage);
        //            //    var errDetails = information
        //            //.AdditionalDetails
        //            //.Aggregate("", (s, pair) =>
        //            //{
        //            //    return s + string.Format("{0}={1},", pair.Key, pair.Value);
        //            //});
        //            _logger.LogError(errMessage);
        //        }

        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError("GetAuditLogs: AddO365AuditContent - Error adding entry to table: " + ex.Message);
        //    }

        //    isRecordAdded = true;

        //    return isRecordAdded;
        //}

        public static List<POCO.O365.AuditContentEntity> GetAuditContentFile(DataConfig providerConfig, POCO.O365.AuditContentEntity auditContentFile)
        {
            List<DataFactory.Filter> filters = new List<Filter>();
            string partitionKey = Utils.CleanTableKey(auditContentFile.contentId);
            string rowKey = Utils.CleanTableKey(auditContentFile.contentCreated);

            DataFactory.Filter pkfilt = new Filter("PartitionKey", partitionKey, "eq");
            filters.Add(pkfilt);
            DataFactory.Filter rkfilt = new Filter("RowKey", rowKey, "eq");
            filters.Add(rkfilt);
            return GetAuditContentFile(providerConfig, filters);


        }
        public static List<POCO.O365.AuditContentEntity> GetAuditContentFile(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.AuditContentEntity> auditContentFiles = new List<POCO.O365.AuditContentEntity>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureAuditContentEntity> azdata = new List<AzureAuditContentEntity>();
                    AzureTableAdaptor<AzureAuditContentEntity> adaptor = new AzureTableAdaptor<AzureAuditContentEntity>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.AuditContentFiles, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        auditContentFiles.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoAuditContentEntity>(providerConfig, MongoTableNames.AuditContentFiles);

                    FilterDefinition<MongoAuditContentEntity> filter = Utils.GenerateMongoFilter<MongoAuditContentEntity>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var logentry in documents)
                    {
                        auditContentFiles.Add(logentry);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return auditContentFiles;
        }

        //private static POCO.O365.AuditContentEntity GetO365AuditContent(POCO.Config.DbConnectionConfig cpConfig, POCO.O365.AuditContentEntity auditContentFile, ILogger _logger)
        //{
        //    List<POCO.O365.AuditContentEntity> o365AuditContents = new List<POCO.O365.AuditContentEntity>();

        //    CloudTable table = Utils.GetCloudTable(cpConfig, "stlpo365auditcontentfiles", _logger);

        //    string partitionKey = Utils.CleanTableKey(auditContentFile.contentId);
        //    string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
        //    string rowKey = Utils.CleanTableKey(auditContentFile.contentCreated);
        //    string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey);

        //    string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

        //    TableQuery<POCO.O365.AuditContentEntity> query = new TableQuery<POCO.O365.AuditContentEntity>().Where(pkFilter);

        //    TableContinuationToken token = null;

        //    var runningQuery = new TableQuery<POCO.O365.AuditContentEntity>()
        //    {
        //        FilterString = query.FilterString,
        //        SelectColumns = query.SelectColumns
        //    };

        //    do
        //    {
        //        runningQuery.TakeCount = query.TakeCount - o365AuditContents.Count;

        //        Task<TableQuerySegment<POCO.O365.AuditContentEntity>> tSeg = table.ExecuteQuerySegmentedAsync<POCO.O365.AuditContentEntity>(runningQuery, token);
        //        tSeg.Wait();
        //        token = tSeg.Result.ContinuationToken;
        //        o365AuditContents.AddRange(tSeg.Result);

        //    } while (token != null && (query.TakeCount == null || o365AuditContents.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

        //    if (o365AuditContents.Count > 0)
        //    {
        //        // Validate the count, warn if not == 1
        //        if (o365AuditContents.Count > 1)
        //        {
        //            string logLine = "GetAuditLogs: Found (" + o365AuditContents.Count.ToString() + "), expected single entry for O365 Audit Content file: " + auditContentFile.contentId;
        //            _logger.LogWarning(logLine);
        //            Utils.LogErrorToStorageAsync(cpConfig, Guid.NewGuid().ToString(), "Warning", logLine, _logger);
        //        }

        //        return o365AuditContents[0];
        //    }
        //    else
        //    {
        //        string logLine = "GetAuditLogs: O365 Audit Content file not found. This is expected if the audit content file hasn't been processed previously: " + auditContentFile.contentId;
        //        _logger.LogDebug(logLine);
        //        return null;
        //    }
        //}

    }
}
