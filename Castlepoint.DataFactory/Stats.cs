using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Castlepoint.DataFactory
{
    class MongoStat : POCO.Stat
    {
        public ObjectId _id { get; }
    }
    class MongoStatUpsert : POCO.Stat
    {
        //public ObjectId _id { get; }
    }
    class AzureStat : EntityAdapter<POCO.Stat>
    {
        public AzureStat() { }
        public AzureStat(POCO.Stat o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoRecordStat : POCO.RecordStat
    {
        public ObjectId _id { get; }
    }

    public static class Stats
    {
        internal static class AzureTableNames
        {
            internal const string CPStats = "cpstats";
            internal const string FileTypeStats = "stlpsystemfiletypestats";
            internal const string FileSizeStats = "stlpsystemfilesizestats";
            internal const string FileProcessStats = "stlpsystemfileprocessstats";
            internal const string FileMetadataStats = "stlpsystemfilemetadatastats";
        }
        internal static class MongoTableNames
        {
            internal const string CPStats = "cpstats";
            internal const string FileTypeStats = "systemfiletypestats";
            internal const string FileSizeStats = "systemfilesizestats";
            internal const string FileProcessStats = "systemfileprocessstats";
            internal const string FileMetadataStats = "systemfilemetadatastats";
        }

        public static bool AddFileTypeStats(DataConfig providerConfig, List<POCO.Stat> fileTypeStats)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileTypeStats);
                    foreach (POCO.Stat stat in fileTypeStats)
                    {
                        AzureStat az = new AzureStat(stat);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoStatUpsert> collection = Utils.GetMongoCollection<MongoStatUpsert>(providerConfig, MongoTableNames.FileTypeStats);

                    var operationList = new List<WriteModel<MongoStatUpsert>>();
                    foreach (POCO.Stat stat in fileTypeStats)
                    {
                        // Convert to mongo-compatible object
                        MongoStatUpsert mongoObject = Utils.ConvertType<MongoStatUpsert>(stat);

                        // Create the filter for the upsert
                        FilterDefinition<MongoStatUpsert> filter = Builders<MongoStatUpsert>.Filter.Eq(x => x.PartitionKey, stat.PartitionKey) &
                            Builders<MongoStatUpsert>.Filter.Eq(x => x.RowKey, stat.RowKey);

                        UpdateDefinition<MongoStatUpsert> updateDefinition = new UpdateDefinitionBuilder<MongoStatUpsert>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("StatsType", mongoObject.StatsType)
                            .Set("JsonStats", mongoObject.JsonStats);

                        UpdateOneModel<MongoStatUpsert> update = new UpdateOneModel<MongoStatUpsert>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);
                        operationList.Add(update);

                    }

                    if (operationList.Count > 0)
                    {
                        collection.BulkWrite(operationList);
                    }

                    return true;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return true;
        }

        public static bool AddFileSizeStats(DataConfig providerConfig, List<POCO.Stat> fileSizeStats)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileSizeStats);
                    foreach (POCO.Stat stat in fileSizeStats)
                    {
                        AzureStat az = new AzureStat(stat);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoStatUpsert> collection = Utils.GetMongoCollection<MongoStatUpsert>(providerConfig, MongoTableNames.FileSizeStats);

                    var operationList = new List<WriteModel<MongoStatUpsert>>();
                    foreach (POCO.Stat stat in fileSizeStats)
                    {
                        // Convert to mongo-compatible object
                        MongoStatUpsert mongoObject = Utils.ConvertType<MongoStatUpsert>(stat);

                        // Create the filter for the upsert
                        FilterDefinition<MongoStatUpsert> filter = Builders<MongoStatUpsert>.Filter.Eq(x => x.PartitionKey, stat.PartitionKey) &
                            Builders<MongoStatUpsert>.Filter.Eq(x => x.RowKey, stat.RowKey);

                        UpdateDefinition<MongoStatUpsert> updateDefinition = new UpdateDefinitionBuilder<MongoStatUpsert>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("StatsType", mongoObject.StatsType)
                            .Set("JsonStats", mongoObject.JsonStats);

                        UpdateOneModel<MongoStatUpsert> update = new UpdateOneModel<MongoStatUpsert>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);
                        operationList.Add(update);

                    }

                    if (operationList.Count > 0)
                    {
                        collection.BulkWrite(operationList);
                    }

                    return true;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return true;
        }

        public static bool AddFileProcessStats(DataConfig providerConfig, List<POCO.Stat> fileProcessStats)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileProcessStats);
                    foreach (POCO.Stat stat in fileProcessStats)
                    {
                        AzureStat az = new AzureStat(stat);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoStatUpsert> collection = Utils.GetMongoCollection<MongoStatUpsert>(providerConfig, MongoTableNames.FileProcessStats);

                    var operationList = new List<WriteModel<MongoStatUpsert>>();
                    foreach (POCO.Stat stat in fileProcessStats)
                    {
                        // Convert to mongo-compatible object
                        MongoStatUpsert mongoObject = Utils.ConvertType<MongoStatUpsert>(stat);

                        // Create the filter for the upsert
                        FilterDefinition<MongoStatUpsert> filter = Builders<MongoStatUpsert>.Filter.Eq(x => x.PartitionKey, stat.PartitionKey) &
                            Builders<MongoStatUpsert>.Filter.Eq(x => x.RowKey, stat.RowKey);

                        UpdateDefinition<MongoStatUpsert> updateDefinition = new UpdateDefinitionBuilder<MongoStatUpsert>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("StatsType", mongoObject.StatsType)
                            .Set("JsonStats", mongoObject.JsonStats);

                        UpdateOneModel<MongoStatUpsert> update = new UpdateOneModel<MongoStatUpsert>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);
                        operationList.Add(update);

                    }

                    if (operationList.Count > 0)
                    {
                        collection.BulkWrite(operationList);
                    }

                    return true;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return true;
        }

        public static bool AddFileMetadataStats(DataConfig providerConfig, List<POCO.Stat> fileMetadataStats)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    List<TableOperation> ops = new List<TableOperation>();

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileMetadataStats);
                    foreach (POCO.Stat stat in fileMetadataStats)
                    {
                        AzureStat az = new AzureStat(stat);
                        TableOperation operation = TableOperation.InsertOrReplace(az);
                        ops.Add(operation);
                    }

                    Utils.AzureBatchExecute(table, ops);

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoStatUpsert> collection = Utils.GetMongoCollection<MongoStatUpsert>(providerConfig, MongoTableNames.FileMetadataStats);

                    var operationList = new List<WriteModel<MongoStatUpsert>>();
                    foreach (POCO.Stat stat in fileMetadataStats)
                    {
                        // Convert to mongo-compatible object
                        MongoStatUpsert mongoObject = Utils.ConvertType<MongoStatUpsert>(stat);

                        // Create the filter for the upsert
                        FilterDefinition<MongoStatUpsert> filter = Builders<MongoStatUpsert>.Filter.Eq(x => x.PartitionKey, stat.PartitionKey) &
                            Builders<MongoStatUpsert>.Filter.Eq(x => x.RowKey, stat.RowKey);

                        UpdateDefinition<MongoStatUpsert> updateDefinition = new UpdateDefinitionBuilder<MongoStatUpsert>().Unset("______"); // HACK: I found no other way to create an empty update definition

                        updateDefinition = updateDefinition.SetOnInsert("PartitionKey", mongoObject.PartitionKey)
                            .SetOnInsert("RowKey", mongoObject.RowKey)
                            .Set("StatsType", mongoObject.StatsType)
                            .Set("JsonStats", mongoObject.JsonStats);

                        UpdateOneModel<MongoStatUpsert> update = new UpdateOneModel<MongoStatUpsert>(filter, updateDefinition) { IsUpsert = true };
                        operationList.Add(update);
                        operationList.Add(update);

                    }

                    if (operationList.Count > 0)
                    {
                        collection.BulkWrite(operationList);
                    }

                    return true;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return true;
        }
        public static string UpsertRecordStat(DataConfig providerConfig, POCO.RecordStat stat)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureRecordStat az = new AzureRecordStat(stat);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.CPStats);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordStat> collection = Utils.GetMongoCollection<MongoRecordStat>(providerConfig, MongoTableNames.CPStats);
                    MongoRecordStat mongoObject = Utils.ConvertType<MongoRecordStat>(stat);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(stat.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(stat.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoRecordStat> filter = Utils.GenerateMongoFilter<MongoRecordStat>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string UpsertStat(DataConfig providerConfig, POCO.Stat stat)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureStat az = new AzureStat(stat);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.CPStats);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoStatUpsert> collection = Utils.GetMongoCollection<MongoStatUpsert>(providerConfig, MongoTableNames.CPStats);
                    MongoStatUpsert mongoObject = Utils.ConvertType<MongoStatUpsert>(stat);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(stat.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(stat.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoStatUpsert> filter = Utils.GenerateMongoFilter<MongoStatUpsert>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static long DeleteFileTypeStats(DataConfig providerConfig, POCO.System system)
        {

            long numEntriesDeleted = 0;

            string systemKey = system.PartitionKey;
            if (!systemKey.EndsWith("|"))
            {
                systemKey += "|";
            }

            // Create a filter for the system
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", systemKey, "eq");
            filters.Add(pkfilt);

            // Check the data provider to use
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileTypeStats);

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
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.FileTypeStats);

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

        public static long DeleteFileMetadataStats(DataConfig providerConfig, POCO.System system)
        {

            long numEntriesDeleted = 0;

            string systemKey = system.PartitionKey;
            if (!systemKey.EndsWith("|"))
            {
                systemKey += "|";
            }

            // Create a filter for the system
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", systemKey, "eq");
            filters.Add(pkfilt);

            // Check the data provider to use
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileMetadataStats);

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
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.FileMetadataStats);

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

        public static long DeleteFileSizeStats(DataConfig providerConfig, POCO.System system)
        {

            long numEntriesDeleted = 0;

            string systemKey = system.PartitionKey;
            if (!systemKey.EndsWith("|"))
            {
                systemKey += "|";
            }

            // Create a filter for the system
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", systemKey, "eq");
            filters.Add(pkfilt);

            // Check the data provider to use
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileSizeStats);

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
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.FileSizeStats);

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

        public static long DeleteMetadataStats(DataConfig providerConfig, POCO.System system)
        {

            long numEntriesDeleted = 0;

            string systemKey = system.PartitionKey;
            if (!systemKey.EndsWith("|"))
            {
                systemKey += "|";
            }

            // Create a filter for the system
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", systemKey, "eq");
            filters.Add(pkfilt);

            // Check the data provider to use
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileMetadataStats);

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
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.FileMetadataStats);

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

        public static long DeleteFileProcessStats(DataConfig providerConfig, POCO.System system)
        {

            long numEntriesDeleted = 0;

            string systemKey = system.PartitionKey;
            if (!systemKey.EndsWith("|"))
            {
                systemKey += "|";
            }

            // Create a filter for the system
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", systemKey, "eq");
            filters.Add(pkfilt);

            // Check the data provider to use
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.FileProcessStats);

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
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, MongoTableNames.FileProcessStats);

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

        public static List<POCO.Stat> GetStats(DataConfig providerConfig, List<DataFactory.Filter> filters)
        {
            List<POCO.Stat> stats = new List<POCO.Stat>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureStat> azdata = new List<AzureStat>();
                    AzureTableAdaptor<AzureStat> adaptor = new AzureTableAdaptor<AzureStat>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.CPStats, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        stats.Add(doc.Value);
                    }

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoStat>(providerConfig, MongoTableNames.CPStats);

                    FilterDefinition<MongoStat> filter = Utils.GenerateMongoFilter<MongoStat>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var stat in documents)
                    {
                        stats.Add(stat);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return stats;
        }

        public static List<POCO.Stat> GetFileType(DataConfig providerConfig, List<DataFactory.Filter> filters)
        {
            List<POCO.Stat> stats = new List<POCO.Stat>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureStat> azdata = new List<AzureStat>();
                    AzureTableAdaptor<AzureStat> adaptor = new AzureTableAdaptor<AzureStat>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.FileTypeStats, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        stats.Add(doc.Value);
                    }

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoStat>(providerConfig, MongoTableNames.FileTypeStats);

                    FilterDefinition<MongoStat> filter = Utils.GenerateMongoFilter<MongoStat>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var stat in documents)
                    {
                        stats.Add(stat);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return stats;
        }

        public static List<POCO.Stat> GetFileMetadata(DataConfig providerConfig, List<DataFactory.Filter> filters)
        {
            List<POCO.Stat> stats = new List<POCO.Stat>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureStat> azdata = new List<AzureStat>();
                    AzureTableAdaptor<AzureStat> adaptor = new AzureTableAdaptor<AzureStat>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.FileMetadataStats, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        stats.Add(doc.Value);
                    }

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoStat>(providerConfig, MongoTableNames.FileMetadataStats);

                    FilterDefinition<MongoStat> filter = Utils.GenerateMongoFilter<MongoStat>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var stat in documents)
                    {
                        stats.Add(stat);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return stats;
        }

        public static List<POCO.Stat> GetFileProcessing(DataConfig providerConfig, List<DataFactory.Filter> filters)
        {
            List<POCO.Stat> stats = new List<POCO.Stat>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureStat> azdata = new List<AzureStat>();
                    AzureTableAdaptor<AzureStat> adaptor = new AzureTableAdaptor<AzureStat>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.FileProcessStats, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        stats.Add(doc.Value);
                    }

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoStat>(providerConfig, MongoTableNames.FileProcessStats);

                    FilterDefinition<MongoStat> filter = Utils.GenerateMongoFilter<MongoStat>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var stat in documents)
                    {
                        stats.Add(stat);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return stats;
        }

        public static List<POCO.Stat> GetFileSize(DataConfig providerConfig, List<DataFactory.Filter> filters)
        {
            List<POCO.Stat> stats = new List<POCO.Stat>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureStat> azdata = new List<AzureStat>();
                    AzureTableAdaptor<AzureStat> adaptor = new AzureTableAdaptor<AzureStat>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.FileSizeStats, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        stats.Add(doc.Value);
                    }

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoStat>(providerConfig, MongoTableNames.FileSizeStats);

                    FilterDefinition<MongoStat> filter = Utils.GenerateMongoFilter<MongoStat>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var stat in documents)
                    {
                        stats.Add(stat);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return stats;
        }

    }
}
