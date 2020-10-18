using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Castlepoint.POCO;
using Castlepoint.POCO.Config;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Attributes;

namespace Castlepoint.DataFactory
{
    public class MongoFileBatchStatus : POCO.FileBatchStatus
    {

    }

    public class AzureFileBatchStatus : EntityAdapter<POCO.FileBatchStatus>
    {
        public AzureFileBatchStatus() { }
        public AzureFileBatchStatus(POCO.FileBatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoFileHash : POCO.FileHash
    {

    }

    public class AzureFileHash : EntityAdapter<POCO.FileHash>
    {
        public AzureFileHash() { }
        public AzureFileHash(POCO.FileHash o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    [BsonIgnoreExtraElements]
    public class MongoFileMIMEType : POCO.CPFileMIMEType
    {
        public ObjectId _id { get; set; }
    }

    public class AzureFileMIMEType : EntityAdapter<POCO.CPFileMIMEType>
    {
        public AzureFileMIMEType() { }
        public AzureFileMIMEType(POCO.CPFileMIMEType o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoFileSize : POCO.CPFileSize
    {

    }

    public class AzureFileSize : EntityAdapter<POCO.CPFileSize>
    {
        public AzureFileSize() { }
        public AzureFileSize(POCO.CPFileSize o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }


    public static class File
    {


        public static List<POCO.FileBatchStatus> GetFileBatchStatus(DataConfig providerConfig, string tableName, List<Filter> filters)
        {
            List<POCO.FileBatchStatus> batchstatus = new List<POCO.FileBatchStatus>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureFileBatchStatus> azdata = new List<AzureFileBatchStatus>();
                    AzureTableAdaptor<AzureFileBatchStatus> adaptor = new AzureTableAdaptor<AzureFileBatchStatus>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        batchstatus.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoFileBatchStatus>(providerConfig, tableName);

                    FilterDefinition<MongoFileBatchStatus> filter = Utils.GenerateMongoFilter<MongoFileBatchStatus>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        batchstatus.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return batchstatus;
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus, string tableName)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureFileBatchStatusInsert az = new AzureFileBatchStatusInsert(fileBatchStatus);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoFileBatchStatusInsert> collection = Utils.GetMongoCollection<MongoFileBatchStatusInsert>(providerConfig, tableName);
                    MongoFileBatchStatusInsert mongoObject = Utils.ConvertType<MongoFileBatchStatusInsert>(fileBatchStatus);
                    collection.InsertOne(mongoObject);
                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType, string tableName)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureFileMIMEType az = new AzureFileMIMEType(mimeType);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoFileMIMEType> collection = Utils.GetMongoCollection<MongoFileMIMEType>(providerConfig, tableName);
                    //MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(systemStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mimeType.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mimeType.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoFileMIMEType> filter = Utils.GenerateMongoFilter<MongoFileMIMEType>(filters);

                    var update = Builders<MongoFileMIMEType>.Update
                        .Set("MIMEType", mimeType.MIMEType);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus, string tableName)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureFileBatchStatusUpdate az = new AzureFileBatchStatusUpdate(fileBatchStatus);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoFileBatchStatusUpdate> collection = Utils.GetMongoCollection<MongoFileBatchStatusUpdate>(providerConfig, tableName);
                    MongoFileBatchStatusUpdate mongoObject = Utils.ConvertType<MongoFileBatchStatusUpdate>(fileBatchStatus);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoFileBatchStatusUpdate> filter = Utils.GenerateMongoFilter<MongoFileBatchStatusUpdate>(filters);

                    string updateParam = "{$set: {BatchGuid: '" + fileBatchStatus.BatchGuid.ToString() + "', BatchStatus: '" + fileBatchStatus.BatchStatus + "', JsonFileProcessResult: '" + fileBatchStatus.JsonFileProcessResult + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update the batch status
                    collection.UpdateOne(filter, updateDoc);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static string GetMIMEType(DataConfig providerConfig, POCO.System system, POCO.Record record, RecordToRecordAssociation recordAssocEntity, ILogger logger)
        {
            string mimeType = string.Empty;

            List<DataFactory.Filter> filters = new List<Filter>();
            DataFactory.Filter pkFilter = new Filter("PartitionKey", recordAssocEntity.RowKey, "eq");
            filters.Add(pkFilter);
            //DataFactory.Filter rkFilter = new Filter("RowKey", , "eq");
            //filters.Add(rkFilter);

            string nextPageId = string.Empty;

            string tableName = string.Empty;

            // Check which System the file is from
            switch (system.Type)
            {
                case SystemType.SharePoint2010:
                    switch (providerConfig.ProviderType)
                    {
                        case "azure.tableservice":
                            tableName = TableNames.Azure.SharePoint.SPFile;
                            break;
                        case "internal.mongodb":
                            tableName = TableNames.Mongo.SharePoint.SPFile;
                            break;
                    }
                    break;
                case SystemType.SharePoint2013:
                    switch (providerConfig.ProviderType)
                    {
                        case "azure.tableservice":
                            tableName = TableNames.Azure.SharePoint.SPFile;
                            break;
                        case "internal.mongodb":
                            tableName = TableNames.Mongo.SharePoint.SPFile;
                            break;
                    }
                    break;
                case SystemType.SharePointOnline:
                case SystemType.SharePointOneDrive:
                case SystemType.SharePointTeam:
                    switch (providerConfig.ProviderType)
                    {
                        case "azure.tableservice":
                            tableName = TableNames.Azure.SharePointOnline.SPFile;
                            break;
                        case "internal.mongodb":
                            tableName = TableNames.Mongo.SharePointOnline.SPFile;
                            break;
                    }
                    break;
                case SystemType.NTFSShare:
                    switch (providerConfig.ProviderType)
                    {
                        case "azure.tableservice":
                            tableName = TableNames.Azure.NTFS.NTFSFiles;
                            break;
                        case "internal.mongodb":
                            tableName = TableNames.Mongo.NTFS.NTFSFiles;
                            break;
                    }
                    break;
                case SystemType.SakaiAlliance:
                    switch (providerConfig.ProviderType)
                    {
                        case "azure.tableservice":
                            tableName = TableNames.Azure.Sakai.SakaiFiles;
                            break;
                        case "internal.mongodb":
                            tableName = TableNames.Mongo.Sakai.SakaiFiles;
                            break;
                    }
                    break;
                default:
                    throw new NotImplementedException();
                    break;
            }

            List<POCO.CPFileMIMEType> mimetypes = new List<POCO.CPFileMIMEType>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureFileMIMEType> azdata = new List<AzureFileMIMEType>();
                    AzureTableAdaptor<AzureFileMIMEType> adaptor = new AzureTableAdaptor<AzureFileMIMEType>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        mimetypes.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoFileMIMEType>(providerConfig, tableName);

                    FilterDefinition<MongoFileMIMEType> filter = Utils.GenerateMongoFilter<MongoFileMIMEType>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        mimetypes.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            if (mimetypes.Count > 0)
            {
                mimeType = mimetypes[0].MIMEType;
            }

            return mimeType;

        }

        public static void UpsertHashValueForFile(DataConfig providerConfig, POCO.FileHash hash)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureFileHash az = new AzureFileHash(hash);

                    CloudTable table = Utils.GetCloudTable(providerConfig, POCO.TableNames.Azure.FileHashTableNames.FileHash);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoFileHash> collection = Utils.GetMongoCollection<MongoFileHash>(providerConfig, POCO.TableNames.Mongo.FileHashTableNames.FileHash);
                    MongoFileHash mongoObject = Utils.ConvertType<MongoFileHash>(hash);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoFileHash> filter = Utils.GenerateMongoFilter<MongoFileHash>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        internal static void UpdateFileSize(DataConfig providerConfig, string tableName, POCO.CPFileSize fileSize)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureFileSize az = new AzureFileSize(fileSize);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoFileMIMEType> collection = Utils.GetMongoCollection<MongoFileMIMEType>(providerConfig, tableName);
                    //MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(systemStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(fileSize.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(fileSize.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoFileMIMEType> filter = Utils.GenerateMongoFilter<MongoFileMIMEType>(filters);

                    var update = Builders<MongoFileMIMEType>.Update
                        .Set("SizeInBytes", fileSize.SizeInBytes);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }
    }

}
