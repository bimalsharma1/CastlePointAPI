using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Castlepoint.POCO;

using Newtonsoft.Json.Serialization;

namespace Castlepoint.DataFactory
{
    class MongoSPFile : POCO.SPFile
    {
        public ObjectId _id { get; set; }
    }

    class MongoSPFileInfoEntity : POCO.O365.SPOFileInfoEntity
    {
        public ObjectId _id { get; set; }
    }

    class MongoSPOWebInfoEntity : POCO.O365.SPOWebInfoEntity
    {
        public ObjectId _id { get; set; }
    }

    class MongoSPFolder: POCO.O365.SPFolder
    {
        public ObjectId _id { get; set; }
    }

    class MongoSPList: POCO.O365.SPList
    {
        public ObjectId _id { get; set; }
    }

    class MongoSPFolderUpdate : POCO.O365.SPFolderUpdate
    {
        public ObjectId _id { get; set; }
    }

    class AzureSPFolderUpdate: EntityAdapter<POCO.O365.SPFolderUpdate>
    {
        public AzureSPFolderUpdate() { }
        public AzureSPFolderUpdate(POCO.O365.SPFolderUpdate o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureSPList : EntityAdapter<POCO.O365.SPList>
    {
        public AzureSPList() { }
        public AzureSPList(POCO.O365.SPList o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureSPFolder: EntityAdapter<POCO.O365.SPFolder>
    {
        public AzureSPFolder() { }
        public AzureSPFolder(POCO.O365.SPFolder o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureSPOWebInfoEntity : EntityAdapter<POCO.O365.SPOWebInfoEntity>
    {
        public AzureSPOWebInfoEntity() { }
        public AzureSPOWebInfoEntity(POCO.O365.SPOWebInfoEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureSPFileInfoEntity : EntityAdapter<POCO.O365.SPOFileInfoEntity>
    {
        public AzureSPFileInfoEntity() { }
        public AzureSPFileInfoEntity(POCO.O365.SPOFileInfoEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureSPFile : EntityAdapter<POCO.SPFile>
    {
        public AzureSPFile() { }
        public AzureSPFile(POCO.SPFile o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class SharePointOnline
    {
        internal class AzureTableNames
        {
            internal const string SPFile = "stlpo365spfiles";
            internal const string SPFileBatchStatus = "stlpo365spfilesbatchstatus";
            internal const string SPOTracking = "stlpspotracking";
            internal const string SPFolder = "stlpo365spfolders";
            internal const string SPList = "stlpo365splists";
        }
        internal class MongoTableNames
        {
            internal const string SPFile = "o365spfiles";
            internal const string SPFileBatchStatus = "o365spfilesbatchstatus";
            internal const string SPOTracking = "spotracking";
            internal const string SPFolder = "o365spfolders";
            internal const string SPList = "o365splists";
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, AzureTableNames.SPFile);
                    break;
                case "internal.mongodb":
                    DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.SPFile);
                    break;
            }
            return;
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    DataFactory.File.AddFileBatchStatus(providerConfig, fileBatchStatus, AzureTableNames.SPFileBatchStatus);
                    break;
                case "internal.mongodb":
                    DataFactory.File.AddFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.SPFileBatchStatus);
                    break;
            }

            //TODO return id of new object if supported
            return;
        }

        public static string AddFile(DataConfig providerConfig, POCO.SPFile spFile)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSPFile az = new AzureSPFile(spFile);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPFile);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSPFile> collection = Utils.GetMongoCollection<MongoSPFile>(providerConfig, MongoTableNames.SPFile);
                    MongoSPFile mongoObject = Utils.ConvertType<MongoSPFile>(spFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddFolder(DataConfig providerConfig, POCO.O365.SPFolder spFolder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSPFolder az = new AzureSPFolder(spFolder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSPFolder> collection = Utils.GetMongoCollection<MongoSPFolder>(providerConfig, MongoTableNames.SPFolder);
                    MongoSPFolder mongoObject = Utils.ConvertType<MongoSPFolder>(spFolder);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.O365.SPList> GetList(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.SPList> listInfo = new List<POCO.O365.SPList>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSPList> azdata = new List<AzureSPList>();
                    AzureTableAdaptor<AzureSPList> adaptor = new AzureTableAdaptor<AzureSPList>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPList, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        listInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSPList>(providerConfig, MongoTableNames.SPList);

                    FilterDefinition<MongoSPList> filter = Utils.GenerateMongoFilter<MongoSPList>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var doc in documents)
                    {
                        listInfo.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return listInfo;
        }

        public static string AddList(DataConfig providerConfig, POCO.O365.SPList spList)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSPList az = new AzureSPList(spList);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPList);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSPList> collection = Utils.GetMongoCollection<MongoSPList>(providerConfig, MongoTableNames.SPList);
                    MongoSPList mongoObject = Utils.ConvertType<MongoSPList>(spList);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.SPFile> GetFiles(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;

            List<POCO.SPFile> files = new List<POCO.SPFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    {
                        string combinedFilter = Utils.GenerateAzureFilter(filters);
                        TableContinuationToken thisPageToken = null;
                        if (thisPageId != null && thisPageId != string.Empty)
                        {
                            thisPageToken = Newtonsoft.Json.JsonConvert.DeserializeObject<TableContinuationToken>(thisPageId);
                        }
                        TableContinuationToken nextPageToken = null;
                        List<AzureSPFile> azdata = new List<AzureSPFile>();
                        AzureTableAdaptor<AzureSPFile> adaptor = new AzureTableAdaptor<AzureSPFile>();
                        azdata = adaptor.ReadTableDataWithToken(providerConfig, AzureTableNames.SPFile, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

                        foreach (var doc in azdata)
                        {
                            files.Add(doc.Value);
                        }

                        // Check if there is a next page token available
                        if (nextPageToken != null)
                        {
                            nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                        }

                        break;
                    }

                case "internal.mongodb":
                    {
                        var lastId = "";
                        var collection = Utils.GetMongoCollection<MongoSPFile>(providerConfig, MongoTableNames.SPFile);

                        // Add an _id filter if a page has been requested
                        if (thisPageId != null && thisPageId != string.Empty)
                        {
                            filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                        }


                        FilterDefinition<MongoSPFile> filter = Utils.GenerateMongoFilter<MongoSPFile>(filters);

                        var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();

                        foreach (var file in documents)
                        {
                            files.Add(file);
                            lastId = file._id.ToString();
                        }

                        // Check if there are more pages of data
                        if (files.Count==rowLimit)
                        {
                            // Return the _id of the last row
                            nextPageId = lastId;
                        }

                        break;
                    }
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return files;
        }

        public static List<POCO.O365.SPOFileInfoEntity> GetFileInfo(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.SPOFileInfoEntity> fileInfo = new List<POCO.O365.SPOFileInfoEntity>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSPFileInfoEntity> azdata = new List<AzureSPFileInfoEntity>();
                    AzureTableAdaptor<AzureSPFileInfoEntity> adaptor = new AzureTableAdaptor<AzureSPFileInfoEntity>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPFile, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        fileInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSPFileInfoEntity>(providerConfig, MongoTableNames.SPFile);

                    FilterDefinition<MongoSPFileInfoEntity> filter = Utils.GenerateMongoFilter<MongoSPFileInfoEntity>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var NTFSFile in documents)
                    {
                        fileInfo.Add(NTFSFile);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return fileInfo;
        }

        public static List<POCO.O365.SPOWebInfoEntity> GetWebInfo(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.SPOWebInfoEntity> webInfo = new List<POCO.O365.SPOWebInfoEntity>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSPOWebInfoEntity> azdata = new List<AzureSPOWebInfoEntity>();
                    AzureTableAdaptor<AzureSPOWebInfoEntity> adaptor = new AzureTableAdaptor<AzureSPOWebInfoEntity>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPOTracking, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        webInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSPOWebInfoEntity>(providerConfig, MongoTableNames.SPOTracking);

                    FilterDefinition<MongoSPOWebInfoEntity> filter = Utils.GenerateMongoFilter<MongoSPOWebInfoEntity>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var doc in documents)
                    {
                        webInfo.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return webInfo;
        }

        public static List<POCO.O365.SPFolder> GetFolders(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.SPFolder> webInfo = new List<POCO.O365.SPFolder>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSPFolder> azdata = new List<AzureSPFolder>();
                    AzureTableAdaptor<AzureSPFolder> adaptor = new AzureTableAdaptor<AzureSPFolder>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPFolder, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        webInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSPFolder>(providerConfig, MongoTableNames.SPFolder);

                    FilterDefinition<MongoSPFolder> filter = Utils.GenerateMongoFilter<MongoSPFolder>(filters);

                    //TODO paging
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var doc in documents)
                    {
                        webInfo.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return webInfo;
        }

        public static void UpdateSPOWebInfoLastProcessed(DataConfig providerConfig, POCO.O365.SPOWebInfoEntity webInfo)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSPOWebInfoEntity az = new AzureSPOWebInfoEntity(webInfo);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPOTracking);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSPOWebInfoEntity> collection = Utils.GetMongoCollection<MongoSPOWebInfoEntity>(providerConfig, MongoTableNames.SPOTracking);
                    MongoSPOWebInfoEntity mongoObject = Utils.ConvertType<MongoSPOWebInfoEntity>(webInfo);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSPOWebInfoEntity> filter = Utils.GenerateMongoFilter<MongoSPOWebInfoEntity>(filters);

                    var update = Builders<MongoSPOWebInfoEntity>.Update
                        .Set("LastItemModifiedDate", webInfo.LastItemModifiedDate)
                        .Set("LastItemUserModifiedDate", webInfo.LastItemUserModifiedDate);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void UpdateSPOFolder(DataConfig providerConfig, POCO.O365.SPFolderUpdate folderUpdate)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSPFolderUpdate az = new AzureSPFolderUpdate(folderUpdate);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSPFolderUpdate> collection = Utils.GetMongoCollection<MongoSPFolderUpdate>(providerConfig, MongoTableNames.SPFolder);
                    MongoSPFolderUpdate mongoObject = Utils.ConvertType<MongoSPFolderUpdate>(folderUpdate);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSPFolderUpdate> filter = Utils.GenerateMongoFilter<MongoSPFolderUpdate>(filters);

                    var update = Builders<MongoSPFolderUpdate>.Update
                        .Set("TimeCreated", folderUpdate.TimeCreated)
                        .Set("TimeLastModified", folderUpdate.TimeLastModified)
                        .Set("ItemCount", folderUpdate.ItemCount)
                        .Set("Name", folderUpdate.Name)
                        .Set("CPFolderStatus", folderUpdate.CPFolderStatus);


                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = SharePointOnline.AzureTableNames.SPFile;

                    break;

                case "internal.mongodb":

                    tableName = SharePointOnline.MongoTableNames.SPFile;

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Call the update MIME type function
            File.UpdateMIMEType(providerConfig, mimeType, tableName);

            return;
        }
    }
}
