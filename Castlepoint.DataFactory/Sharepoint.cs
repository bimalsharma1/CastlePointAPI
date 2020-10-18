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

    class MongoSharePointFile : POCO.SharePoint.SPFile
    {
        public ObjectId _id { get; set; }
    }

    //class MongoSPFileInfoEntity : POCO.O365.SPOFileInfoEntity
    //{
    //    public ObjectId _id { get; set; }
    //}

    class MongoSharePointWebInfo : POCO.SharePoint.SharePointWebInfo
    {
        public ObjectId _id { get; set; }
    }

    class MongoSharePointWebInfoLastUpdated : POCO.SharePoint.SharePointWebInfoLastUpdated
    {
        public ObjectId _id { get; set; }
    }

    class MongoSharePointFolder : POCO.SharePoint.SPFolder
    {
        public ObjectId _id { get; set; }
    }

    class MongoSharePointList : POCO.SharePoint.SPList
    {
        public ObjectId _id { get; set; }
    }

    class MongoSharePointFolderUpdate : POCO.O365.SPFolderUpdate
    {
        public ObjectId _id { get; set; }
    }

    class AzureSharePointFolderUpdate : EntityAdapter<POCO.SharePoint.SPFolderUpdate>
    {
        public AzureSharePointFolderUpdate() { }
        public AzureSharePointFolderUpdate(POCO.SharePoint.SPFolderUpdate o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureSharePointList : EntityAdapter<POCO.SharePoint.SPList>
    {
        public AzureSharePointList() { }
        public AzureSharePointList(POCO.SharePoint.SPList o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class AzureSharePointFolder : EntityAdapter<POCO.SharePoint.SPFolder>
    {
        public AzureSharePointFolder() { }
        public AzureSharePointFolder(POCO.SharePoint.SPFolder o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureSharePointWebInfo : EntityAdapter<POCO.SharePoint.SharePointWebInfo>
    {
        public AzureSharePointWebInfo() { }
        public AzureSharePointWebInfo(POCO.SharePoint.SharePointWebInfo o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureSharePointWebInfoLastUpdated : EntityAdapter<POCO.SharePoint.SharePointWebInfoLastUpdated>
    {
        public AzureSharePointWebInfoLastUpdated() { }
        public AzureSharePointWebInfoLastUpdated(POCO.SharePoint.SharePointWebInfoLastUpdated o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureSharePointFile : EntityAdapter<POCO.SharePoint.SPFile>
    {
        public AzureSharePointFile() { }
        public AzureSharePointFile(POCO.SharePoint.SPFile o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    //class AzureSPFile : EntityAdapter<POCO.SPFile>
    //{
    //    public AzureSPFile() { }
    //    public AzureSPFile(POCO.SPFile o) : base(o) { }
    //    protected override string BuildPartitionKey()
    //    {
    //        return this.Value.PartitionKey;
    //    }

    //    protected override string BuildRowKey()
    //    {
    //        return this.Value.RowKey;
    //    }
    //}

    public static class SharePoint
    {
        internal class AzureTableNames
        {
            internal const string SPFile = "stlpspfiles";
            internal const string SPFileBatchStatus = "stlpspfilesbatchstatus";
            internal const string SPTracking = "stlpsptracking";
            internal const string SPFolder = "stlpspfolders";
            internal const string SPList = "stlpsplists";
        }
        internal class MongoTableNames
        {
            internal const string SPFile = "spfiles";
            internal const string SPFileBatchStatus = "spfilesbatchstatus";
            internal const string SPTracking = "sptracking";
            internal const string SPFolder = "spfolders";
            internal const string SPList = "splists";
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

        public static string AddFile(DataConfig providerConfig, POCO.SharePoint.SPFile spFile)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSharePointFile az = new AzureSharePointFile(spFile);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPFile);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSharePointFile> collection = Utils.GetMongoCollection<MongoSharePointFile>(providerConfig, MongoTableNames.SPFile);
                    MongoSharePointFile mongoObject = Utils.ConvertType<MongoSharePointFile>(spFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
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
                    IMongoCollection<MongoSharePointFile> collection = Utils.GetMongoCollection<MongoSharePointFile>(providerConfig, MongoTableNames.SPFile);
                    MongoSharePointFile mongoObject = Utils.ConvertType<MongoSharePointFile>(spFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }


        public static List<POCO.SharePoint.SPList> GetList(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SharePoint.SPList> listInfo = new List<POCO.SharePoint.SPList>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSharePointList> azdata = new List<AzureSharePointList>();
                    AzureTableAdaptor<AzureSharePointList> adaptor = new AzureTableAdaptor<AzureSharePointList>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPList, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        listInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSharePointList>(providerConfig, MongoTableNames.SPList);

                    FilterDefinition<MongoSharePointList> filter = Utils.GenerateMongoFilter<MongoSharePointList>(filters);

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
        public static string AddFolder(DataConfig providerConfig, POCO.SharePoint.SPFolder spFolder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSharePointFolder az = new AzureSharePointFolder(spFolder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSharePointFolder> collection = Utils.GetMongoCollection<MongoSharePointFolder>(providerConfig, MongoTableNames.SPFolder);
                    MongoSharePointFolder mongoObject = Utils.ConvertType<MongoSharePointFolder>(spFolder);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddList(DataConfig providerConfig, POCO.SharePoint.SPList spList)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSharePointList az = new AzureSharePointList(spList);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPList);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSharePointList> collection = Utils.GetMongoCollection<MongoSharePointList>(providerConfig, MongoTableNames.SPList);
                    MongoSharePointList mongoObject = Utils.ConvertType<MongoSharePointList>(spList);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.SharePoint.SPFile> GetFiles(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;

            List<POCO.SharePoint.SPFile> spfiles = new List<POCO.SharePoint.SPFile>();

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
                    List<AzureSharePointFile> azdata = new List<AzureSharePointFile>();
                    AzureTableAdaptor<AzureSharePointFile> adaptor = new AzureTableAdaptor<AzureSharePointFile>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, AzureTableNames.SPFile, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

                    foreach (var doc in azdata)
                    {
                        spfiles.Add(doc.Value);
                    }

                    // Check if there is a next page token available
                    if (nextPageToken != null)
                    {
                        nextPageId = Newtonsoft.Json.JsonConvert.SerializeObject(nextPageToken);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSharePointFile>(providerConfig, MongoTableNames.SPFile);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoSharePointFile> filter = Utils.GenerateMongoFilter<MongoSharePointFile>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();

                    foreach (var doc in documents)
                    {
                        spfiles.Add(doc);
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

            return spfiles;
        }

        public static List<POCO.SharePoint.SPFile> GetFileInfo(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SharePoint.SPFile> fileInfo = new List<POCO.SharePoint.SPFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSharePointFile> azdata = new List<AzureSharePointFile>();
                    AzureTableAdaptor<AzureSharePointFile> adaptor = new AzureTableAdaptor<AzureSharePointFile>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPFile, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        fileInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSharePointFile>(providerConfig, MongoTableNames.SPFile);

                    FilterDefinition<MongoSharePointFile> filter = Utils.GenerateMongoFilter<MongoSharePointFile>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var file in documents)
                    {
                        fileInfo.Add(file);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return fileInfo;
        }

        public static List<POCO.SharePoint.SharePointWebInfo> GetWebInfo(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SharePoint.SharePointWebInfo> webInfo = new List<POCO.SharePoint.SharePointWebInfo>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSharePointWebInfo> azdata = new List<AzureSharePointWebInfo>();
                    AzureTableAdaptor<AzureSharePointWebInfo> adaptor = new AzureTableAdaptor<AzureSharePointWebInfo>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPTracking, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        webInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSharePointWebInfo>(providerConfig, MongoTableNames.SPTracking);

                    FilterDefinition<MongoSharePointWebInfo> filter = Utils.GenerateMongoFilter<MongoSharePointWebInfo>(filters);

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

        public static List<POCO.SharePoint.SPFolder> GetFolders(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SharePoint.SPFolder> folderInfo = new List<POCO.SharePoint.SPFolder>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSharePointFolder> azdata = new List<AzureSharePointFolder>();
                    AzureTableAdaptor<AzureSharePointFolder> adaptor = new AzureTableAdaptor<AzureSharePointFolder>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPFolder, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        folderInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSharePointFolder>(providerConfig, MongoTableNames.SPFolder);

                    FilterDefinition<MongoSharePointFolder> filter = Utils.GenerateMongoFilter<MongoSharePointFolder>(filters);

                    //TODO paging
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var doc in documents)
                    {
                        folderInfo.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return folderInfo;
        }

        public static void UpdateSharePointWebInfoLastProcessed(DataConfig providerConfig, POCO.SharePoint.SharePointWebInfoLastUpdated webInfo)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSharePointWebInfoLastUpdated az = new AzureSharePointWebInfoLastUpdated(webInfo);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPTracking);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSharePointWebInfoLastUpdated> collection = Utils.GetMongoCollection<MongoSharePointWebInfoLastUpdated>(providerConfig, MongoTableNames.SPTracking);
                    MongoSharePointWebInfoLastUpdated mongoObject = Utils.ConvertType<MongoSharePointWebInfoLastUpdated>(webInfo);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSharePointWebInfoLastUpdated> filter = Utils.GenerateMongoFilter<MongoSharePointWebInfoLastUpdated>(filters);

                    var update = Builders<MongoSharePointWebInfoLastUpdated>.Update
                        .Set("LastItemModifiedDate", webInfo.LastItemModifiedDate);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void AddWebInfo(DataConfig providerConfig, POCO.SharePoint.SharePointWebInfo webInfo)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSharePointWebInfo az = new AzureSharePointWebInfo(webInfo);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPTracking);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSharePointWebInfo> collection = Utils.GetMongoCollection<MongoSharePointWebInfo>(providerConfig, MongoTableNames.SPTracking);
                    MongoSharePointWebInfo mongoObject = Utils.ConvertType<MongoSharePointWebInfo>(webInfo);
                    collection.InsertOne(mongoObject);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void UpdateSharePointFolder(DataConfig providerConfig, POCO.SharePoint.SPFolderUpdate folderUpdate)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSharePointFolderUpdate az = new AzureSharePointFolderUpdate(folderUpdate);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSharePointFolderUpdate> collection = Utils.GetMongoCollection<MongoSharePointFolderUpdate>(providerConfig, MongoTableNames.SPFolder);
                    MongoSharePointFolderUpdate mongoObject = Utils.ConvertType<MongoSharePointFolderUpdate>(folderUpdate);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSharePointFolderUpdate> filter = Utils.GenerateMongoFilter<MongoSharePointFolderUpdate>(filters);

                    var update = Builders<MongoSharePointFolderUpdate>.Update
                        //.Set("TimeCreated", folderUpdate.TimeCreated)
                        //.Set("TimeLastModified", folderUpdate.TimeLastModified)
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

        public static bool ForceRescan(DataConfig providerConfig, POCO.SharePoint.SPFile file)
        {
            POCO.FileBatchStatus batchstatus = new FileBatchStatus(file.PartitionKey, file.RowKey);
            batchstatus.BatchGuid = Guid.Empty;
            batchstatus.BatchStatus = string.Empty;
            batchstatus.JsonFileProcessResult = "{}";

            string tableName = string.Empty;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    tableName = POCO.TableNames.Azure.SharePoint.SPFile;
                    break;
                case "internal.mongodb":
                    tableName = POCO.TableNames.Mongo.SharePoint.SPFile;
                    break;
                default:
                    throw new ApplicationException("Unknown provider type: " + providerConfig.ProviderType);
                    break;
            }

            DataFactory.File.UpdateFileBatchStatus(providerConfig, batchstatus, tableName);

            return true;
        }
    }
}
