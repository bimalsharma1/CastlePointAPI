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
    class MongoEOMessage : POCO.O365.EOMessage
    {
        public ObjectId _id { get; set; }
    }

    class MongoEOMessageEntity : POCO.O365.EOMessageEntity
    {
        public ObjectId _id { get; set; }
    }

    class MongoEOFolderEntity : POCO.O365.EOFolderEntity
    {
        public ObjectId _id { get; set; }
    }

    class MongoEOFolderUpdate : POCO.O365.EOFolderUpdate
    {
        public ObjectId _id { get; set; }
    }

    class AzureEOFolderUpdate : EntityAdapter<POCO.O365.EOFolderUpdate>
    {
        public AzureEOFolderUpdate() { }
        public AzureEOFolderUpdate(POCO.O365.EOFolderUpdate o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureEOMessageEntity : EntityAdapter<POCO.O365.EOMessageEntity>
    {
        public AzureEOMessageEntity() { }
        public AzureEOMessageEntity(POCO.O365.EOMessageEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    class AzureEOFolder : EntityAdapter<POCO.O365.EOFolderEntity>
    {
        public AzureEOFolder() { }
        public AzureEOFolder(POCO.O365.EOFolderEntity o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class ExchangeOnline
    {
        internal class AzureTableNames
        {
            internal const string EOMessage = "stlpo365eomessages";
            internal const string EOMessageBatchStatus = "stlpo365eomessagesbatchstatus";
            internal const string EOMessageTracking = "stlpeotracking";
            internal const string EOFolder = "stlpo365eofolders";
            internal const string EOMailbox = "stlpo365eomailbox";
        }
        internal class MongoTableNames
        {
            internal const string EOMessage = "o365eomessages";
            internal const string EOMessageBatchStatus = "o365eomessagesbatchstatus";
            internal const string EOMessageTracking = "eotracking";
            internal const string EOFolder = "o365eofolders";
            internal const string EOMailbox = "o365eomailbox";
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, AzureTableNames.EOMessage);
                    break;
                case "internal.mongodb":
                    DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.EOMessage);
                    break;
            }
            return;
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    DataFactory.File.AddFileBatchStatus(providerConfig, fileBatchStatus, AzureTableNames.EOMessageBatchStatus);
                    break;
                case "internal.mongodb":
                    DataFactory.File.AddFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.EOMessageBatchStatus);
                    break;
            }

            //TODO return id of new object if supported
            return;
        }

        public static string AddMessage(DataConfig providerConfig, POCO.O365.EOMessageEntity eomessage)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureEOMessageEntity az = new AzureEOMessageEntity(eomessage);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.EOMessage);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoEOMessageEntity> collection = Utils.GetMongoCollection<MongoEOMessageEntity>(providerConfig, MongoTableNames.EOMessage);
                    MongoEOMessageEntity mongoObject = Utils.ConvertType<MongoEOMessageEntity>(eomessage);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.O365.EOFolderEntity> GetFolders(DataConfig providerConfig, string emailAddress, POCO.O365.EOMailboxFolder mailboxFolder)
        {
            List<DataFactory.Filter> filters = new List<Filter>();
            DataFactory.Filter pkfilt = new Filter("PartitionKey", Utils.CleanTableKey(emailAddress), "eq");
            filters.Add(pkfilt);
            DataFactory.Filter rkfilt = new Filter("RowKey", mailboxFolder.displayName, "eq");
            filters.Add(rkfilt);
            return GetFolders(providerConfig, filters);

        }

        public static List<POCO.O365.EOFolderEntity> GetFolders(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.EOFolderEntity> folderEnt = new List<POCO.O365.EOFolderEntity>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureEOFolder> azdata = new List<AzureEOFolder>();
                    AzureTableAdaptor<AzureEOFolder> adaptor = new AzureTableAdaptor<AzureEOFolder>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.EOFolder, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        folderEnt.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoEOFolderEntity>(providerConfig, MongoTableNames.EOFolder);

                    FilterDefinition<MongoEOFolderEntity> filter = Utils.GenerateMongoFilter<MongoEOFolderEntity>(filters);

                    //TODO paging
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var doc in documents)
                    {
                        folderEnt.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return folderEnt;
        }


        public static string AddFolder(DataConfig providerConfig, POCO.O365.EOFolderEntity folder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureEOFolder az = new AzureEOFolder(folder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.EOFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoEOFolderEntity> collection = Utils.GetMongoCollection<MongoEOFolderEntity>(providerConfig, MongoTableNames.EOFolder);
                    MongoEOFolderEntity mongoObject = Utils.ConvertType<MongoEOFolderEntity>(folder);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }


        public static List<POCO.O365.EOMessageEntity> GetMessages(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;

            List<POCO.O365.EOMessageEntity> files = new List<POCO.O365.EOMessageEntity>();

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
                        List<AzureEOMessageEntity> azdata = new List<AzureEOMessageEntity>();
                        AzureTableAdaptor<AzureEOMessageEntity> adaptor = new AzureTableAdaptor<AzureEOMessageEntity>();
                        azdata = adaptor.ReadTableDataWithToken(providerConfig, AzureTableNames.EOMessage, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

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
                        var collection = Utils.GetMongoCollection<MongoEOMessageEntity>(providerConfig, MongoTableNames.EOMessage);

                        // Add an _id filter if a page has been requested
                        if (thisPageId != null && thisPageId != string.Empty)
                        {
                            filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                        }


                        FilterDefinition<MongoEOMessageEntity> filter = Utils.GenerateMongoFilter<MongoEOMessageEntity>(filters);

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

        public static List<POCO.O365.EOMessageEntity> GetMessageInfo(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.O365.EOMessageEntity> messageInfo = new List<POCO.O365.EOMessageEntity>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureEOMessageEntity> azdata = new List<AzureEOMessageEntity>();
                    AzureTableAdaptor<AzureEOMessageEntity> adaptor = new AzureTableAdaptor<AzureEOMessageEntity>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.EOMessage, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        messageInfo.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoEOMessageEntity>(providerConfig, MongoTableNames.EOMessage);

                    FilterDefinition<MongoEOMessageEntity> filter = Utils.GenerateMongoFilter<MongoEOMessageEntity>(filters);

                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

                    foreach (var obj in documents)
                    {
                        messageInfo.Add(obj);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return messageInfo;
        }

        

        //public static List<POCO.O365.SPFolder> GetFolders(DataConfig providerConfig, List<Filter> filters)
        //{
        //    List<POCO.O365.SPFolder> webInfo = new List<POCO.O365.SPFolder>();

        //    switch (providerConfig.ProviderType)
        //    {
        //        case "azure.tableservice":

        //            string combinedFilter = Utils.GenerateAzureFilter(filters);

        //            List<AzureSPFolder> azdata = new List<AzureSPFolder>();
        //            AzureTableAdaptor<AzureSPFolder> adaptor = new AzureTableAdaptor<AzureSPFolder>();
        //            azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.SPFolder, combinedFilter);

        //            foreach (var doc in azdata)
        //            {
        //                webInfo.Add(doc.Value);
        //            }

        //            break;
        //        case "internal.mongodb":
        //            var collection = Utils.GetMongoCollection<MongoSPFolder>(providerConfig, MongoTableNames.SPFolder);

        //            FilterDefinition<MongoSPFolder> filter = Utils.GenerateMongoFilter<MongoSPFolder>(filters);

        //            //TODO paging
        //            var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(1000).ToList();

        //            foreach (var doc in documents)
        //            {
        //                webInfo.Add(doc);
        //            }
        //            break;
        //        default:
        //            throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
        //    }

        //    return webInfo;
        //}

        //public static void UpdateSPOWebInfoLastProcessed(DataConfig providerConfig, POCO.O365.SPOWebInfoEntity webInfo)
        //{
        //    switch (providerConfig.ProviderType)
        //    {
        //        case "azure.tableservice":
        //            AzureSPOWebInfoEntity az = new AzureSPOWebInfoEntity(webInfo);

        //            CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SPOTracking);
        //            TableOperation operation = TableOperation.InsertOrMerge(az);
        //            Task tUpdate = table.ExecuteAsync(operation);
        //            tUpdate.Wait();

        //            break;

        //        case "internal.mongodb":
        //            IMongoCollection<MongoSPOWebInfoEntity> collection = Utils.GetMongoCollection<MongoSPOWebInfoEntity>(providerConfig, MongoTableNames.SPOTracking);
        //            MongoSPOWebInfoEntity mongoObject = Utils.ConvertType<MongoSPOWebInfoEntity>(webInfo);

        //            // Create the update filter
        //            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
        //            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
        //            DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
        //            filters.Add(pkFilter);
        //            filters.Add(rkFilter);
        //            FilterDefinition<MongoSPOWebInfoEntity> filter = Utils.GenerateMongoFilter<MongoSPOWebInfoEntity>(filters);

        //            var update = Builders<MongoSPOWebInfoEntity>.Update
        //                .Set("LastItemModifiedDate", webInfo.LastItemModifiedDate)
        //                .Set("LastItemUserModifiedDate", webInfo.LastItemUserModifiedDate);

        //            // Update the batch status
        //            UpdateResult result = collection.UpdateOne(filter, update);

        //            return;

        //        default:
        //            throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
        //    }
        //    return;
        //}

        public static void UpdateEOFolder(DataConfig providerConfig, POCO.O365.EOFolderUpdate folderUpdate)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureEOFolderUpdate az = new AzureEOFolderUpdate(folderUpdate);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.EOFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoEOFolderUpdate> collection = Utils.GetMongoCollection<MongoEOFolderUpdate>(providerConfig, MongoTableNames.EOFolder);
                    MongoEOFolderUpdate mongoObject = Utils.ConvertType<MongoEOFolderUpdate>(folderUpdate);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoEOFolderUpdate> filter = Utils.GenerateMongoFilter<MongoEOFolderUpdate>(filters);

                    var update = Builders<MongoEOFolderUpdate>.Update
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

                    tableName = AzureTableNames.EOMessage;

                    break;

                case "internal.mongodb":

                    tableName = MongoTableNames.EOMessage;

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
