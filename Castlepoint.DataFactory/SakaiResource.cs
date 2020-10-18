using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Castlepoint.POCO;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{

        public class MongoSakaiFileBatchStatusInsert:POCO.SakaiFileBatchStatus
    {

    }

    public class AzureSakaiFolder : EntityAdapter<POCO.SakaiFolder>
    {
        public AzureSakaiFolder() { }
        public AzureSakaiFolder(POCO.SakaiFolder o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoSakaiFolder:POCO.SakaiFolder
    {
        public ObjectId _id { get; set; }
    }

    public class MongoSakaiFolderUpsert : POCO.SakaiFolder
    {
        //public ObjectId _id { get; set; }
    }

    public class AzureSakaiFile : EntityAdapter<POCO.SakaiFile>
    {
        public AzureSakaiFile() { }
        public AzureSakaiFile(POCO.SakaiFile o) : base(o) { }
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
    public class MongoSakaiFile : POCO.SakaiFile
    {
        public ObjectId _id { get; set; }
    }

    public class MongoSakaiFileBatchStatus : POCO.SakaiFileBatchStatus
    {
        public ObjectId _id { get; set; }
    }

    public class AzureSakaiFileBatchStatus : EntityAdapter<POCO.SakaiFileBatchStatus>
    {
        public AzureSakaiFileBatchStatus() { }
        public AzureSakaiFileBatchStatus(POCO.SakaiFileBatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class SakaiResource
    {
        internal class AzureTableNames
        {
            internal const string SakaiFolders = "stlpsakaifolders";
            internal const string SakaiFiles = "stlpsakaifiles";
            internal const string SakaiFilesBatchStatus = "stlpsakaifilesbatchstatus";
        }

        internal class MongoTableNames
        {
            internal const string SakaiFolders = "sakaifolders";
            internal const string SakaiFiles = "sakaifiles";
            internal const string SakaiFilesBatchStatus = "sakaifilesbatchstatus";
        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = SakaiResource.AzureTableNames.SakaiFiles;

                    break;

                case "internal.mongodb":

                    tableName = SakaiResource.MongoTableNames.SakaiFiles;

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Call the update MIME type function
            File.UpdateMIMEType(providerConfig, mimeType, tableName);

            return;
        }

        public static string AddFolder(DataConfig providerConfig, POCO.SakaiFolder ntfsFolder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSakaiFolder az = new AzureSakaiFolder(ntfsFolder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, SakaiResource.AzureTableNames.SakaiFolders);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSakaiFolder> collection = Utils.GetMongoCollection<MongoSakaiFolder>(providerConfig, SakaiResource.MongoTableNames.SakaiFolders);
                    MongoSakaiFolder mongoObject = Utils.ConvertType<MongoSakaiFolder>(ntfsFolder);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static void UpdateFileMIMEType(DataConfig dataConfig, CPFileMIMEType fileMimeType)
        {
            DataFactory.File.UpdateMIMEType(dataConfig, fileMimeType, "sakaifiles");

            return;
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, "sakaifiles");

            return;
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            DataFactory.File.AddFileBatchStatus(providerConfig, fileBatchStatus, "sakaifilesbatchstatus");

            //TODO return id of new object if supported
            return;
        }

        public static List<POCO.SakaiFolder> GetFolders(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SakaiFolder> folders = new List<POCO.SakaiFolder>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSakaiFolder> azdata = new List<AzureSakaiFolder>();
                    AzureTableAdaptor<AzureSakaiFolder> adaptor = new AzureTableAdaptor<AzureSakaiFolder>();
                    azdata = adaptor.ReadTableData(providerConfig, SakaiResource.AzureTableNames.SakaiFolders, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        folders.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFolder>(providerConfig, SakaiResource.MongoTableNames.SakaiFolders);
                    FilterDefinition<MongoSakaiFolder> filter = Utils.GenerateMongoFilter<MongoSakaiFolder>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var ntfsfolder in documents)
                    {
                        folders.Add(ntfsfolder);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return folders;
        }

        public static bool ForceRescan(DataConfig providerConfig, POCO.SakaiFile file)
        {
            POCO.FileBatchStatus batchstatus = new FileBatchStatus(file.PartitionKey, file.RowKey);
            batchstatus.BatchGuid = Guid.Empty;
            batchstatus.BatchStatus = string.Empty;
            batchstatus.JsonFileProcessResult = "{}";

            DataFactory.File.UpdateFileBatchStatus(providerConfig, batchstatus, "ntfsfiles");

            return true;
        }

            public static List<POCO.SakaiFile> GetFiles(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;
            List<POCO.SakaiFile> files = new List<POCO.SakaiFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSakaiFile> azdata = new List<AzureSakaiFile>();
                    AzureTableAdaptor<AzureSakaiFile> adaptor = new AzureTableAdaptor<AzureSakaiFile>();
                    azdata = adaptor.ReadTableData(providerConfig, SakaiResource.AzureTableNames.SakaiFiles, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        files.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFile>(providerConfig, SakaiResource.MongoTableNames.SakaiFiles);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoSakaiFile> filter = Utils.GenerateMongoFilter<MongoSakaiFile>(filters);

                    //DEBUG output the filter values
                    //foreach (Castlepoint.DataFactory.Filter debugFilter in filters)
                    //{
                    //    // Output the filter field names and values
                    //    Console.WriteLine("DEBUG filter: " + debugFilter.FieldName + " : " + debugFilter.FieldValue);
                    //}
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();


                    foreach (var SakaiFile in documents)
                    {
                        files.Add(SakaiFile);
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

            return files;
        }

        public static List<POCO.SakaiFile> GetFiles(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SakaiFile> files = new List<POCO.SakaiFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSakaiFile> azdata = new List<AzureSakaiFile>();
                    AzureTableAdaptor<AzureSakaiFile> adaptor = new AzureTableAdaptor<AzureSakaiFile>();
                    azdata = adaptor.ReadTableData(providerConfig, SakaiResource.AzureTableNames.SakaiFiles, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        files.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFile>(providerConfig, SakaiResource.MongoTableNames.SakaiFiles);
                    FilterDefinition<MongoSakaiFile> filter = Utils.GenerateMongoFilter<MongoSakaiFile>(filters);

                    //DEBUG output the filter values
                    //foreach (Castlepoint.DataFactory.Filter debugFilter in filters)
                    //{
                    //    // Output the filter field names and values
                    //    Console.WriteLine("DEBUG filter: " + debugFilter.FieldName + " : " + debugFilter.FieldValue);
                    //}

                    var documents = collection.Find(filter).ToList();

                    foreach (var SakaiFile in documents)
                    {
                        files.Add(SakaiFile);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return files;
        }

        

        public static string AddFile(DataConfig providerConfig, POCO.SakaiFile ntfsFile)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSakaiFile az = new AzureSakaiFile(ntfsFile);

                    CloudTable table = Utils.GetCloudTable(providerConfig, SakaiResource.AzureTableNames.SakaiFiles);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSakaiFile> collection = Utils.GetMongoCollection<MongoSakaiFile>(providerConfig, SakaiResource.MongoTableNames.SakaiFiles);
                    MongoSakaiFile mongoObject = Utils.ConvertType<MongoSakaiFile>(ntfsFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.SakaiFileBatchStatus> GetFileBatchStatus(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SakaiFileBatchStatus> fileBatchStatus = new List<POCO.SakaiFileBatchStatus>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSakaiFileBatchStatus> azdata = new List<AzureSakaiFileBatchStatus>();
                    AzureTableAdaptor<AzureSakaiFileBatchStatus> adaptor = new AzureTableAdaptor<AzureSakaiFileBatchStatus>();
                    azdata = adaptor.ReadTableData(providerConfig, SakaiResource.AzureTableNames.SakaiFilesBatchStatus, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        fileBatchStatus.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFileBatchStatus>(providerConfig, SakaiResource.MongoTableNames.SakaiFilesBatchStatus);
                    FilterDefinition<MongoSakaiFileBatchStatus> filter = Utils.GenerateMongoFilter<MongoSakaiFileBatchStatus>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var status in documents)
                    {
                        fileBatchStatus.Add(status);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return fileBatchStatus;
        }

        public static void UpdateFolder(DataConfig providerConfig, POCO.SakaiFolder ntfsFolder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSakaiFolder az = new AzureSakaiFolder(ntfsFolder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, SakaiResource.AzureTableNames.SakaiFolders);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSakaiFolderUpsert> collection = Utils.GetMongoCollection<MongoSakaiFolderUpsert>(providerConfig, SakaiResource.MongoTableNames.SakaiFolders);
                    MongoSakaiFolderUpsert mongoObject = Utils.ConvertType<MongoSakaiFolderUpsert>(ntfsFolder);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSakaiFolderUpsert> filter = Utils.GenerateMongoFilter<MongoSakaiFolderUpsert>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void UpdateFileSize(DataConfig providerConfig, POCO.CPFileSize fileSize)
        {
            DataFactory.File.UpdateFileSize(providerConfig, "ntfsfiles", fileSize);

            return;
        }
    }
}
