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
    public class MongoFileBatchStatusInsert:POCO.FileBatchStatus
    {

    }
    public class AzureFileBatchStatusInsert : EntityAdapter<POCO.FileBatchStatus>
    {
        public AzureFileBatchStatusInsert() { }
        public AzureFileBatchStatusInsert(POCO.FileBatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class MongoFileBatchStatusUpdate: POCO.FileBatchStatus
    {
        //public ObjectId _id { get; set; }
    }
    public class AzureFileBatchStatusUpdate : EntityAdapter<POCO.FileBatchStatus>
    {
        public AzureFileBatchStatusUpdate() { }
        public AzureFileBatchStatusUpdate(POCO.FileBatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
        public class MongoNTFSFileBatchStatusInsert:POCO.NTFSFileBatchStatus
    {

    }

    public class AzureNTFSFolder : EntityAdapter<POCO.NTFSFolder>
    {
        public AzureNTFSFolder() { }
        public AzureNTFSFolder(POCO.NTFSFolder o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoNTFSFolder:POCO.NTFSFolder
    {
        public ObjectId _id { get; set; }
    }

    public class MongoNTFSFolderUpsert : POCO.NTFSFolder
    {
        //public ObjectId _id { get; set; }
    }

    public class AzureNTFSFile : EntityAdapter<POCO.NTFSFile>
    {
        public AzureNTFSFile() { }
        public AzureNTFSFile(POCO.NTFSFile o) : base(o) { }
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
    public class MongoNTFSFile : POCO.NTFSFile
    {
        public ObjectId _id { get; set; }
    }

    public class MongoNTFSFileBatchStatus : POCO.NTFSFileBatchStatus
    {
        public ObjectId _id { get; set; }
    }

    public class AzureNTFSFileBatchStatus : EntityAdapter<POCO.NTFSFileBatchStatus>
    {
        public AzureNTFSFileBatchStatus() { }
        public AzureNTFSFileBatchStatus(POCO.NTFSFileBatchStatus o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class NTFS
    {
        internal class AzureTableNames
        {
            internal const string NTFSFolders = "stlpntfsfolders";
            internal const string NTFSFiles = "stlpntfsfiles";
            internal const string NTFSFilesBatchStatus = "stlpntfsfilesbatchstatus";
        }

        internal class MongoTableNames
        {
            internal const string NTFSFolders = "ntfsfolders";
            internal const string NTFSFiles = "ntfsfiles";
            internal const string NTFSFilesBatchStatus = "ntfsfilesbatchstatus";
        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = NTFS.AzureTableNames.NTFSFiles;

                    break;

                case "internal.mongodb":

                    tableName = NTFS.MongoTableNames.NTFSFiles;

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Call the update MIME type function
            File.UpdateMIMEType(providerConfig, mimeType, tableName);

            return;
        }

        public static string AddFolder(DataConfig providerConfig, POCO.NTFSFolder ntfsFolder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureNTFSFolder az = new AzureNTFSFolder(ntfsFolder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, NTFS.AzureTableNames.NTFSFolders);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoNTFSFolder> collection = Utils.GetMongoCollection<MongoNTFSFolder>(providerConfig,MongoTableNames.NTFSFolders);
                    MongoNTFSFolder mongoObject = Utils.ConvertType<MongoNTFSFolder>(ntfsFolder);
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
            throw new NotImplementedException();
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            
            DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.NTFSFiles);

            return;
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            DataFactory.File.AddFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.NTFSFilesBatchStatus);

            //TODO return id of new object if supported
            return;
        }

        public static List<POCO.NTFSFolder> GetFolders(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.NTFSFolder> folders = new List<POCO.NTFSFolder>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureNTFSFolder> azdata = new List<AzureNTFSFolder>();
                    AzureTableAdaptor<AzureNTFSFolder> adaptor = new AzureTableAdaptor<AzureNTFSFolder>();
                    azdata = adaptor.ReadTableData(providerConfig, NTFS.AzureTableNames.NTFSFolders, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        folders.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoNTFSFolder>(providerConfig, NTFS.MongoTableNames.NTFSFolders);
                    FilterDefinition<MongoNTFSFolder> filter = Utils.GenerateMongoFilter<MongoNTFSFolder>(filters);

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

        public static bool ForceRescan(DataConfig providerConfig, POCO.NTFSFile file)
        {
            POCO.FileBatchStatus batchstatus = new FileBatchStatus(file.PartitionKey, file.RowKey);
            batchstatus.BatchGuid = Guid.Empty;
            batchstatus.BatchStatus = string.Empty;
            batchstatus.JsonFileProcessResult = "{}";

            DataFactory.File.UpdateFileBatchStatus(providerConfig, batchstatus, "ntfsfiles");

            return true;
        }

            public static List<POCO.NTFSFile> GetFiles(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;
            List<POCO.NTFSFile> files = new List<POCO.NTFSFile>();

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
                    List<AzureNTFSFile> azdata = new List<AzureNTFSFile>();
                    AzureTableAdaptor<AzureNTFSFile> adaptor = new AzureTableAdaptor<AzureNTFSFile>();
                    azdata = adaptor.ReadTableDataWithToken(providerConfig, NTFS.AzureTableNames.NTFSFiles, combinedFilter, rowLimit, thisPageToken, out nextPageToken);

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

                    //throw new NotImplementedException();

                    //string combinedFilter = Utils.GenerateAzureFilter(filters);

                    //List<AzureNTFSFile> azdata = new List<AzureNTFSFile>();
                    //AzureTableAdaptor<AzureNTFSFile> adaptor = new AzureTableAdaptor<AzureNTFSFile>();
                    //azdata = adaptor.ReadTableData(providerConfig, NTFS.AzureTableNames.NTFSFiles, combinedFilter);

                    //foreach (var doc in azdata)
                    //{
                    //    files.Add(doc.Value);
                    //}

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoNTFSFile>(providerConfig, NTFS.MongoTableNames.NTFSFiles);

                    // Add an _id filter if a page has been requested
                    if (thisPageId != null && thisPageId != string.Empty)
                    {
                        filters.Insert(0, new Filter("_id", thisPageId, "gt"));
                    }

                    FilterDefinition<MongoNTFSFile> filter = Utils.GenerateMongoFilter<MongoNTFSFile>(filters);

                    //DEBUG output the filter values
                    //foreach (Castlepoint.DataFactory.Filter debugFilter in filters)
                    //{
                    //    // Output the filter field names and values
                    //    Console.WriteLine("DEBUG filter: " + debugFilter.FieldName + " : " + debugFilter.FieldValue);
                    //}
                    var documents = collection.Find(filter).Sort("{\"_id\":1}").Limit(rowLimit).ToList();


                    foreach (var NTFSFile in documents)
                    {
                        files.Add(NTFSFile);
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

        public static List<POCO.NTFSFile> GetFiles(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.NTFSFile> files = new List<POCO.NTFSFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureNTFSFile> azdata = new List<AzureNTFSFile>();
                    AzureTableAdaptor<AzureNTFSFile> adaptor = new AzureTableAdaptor<AzureNTFSFile>();
                    azdata = adaptor.ReadTableData(providerConfig, NTFS.AzureTableNames.NTFSFiles, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        files.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoNTFSFile>(providerConfig, NTFS.MongoTableNames.NTFSFiles);
                    FilterDefinition<MongoNTFSFile> filter = Utils.GenerateMongoFilter<MongoNTFSFile>(filters);

                    //DEBUG output the filter values
                    //foreach (Castlepoint.DataFactory.Filter debugFilter in filters)
                    //{
                    //    // Output the filter field names and values
                    //    Console.WriteLine("DEBUG filter: " + debugFilter.FieldName + " : " + debugFilter.FieldValue);
                    //}

                    var documents = collection.Find(filter).ToList();

                    foreach (var NTFSFile in documents)
                    {
                        files.Add(NTFSFile);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return files;
        }

        

        public static string AddFile(DataConfig providerConfig, POCO.NTFSFile ntfsFile)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureNTFSFile az = new AzureNTFSFile(ntfsFile);

                    CloudTable table = Utils.GetCloudTable(providerConfig, NTFS.AzureTableNames.NTFSFiles);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoNTFSFile> collection = Utils.GetMongoCollection<MongoNTFSFile>(providerConfig, MongoTableNames.NTFSFiles);
                    MongoNTFSFile mongoObject = Utils.ConvertType<MongoNTFSFile>(ntfsFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.NTFSFileBatchStatus> GetFileBatchStatus(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.NTFSFileBatchStatus> fileBatchStatus = new List<POCO.NTFSFileBatchStatus>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureNTFSFileBatchStatus> azdata = new List<AzureNTFSFileBatchStatus>();
                    AzureTableAdaptor<AzureNTFSFileBatchStatus> adaptor = new AzureTableAdaptor<AzureNTFSFileBatchStatus>();
                    azdata = adaptor.ReadTableData(providerConfig, NTFS.AzureTableNames.NTFSFilesBatchStatus, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        fileBatchStatus.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoNTFSFileBatchStatus>(providerConfig, MongoTableNames.NTFSFilesBatchStatus);
                    FilterDefinition<MongoNTFSFileBatchStatus> filter = Utils.GenerateMongoFilter<MongoNTFSFileBatchStatus>(filters);

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

        public static void UpdateFolder(DataConfig providerConfig, POCO.NTFSFolder ntfsFolder)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureNTFSFolder az = new AzureNTFSFolder(ntfsFolder);

                    CloudTable table = Utils.GetCloudTable(providerConfig, NTFS.AzureTableNames.NTFSFolders);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoNTFSFolderUpsert> collection = Utils.GetMongoCollection<MongoNTFSFolderUpsert>(providerConfig, NTFS.MongoTableNames.NTFSFolders);
                    MongoNTFSFolderUpsert mongoObject = Utils.ConvertType<MongoNTFSFolderUpsert>(ntfsFolder);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoNTFSFolderUpsert> filter = Utils.GenerateMongoFilter<MongoNTFSFolderUpsert>(filters);

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
