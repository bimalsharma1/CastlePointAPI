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

namespace Castlepoint.DataFactory
{
    class MongoGoogleFile : POCO.Files.GoogleFile
    {
        public ObjectId _id { get; set; }
    }

    class AzureGoogleFile : EntityAdapter<POCO.Files.GoogleFile>
    {
        public AzureGoogleFile() { }
        public AzureGoogleFile(POCO.Files.GoogleFile o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class Google
    {
        internal class AzureTableNames
        {
            internal const string GoogleFile = "stlpgooglefiles";
            internal const string GoogleFolder = "stlpgooglefolders";
        }
        internal class MongoTableNames
        {
            internal const string GoogleFile = "googlefiles";
            internal const string GoogleFolder = "googlefolders";
        }
        public static List<POCO.Files.GoogleFile> GetFiles(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.Files.GoogleFile> files = new List<POCO.Files.GoogleFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureGoogleFile> azdata = new List<AzureGoogleFile>();
                    AzureTableAdaptor<AzureGoogleFile> adaptor = new AzureTableAdaptor<AzureGoogleFile>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.GoogleFile, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        files.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoGoogleFile>(providerConfig, MongoTableNames.GoogleFile);

                    FilterDefinition<MongoGoogleFile> filter = Utils.GenerateMongoFilter<MongoGoogleFile>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        files.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return files;
        }

        public static string AddFile(DataConfig providerConfig, POCO.Files.GoogleFile gFile)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureGoogleFile az = new AzureGoogleFile(gFile);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.GoogleFile);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoGoogleFile> collection = Utils.GetMongoCollection<MongoGoogleFile>(providerConfig, MongoTableNames.GoogleFile);
                    MongoGoogleFile mongoObject = Utils.ConvertType<MongoGoogleFile>(gFile);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static void UpdateGoogleFolder(DataConfig providerConfig, POCO.O365.SPFolderUpdate folderUpdate)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSPFolderUpdate az = new AzureSPFolderUpdate(folderUpdate);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.GoogleFolder);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSPFolderUpdate> collection = Utils.GetMongoCollection<MongoSPFolderUpdate>(providerConfig, MongoTableNames.GoogleFolder);
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

                    tableName = AzureTableNames.GoogleFile;

                    break;

                case "internal.mongodb":

                    tableName = MongoTableNames.GoogleFile;

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
