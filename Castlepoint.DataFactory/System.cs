using System;
using System.Collections.Generic;
using System.Text;
using System.Web;
using Castlepoint.POCO;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

namespace Castlepoint.DataFactory
{
    public class MongoSystemBatchConfig : POCO.SystemNodeConfig
    {
        public ObjectId _id { get; set; }
    }

    public class AzureSystemBatchConfig : EntityAdapter<POCO.SystemNodeConfig>
    {
        public AzureSystemBatchConfig() { }
        public AzureSystemBatchConfig(POCO.SystemNodeConfig o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoSystem : POCO.System
    {
        public ObjectId _id { get; set; }
    }

    public class MongoSystemStatUpdate : POCO.System
    {

    }
    public class MongoSystemConfig : POCO.SystemConfigUpdate
    {

    }

    public class MongoConnectionConfig : POCO.ConnectionConfigUpdate
    {

    }

    public class MongoRecordIdentificationConfig : POCO.RecordConfigUpdate
    {

    }

    public class MongoSystemEnabledUpdate : POCO.SystemEnabledUpdate
    {

    }

    public class AzureSystem : EntityAdapter<POCO.System>
    {
        public AzureSystem() { }
        public AzureSystem(POCO.System o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public class AzureSystemStatUpdate : TableEntity
    {
        public AzureSystemStatUpdate(POCO.System system, POCO.SystemStat stat)
        {
            string jsonstat = JsonConvert.SerializeObject(stat);
            this.PartitionKey = system.PartitionKey;
            this.RowKey = system.RowKey;
            this.JsonSystemStats = jsonstat;

        }
        public string JsonSystemStats { get; set; }
    }
    public class AzureSentenceStatUpdate : TableEntity
    {
        public AzureSentenceStatUpdate(POCO.System system, List<POCO.SentenceStat> stats)
        {
            string jsonstat = JsonConvert.SerializeObject(stats);
            this.PartitionKey = system.PartitionKey;
            this.RowKey = system.RowKey;
            this.JsonSentenceStats = jsonstat;

        }
        public string JsonSentenceStats { get; set; }
    }

    public class AzureSystemEnabledUpdate : TableEntity
    {
        public AzureSystemEnabledUpdate(POCO.SystemEnabledUpdate syscfg)
        {
            this.PartitionKey = syscfg.PartitionKey;
            this.RowKey = syscfg.RowKey;
            this.Enabled = syscfg.Enabled;

        }
        public bool Enabled { get; set; }
    }
    public class AzureSystemConfig : TableEntity
    {
        public AzureSystemConfig(POCO.SystemConfigUpdate syscfg)
        {
            this.PartitionKey = syscfg.PartitionKey;
            this.RowKey = syscfg.RowKey;
            this.JsonSystemConfig = syscfg.JsonSystemConfig;

        }
        public string JsonSystemConfig { get; set; }
    }

    public class AzureConnectionConfig : TableEntity
    {
        public AzureConnectionConfig(POCO.ConnectionConfigUpdate syscfg)
        {
            this.PartitionKey = syscfg.PartitionKey;
            this.RowKey = syscfg.RowKey;
            this.JsonConnectionConfig = syscfg.JsonConnectionConfig;

        }
        public string JsonConnectionConfig { get; set; }
    }

    public class AzureRecordIdentificationConfig : TableEntity
    {
        public AzureRecordIdentificationConfig(POCO.RecordConfigUpdate syscfg)
        {
            this.PartitionKey = syscfg.PartitionKey;
            this.RowKey = syscfg.RowKey;
            this.JsonRecordIdentificationConfig = syscfg.JsonRecordIdentificationConfig;
        }
        public string JsonRecordIdentificationConfig { get; set; }
    }

    public static class System
    {
        private static class TableNames
        {
            internal const string System = "stlpsystems";
        }

        public static string Add(DataConfig providerConfig, POCO.System system)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSystem az = new AzureSystem(system);

                    CloudTable table = Utils.GetCloudTable(providerConfig, TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    //TODO return the inserted record id/timestamp
                    return string.Empty;


                case "internal.mongodb":
                    IMongoCollection<MongoSystem> collection = Utils.GetMongoCollection<MongoSystem>(providerConfig, TableNames.System);
                    MongoSystem mongoObject = Utils.ConvertType<MongoSystem>(system);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return string.Empty;
        }

        public static List<POCO.System> Get(DataConfig providerConfig)
        {
            List<POCO.System> systems = new List<POCO.System>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<AzureSystem> azs = new List<AzureSystem>();
                    AzureTableAdaptor<AzureSystem> az = new AzureTableAdaptor<AzureSystem>();
                    azs = az.ReadTableData(providerConfig, System.TableNames.System);

                    foreach (var doc in azs)
                    {
                        systems.Add(doc.Value);
                    }

                    return systems;


                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSystem>(providerConfig, System.TableNames.System);

                    var documents = collection.Find(new BsonDocument()).ToList();

                    foreach (var system in documents)
                    {
                        systems.Add(system);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return systems;
        }

        internal static List<POCO.Files.CPFile> GetFilesAssociatedToRecordForBatchProcessing(DataConfig dataConfig, POCO.System system, POCO.Record record)
        {
            List<POCO.Files.CPFile> filesToProcess = new List<POCO.Files.CPFile>();

            // Create the filter
            string recordUri = record.RowKey;
            if (!recordUri.EndsWith("|")) { recordUri += "|"; } // Standardise the url to end in a pipe '|' to help match files via partial Uri

            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkGTEFilter = new DataFactory.Filter("PartitionKey", recordUri, "ge");
            DataFactory.Filter pkLTFilter = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(recordUri), "lt");
            filters.Add(pkGTEFilter);
            filters.Add(pkLTFilter);

            // Check the system type
            string thisPageId = string.Empty;
            string nextPageId = string.Empty;
            switch (system.Type)
            {
                case POCO.SystemType.SharePointOnline:
                case POCO.SystemType.SharePointTeam:
                case POCO.SystemType.SharePointOneDrive:
                    {

                        do
                        {
                            List<POCO.SPFile> spfiles = DataFactory.SharePointOnline.GetFiles(dataConfig, filters, thisPageId, 1000, out nextPageId);

                            // Convert to ICPFile format
                            foreach (POCO.SPFile fileToConvert in spfiles)
                            {
                                // Convert to ICPFile compatible object
                                POCO.Files.SPFile file = JsonConvert.DeserializeObject<POCO.Files.SPFile>(JsonConvert.SerializeObject(fileToConvert));
                                filesToProcess.Add(file);
                            }

                            thisPageId = nextPageId;
                        }
                        while (nextPageId != string.Empty);


                        break;
                    }

                case POCO.SystemType.GoogleDrive:
                    {

                        List<POCO.Files.GoogleFile> googlefiles = DataFactory.Google.GetFiles(dataConfig, filters);

                        // Convert to ICPFile format
                        foreach (POCO.Files.GoogleFile fileToConvert in googlefiles)
                        {
                            // Convert to ICPFile compatible object
                            POCO.Files.GoogleFile file = JsonConvert.DeserializeObject<POCO.Files.GoogleFile>(JsonConvert.SerializeObject(fileToConvert));
                            filesToProcess.Add(file);
                        }

                        break;
                    }
                case POCO.SystemType.NTFSShare:

                    List<POCO.NTFSFile> ntfsFiles = DataFactory.NTFS.GetFiles(dataConfig, filters);

                    // Convert to ICPFile format
                    foreach (POCO.NTFSFile fileToConvert in ntfsFiles)
                    {
                        // Convert to ICPFile compatible object
                        POCO.Files.NTFSFile ntfsfile = JsonConvert.DeserializeObject<POCO.Files.NTFSFile>(JsonConvert.SerializeObject(fileToConvert));
                        filesToProcess.Add(ntfsfile);
                    }

                    break;
                case POCO.SystemType.Basecamp:
                    // Get the Basecamp files

                    // URL format of the basecamp project is:  https://<basecamp api>/<account number>/projects/<project id>.json
                    // all data for a project is in "buckets": https://<basecamp api>/<account number>/buckets/<project id>/<type of bucket>/<id>.json
                    // any data matching should use the buckets/<project id>>
                    string basecampFilesUri = record.RecordUri.Replace("projects", "buckets");
                    if (basecampFilesUri.ToLower().EndsWith(".json")) { basecampFilesUri = basecampFilesUri.Substring(0, basecampFilesUri.Length - 5); }
                    if (!basecampFilesUri.EndsWith("|")) { basecampFilesUri += "|"; }

                    List<DataFactory.Filter> basecampFilters = new List<DataFactory.Filter>();

                    string pk = Utils.CleanTableKey(basecampFilesUri);
                    DataFactory.Filter basecampPK = new DataFactory.Filter("PartitionKey", pk, "eq");
                    basecampFilters.Add(basecampPK);

                    //DataFactory.Filter basecampPKLT = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(pk), "lt");
                    //basecampFilters.Add(basecampPKGE);
                    //basecampFilters.Add(basecampPKLT);

                    DataFactory.DataConfig basecampDataConfig = dataConfig;
                    List<POCO.BasecampDocumentEntity> basecampFiles = DataFactory.BasecampProject.GetFiles(basecampDataConfig, basecampFilters);

                    // Convert to ICPFile format
                    foreach (POCO.BasecampDocumentEntity fileToConvert in basecampFiles)
                    {
                        // Convert to ICPFile compatible object
                        //BasecampFile basefile = JsonConvert.DeserializeObject<BasecampFile>(JsonConvert.SerializeObject(fileToConvert));

                        POCO.Files.BasecampFile bf = new POCO.Files.BasecampFile();
                        bf.BatchGuid = Guid.Empty;
                        bf.BatchStatus = null;
                        bf.CreationTime = fileToConvert.TimeCreated;
                        bf.FolderUri = fileToConvert.PartitionKey;
                        bf.ItemUri = fileToConvert.ServerRelativeUrl;
                        bf.JsonFileProcessResult = null;
                        bf.LastModifiedTime = fileToConvert.TimeLastModified;
                        bf.Name = fileToConvert.Name;
                        bf.OrganisationId = Guid.Empty;
                        bf.PartitionKey = fileToConvert.PartitionKey;
                        bf.RowKey = fileToConvert.RowKey;
                        bf.SiteUrl = fileToConvert.ServerRelativeUrl;
                        bf.SourceFileName = fileToConvert.Name;
                        bf.SourceRelativeUrl = fileToConvert.ServerRelativeUrl;
                        bf.Title = fileToConvert.Title;
                        bf.UniqueId = fileToConvert.UniqueId;

                        filesToProcess.Add(bf);
                    }

                    break;
                default:
                    {
                        throw new NotImplementedException("Unknown system type: " + system.Type);
                    }
            }

            return filesToProcess;

        }



        public static List<POCO.Files.CPFile> GetFiles(DataConfig dataConfig, POCO.System system, List<Filter> filters, string thisPageId, int maxRows, out string nextPageId)
        {
            List<POCO.Files.CPFile> filesToProcess = new List<POCO.Files.CPFile>();

            // Default next page id to "no more data" (empty string)
            nextPageId = string.Empty;

            // Check the system type
            switch (system.Type)
            {
                case POCO.SystemType.SharePoint2010:
                    {
                        List<POCO.SharePoint.SPFile> spfiles = DataFactory.SharePoint.GetFiles(dataConfig, filters, thisPageId, maxRows, out nextPageId);

                        // Convert to ICPFile format
                        foreach (POCO.SharePoint.SPFile fileToConvert in spfiles)
                        {
                            // Convert to ICPFile compatible object
                            POCO.Files.SPFile file = JsonConvert.DeserializeObject<POCO.Files.SPFile>(JsonConvert.SerializeObject(fileToConvert));
                            filesToProcess.Add(file);
                        }

                        break;
                    }
                case POCO.SystemType.SharePoint2013:
                    {
                        List<POCO.SharePoint.SPFile> spfiles = DataFactory.SharePoint.GetFiles(dataConfig, filters, thisPageId, maxRows, out nextPageId);

                        // Convert to ICPFile format
                        foreach (POCO.SharePoint.SPFile fileToConvert in spfiles)
                        {
                            // Convert to ICPFile compatible object
                            POCO.Files.SPFile file = JsonConvert.DeserializeObject<POCO.Files.SPFile>(JsonConvert.SerializeObject(fileToConvert));
                            filesToProcess.Add(file);
                        }

                        break;
                    }
                case POCO.SystemType.SharePointOneDrive:
                case POCO.SystemType.SharePointTeam:
                case POCO.SystemType.SharePointOnline:
                    {
                        List<POCO.SPFile> spfiles = DataFactory.SharePointOnline.GetFiles(dataConfig, filters, thisPageId, maxRows, out nextPageId);

                        // Convert to ICPFile format
                        foreach (POCO.SPFile fileToConvert in spfiles)
                        {
                            // Convert to ICPFile compatible object
                            POCO.Files.SPFile file = JsonConvert.DeserializeObject<POCO.Files.SPFile>(JsonConvert.SerializeObject(fileToConvert));
                            filesToProcess.Add(file);
                        }

                        break;
                    }
                case POCO.SystemType.GoogleDrive:
                    {

                        List<POCO.Files.GoogleFile> googlefiles = DataFactory.Google.GetFiles(dataConfig, filters);

                        // Convert to ICPFile format
                        foreach (POCO.Files.GoogleFile fileToConvert in googlefiles)
                        {
                            // Convert to ICPFile compatible object
                            POCO.Files.GoogleFile file = JsonConvert.DeserializeObject<POCO.Files.GoogleFile>(JsonConvert.SerializeObject(fileToConvert));
                            filesToProcess.Add(file);
                        }

                        break;
                    }
                case POCO.SystemType.NTFSShare:

                    List<POCO.NTFSFile> ntfsFiles = DataFactory.NTFS.GetFiles(dataConfig, filters);

                    // Convert to ICPFile format
                    foreach (POCO.NTFSFile fileToConvert in ntfsFiles)
                    {
                        // Convert to ICPFile compatible object
                        POCO.Files.NTFSFile ntfsfile = JsonConvert.DeserializeObject<POCO.Files.NTFSFile>(JsonConvert.SerializeObject(fileToConvert));
                        filesToProcess.Add(ntfsfile);
                    }

                    break;
                case POCO.SystemType.SakaiAlliance:

                    List<POCO.SakaiFile> sakaiFiles = DataFactory.SakaiResource.GetFiles(dataConfig, filters);

                    // Convert to ICPFile format
                    foreach (POCO.SakaiFile fileToConvert in sakaiFiles)
                    {
                        // Convert to ICPFile compatible object
                        POCO.Files.SakaiFile sakaifile = JsonConvert.DeserializeObject<POCO.Files.SakaiFile>(JsonConvert.SerializeObject(fileToConvert));
                        filesToProcess.Add(sakaifile);
                    }

                    break;
                case POCO.SystemType.Basecamp:
                    // Get the Basecamp files

                    throw new NotImplementedException("GetFiles: system type not implemented=" + system.Type);

                    // URL format of the basecamp project is:  https://<basecamp api>/<account number>/projects/<project id>.json
                    // all data for a project is in "buckets": https://<basecamp api>/<account number>/buckets/<project id>/<type of bucket>/<id>.json
                    // any data matching should use the buckets/<project id>>
                    //string basecampFilesUri = record.RecordUri.Replace("projects", "buckets");
                    //if (basecampFilesUri.ToLower().EndsWith(".json")) { basecampFilesUri = basecampFilesUri.Substring(0, basecampFilesUri.Length - 5); }
                    //if (!basecampFilesUri.EndsWith("|")) { basecampFilesUri += "|"; }

                    //List<DataFactory.Filter> basecampFilters = new List<DataFactory.Filter>();

                    //string pk = Utils.CleanTableKey(basecampFilesUri);
                    //DataFactory.Filter basecampPK = new DataFactory.Filter("PartitionKey", pk, "eq");
                    //basecampFilters.Add(basecampPK);

                    ////DataFactory.Filter basecampPKLT = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(pk), "lt");
                    ////basecampFilters.Add(basecampPKGE);
                    ////basecampFilters.Add(basecampPKLT);

                    //DataFactory.DataConfig basecampDataConfig = dataConfig;
                    //List<POCO.BasecampDocumentEntity> basecampFiles = DataFactory.BasecampProject.GetFiles(basecampDataConfig, basecampFilters);

                    //// Convert to ICPFile format
                    //foreach (POCO.BasecampDocumentEntity fileToConvert in basecampFiles)
                    //{
                    //    // Convert to ICPFile compatible object
                    //    //BasecampFile basefile = JsonConvert.DeserializeObject<BasecampFile>(JsonConvert.SerializeObject(fileToConvert));

                    //    POCO.Files.BasecampFile bf = new POCO.Files.BasecampFile();
                    //    bf.BatchGuid = Guid.Empty;
                    //    bf.BatchStatus = null;
                    //    bf.CreationTime = fileToConvert.TimeCreated;
                    //    bf.FolderUri = fileToConvert.PartitionKey;
                    //    bf.ItemUri = fileToConvert.ServerRelativeUrl;
                    //    bf.JsonFileProcessResult = null;
                    //    bf.LastModifiedTime = fileToConvert.TimeLastModified;
                    //    bf.Name = fileToConvert.Name;
                    //    bf.OrganisationId = Guid.Empty;
                    //    bf.PartitionKey = fileToConvert.PartitionKey;
                    //    bf.RowKey = fileToConvert.RowKey;
                    //    bf.SiteUrl = fileToConvert.ServerRelativeUrl;
                    //    bf.SourceFileName = fileToConvert.Name;
                    //    bf.SourceRelativeUrl = fileToConvert.ServerRelativeUrl;
                    //    bf.Title = fileToConvert.Title;
                    //    bf.UniqueId = fileToConvert.UniqueId;

                    //    filesToProcess.Add(bf);
                    //}

                    break;
                default:
                    {
                        throw new NotImplementedException("GetFiles: unknown system type: " + system.Type);
                    }
            }

            return filesToProcess;

        }

        public static List<POCO.System> GetSystems(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.System> systems = new List<POCO.System>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureSystem> azs = new List<AzureSystem>();
                    AzureTableAdaptor<AzureSystem> az = new AzureTableAdaptor<AzureSystem>();
                    azs = az.ReadTableData(providerConfig, System.TableNames.System, combinedFilter);

                    foreach (var doc in azs)
                    {
                        systems.Add(doc.Value);
                    }

                    return systems;


                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSystem>(providerConfig, System.TableNames.System);
                    FilterDefinition<MongoSystem> filter = Utils.GenerateMongoFilter<MongoSystem>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var system in documents)
                    {
                        systems.Add(system);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return systems;
        }

        public static List<POCO.System> GetAllSystems(DataConfig providerConfig)
        {
            List<POCO.System> systems = new List<POCO.System>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<AzureSystem> azs = new List<AzureSystem>();
                    AzureTableAdaptor<AzureSystem> az = new AzureTableAdaptor<AzureSystem>();
                    azs = az.ReadTableData(providerConfig, System.TableNames.System);

                    foreach (var doc in azs)
                    {
                        systems.Add(doc.Value);
                    }

                    return systems;


                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSystem>(providerConfig, System.TableNames.System);

                    var documents = collection.Find(new BsonDocument()).ToList();

                    foreach (var system in documents)
                    {
                        systems.Add(system);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return systems;
        }

        public static List<POCO.SystemNodeConfig> GetAllBatchNodeConfig(DataConfig providerConfig)
        {
            List<POCO.SystemNodeConfig> systems = new List<POCO.SystemNodeConfig>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<AzureSystemBatchConfig> azs = new List<AzureSystemBatchConfig>();
                    AzureTableAdaptor<AzureSystemBatchConfig> az = new AzureTableAdaptor<AzureSystemBatchConfig>();
                    azs = az.ReadTableData(providerConfig, POCO.TableNames.Azure.System.BatchConfig);

                    foreach (var doc in azs)
                    {
                        systems.Add(doc.Value);
                    }

                    return systems;


                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSystemBatchConfig>(providerConfig, POCO.TableNames.Mongo.System.BatchConfig);

                    var documents = collection.Find(new BsonDocument()).ToList();

                    foreach (var system in documents)
                    {
                        systems.Add(system);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return systems;
        }

        public static void UpdateSystemStat(DataConfig providerConfig, POCO.System system, POCO.SystemStat systemStat)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureSystemStatUpdate az = new AzureSystemStatUpdate(system, systemStat);

                    CloudTable table = Utils.GetCloudTable(providerConfig, System.TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSystemStatUpdate> collection = Utils.GetMongoCollection<MongoSystemStatUpdate>(providerConfig, "stlpsystems");
                    MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(system);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                    //DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                    filters.Add(pkFilter);
                    //filters.Add(rkFilter);
                    FilterDefinition<MongoSystemStatUpdate> filter = Utils.GenerateMongoFilter<MongoSystemStatUpdate>(filters);

                    // Serialize the stats object
                    string jsonStatsSerialized = JsonConvert.SerializeObject(systemStat);

                    //string updateParam = "{$set: {JsonSystemStats: '" + jsonStatsSerialized + "'}}";
                    //BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    var update = Builders<MongoSystemStatUpdate>.Update
                        .Set("JsonSystemStats", jsonStatsSerialized);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static void UpdateSentenceStat(DataConfig providerConfig, POCO.System system, List<POCO.SentenceStat> sentenceStats)
        {

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSentenceStatUpdate az = new AzureSentenceStatUpdate(system, sentenceStats);

                    CloudTable table = Utils.GetCloudTable(providerConfig, System.TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSystemStatUpdate> collection = Utils.GetMongoCollection<MongoSystemStatUpdate>(providerConfig, "stlpsystems");
                    //MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(systemStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(system.PartitionKey), "eq");
                    //DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(system.RowKey), "eq");
                    filters.Add(pkFilter);
                    //filters.Add(rkFilter);
                    FilterDefinition<MongoSystemStatUpdate> filter = Utils.GenerateMongoFilter<MongoSystemStatUpdate>(filters);

                    // Serialize the stats object
                    string jsonStatsSerialized = JsonConvert.SerializeObject(sentenceStats);

                    //string updateParam = "{$set: {JsonSentenceStats: '" + jsonStatsSerialized + "'}}";
                    //BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    var update = Builders<MongoSystemStatUpdate>.Update
                        .Set("JsonSentenceStats", jsonStatsSerialized);


                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }


        public static bool UpdateSystemConfig(DataConfig providerConfig, POCO.System system, POCO.SystemConfig config)
        {
            bool isUpdatedOk = false;

            POCO.SystemConfigUpdate updateSysCfg = new SystemConfigUpdate();
            updateSysCfg.PartitionKey = system.PartitionKey;
            updateSysCfg.RowKey = system.RowKey;
            updateSysCfg.JsonSystemConfig = JsonConvert.SerializeObject(config);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSystemConfig az = new AzureSystemConfig(updateSysCfg);

                    CloudTable table = Utils.GetCloudTable(providerConfig, System.TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    isUpdatedOk = true;

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSystemConfig> collection = Utils.GetMongoCollection<MongoSystemConfig>(providerConfig, "stlpsystems");
                    //MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(systemStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", updateSysCfg.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", updateSysCfg.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSystemConfig> filter = Utils.GenerateMongoFilter<MongoSystemConfig>(filters);

                    var update = Builders<MongoSystemConfig>.Update
                        .Set("JsonSystemConfig", updateSysCfg.JsonSystemConfig);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    isUpdatedOk = true;

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return isUpdatedOk;
        }

        public static bool UpdateConnectionConfig(DataConfig providerConfig, POCO.System system, string config)
        {
            bool isUpdatedOk = false;

            POCO.ConnectionConfigUpdate updateConnCfg = new ConnectionConfigUpdate();
            updateConnCfg.PartitionKey = system.PartitionKey;
            updateConnCfg.RowKey = system.RowKey;
            updateConnCfg.JsonConnectionConfig = config;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureConnectionConfig az = new AzureConnectionConfig(updateConnCfg);

                    CloudTable table = Utils.GetCloudTable(providerConfig, System.TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    isUpdatedOk = true;

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoConnectionConfig> collection = Utils.GetMongoCollection<MongoConnectionConfig>(providerConfig, System.TableNames.System);
                    //MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(systemStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", updateConnCfg.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", updateConnCfg.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoConnectionConfig> filter = Utils.GenerateMongoFilter<MongoConnectionConfig>(filters);

                    var update = Builders<MongoConnectionConfig>.Update
                        .Set("JsonConnectionConfig", updateConnCfg.JsonConnectionConfig);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    isUpdatedOk = true;

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return isUpdatedOk;
        }

        public static bool UpdateRecordConfig(DataConfig providerConfig, POCO.System system, string config)
        {
            bool isUpdatedOk = false;

            POCO.RecordConfigUpdate updateRecordCfg = new RecordConfigUpdate();
            updateRecordCfg.PartitionKey = system.PartitionKey;
            updateRecordCfg.RowKey = system.RowKey;
            updateRecordCfg.JsonRecordIdentificationConfig = config;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureRecordIdentificationConfig az = new AzureRecordIdentificationConfig(updateRecordCfg);

                    CloudTable table = Utils.GetCloudTable(providerConfig, System.TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    isUpdatedOk = true;

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoRecordIdentificationConfig> collection = Utils.GetMongoCollection<MongoRecordIdentificationConfig>(providerConfig, System.TableNames.System);
                    //MongoSystemStatUpdate mongoObject = Utils.ConvertType<MongoSystemStatUpdate>(systemStat);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", updateRecordCfg.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", updateRecordCfg.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoRecordIdentificationConfig> filter = Utils.GenerateMongoFilter<MongoRecordIdentificationConfig>(filters);

                    var update = Builders<MongoRecordIdentificationConfig>.Update
                        .Set("JsonRecordIdentificationConfig", updateRecordCfg.JsonRecordIdentificationConfig);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    isUpdatedOk = true;

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return isUpdatedOk;
        }

        public static bool SetConfigEnabled(DataConfig providerConfig, string systemPartitionKey, string systemRowKey, bool enableSystem)
        {
            bool isUpdatedOk = false;

            POCO.SystemEnabledUpdate updateEnabled = new POCO.SystemEnabledUpdate();
            updateEnabled.PartitionKey = systemPartitionKey;
            updateEnabled.RowKey = systemRowKey;
            updateEnabled.Enabled = enableSystem;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureSystemEnabledUpdate az = new AzureSystemEnabledUpdate(updateEnabled);

                    CloudTable table = Utils.GetCloudTable(providerConfig, System.TableNames.System);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    isUpdatedOk = true;

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSystemEnabledUpdate> collection = Utils.GetMongoCollection<MongoSystemEnabledUpdate>(providerConfig, System.TableNames.System);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", updateEnabled.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", updateEnabled.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSystemEnabledUpdate> filter = Utils.GenerateMongoFilter<MongoSystemEnabledUpdate>(filters);

                    var update = Builders<MongoSystemEnabledUpdate>.Update
                        .Set("Enabled", updateEnabled.Enabled);

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, update);

                    isUpdatedOk = true;

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return isUpdatedOk;
        }
    }
}
