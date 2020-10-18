using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    class MongoSakaiSite:POCO.SakaiSiteEntity
    {

    }
    class MongoSakaiDocument : POCO.SakaiDocumentEntity
    {

    }
    class MongoSakaiFileBatchStatus_Get : POCO.SakaiFileBatchStatus
    {
        public ObjectId _id { get; set; }
    }

    class MongoSakaiSite_Get : POCO.SakaiSiteEntity
    {
        public ObjectId _id { get; set; }
    }
    class MongoSakaiDocument_Get : POCO.SakaiDocumentEntity
    {
        public ObjectId _id { get; set; }

        public string BatchGuid { get; set; }
        public string BatchStatus { get; set; }
        public string JsonFileProcessResult { get; set; }
    }

    class MongoSakaiUpdatePatch001 : POCO.Sakai_Patch001
    {

    }

    class MongoSakaiUpdatePatch002 : POCO.Sakai_Patch001
    {

    }

    class MongoSakaiUpdatePatch003 : POCO.Sakai_Patch003
    {

    }

    class MongoSakaiUpdateSourceRelativeUrl: POCO.SakaiFile_Patch001
    {

    }

    public static class SakaiSite
    {
        internal static class AzureTableNames
        {
            internal const string SakaiFolders = "stlpsakaifolders";
            internal static string SakaiSites { get { return "sakaisites"; } }
            internal static string SakaiFiles { get { return "sakaifiles"; } }
            internal static string SakaiFilesBatchStatus { get { return "sakaifilesbatchstatus"; } }
        }
        internal static class MongoTableNames
        {
            internal const string SakaiFolders = "sakaifolders";
            internal static string SakaiSites { get { return "sakaisites"; } }
            internal static string SakaiFiles { get { return "sakaifiles"; } }
            internal static string SakaiFilesBatchStatus { get { return "sakaifilesbatchstatus"; } }
        }

        public static List<POCO.SakaiSiteEntity> GetSites(DataConfig providerConfig, POCO.System system, POCO.SakaiSite site)
        {
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", Utils.CleanTableKey(system.PartitionKey), "eq");
            filters.Add(pkFilter);
            Filter rkFilter = new Filter("RowKey", Utils.CleanTableKey(site.SITE_ID), "eq");
            filters.Add(rkFilter);

            return SakaiSite.GetSites(providerConfig, filters);
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            // Call the datafactory generic Add
            File.AddFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.SakaiFiles);

            return;
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, MongoTableNames.SakaiFilesBatchStatus);

            return;
        }

        public static List<POCO.SakaiSiteEntity> GetSites(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.SakaiSiteEntity> sites = new List<POCO.SakaiSiteEntity>();


            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    //CloudTable table = Utils.GetCloudTable(cpConfig, "stlprecords", _logger);

                    //string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
                    //string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, Utils.CleanTableKey(rowKey));
                    //string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

                    //TableQuery<RecordEntity> query = new TableQuery<RecordEntity>().Where(combinedFilter);

                    //TableContinuationToken token = null;

                    //var runningQuery = new TableQuery<RecordEntity>()
                    //{
                    //    FilterString = query.FilterString,
                    //    SelectColumns = query.SelectColumns
                    //};

                    //do
                    //{
                    //    runningQuery.TakeCount = query.TakeCount - recordEntities.Count;

                    //    Task<TableQuerySegment<RecordEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordEntity>(runningQuery, token);
                    //    tSeg.Wait();
                    //    token = tSeg.Result.ContinuationToken;
                    //    recordEntities.AddRange(tSeg.Result);

                    //} while (token != null && (query.TakeCount == null || recordEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiSite_Get>(providerConfig, MongoTableNames.SakaiSites);
                    FilterDefinition<MongoSakaiSite_Get> filter = Utils.GenerateMongoFilter<MongoSakaiSite_Get>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var o in documents)
                    {
                        sites.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return sites;

        }

        public static List<POCO.SakaiFile> GetFiles(DataConfig providerConfig, List<Filter> filters, string thisPageId, int rowLimit, out string nextPageId)
        {
            nextPageId = string.Empty;
            List<POCO.SakaiFile> files = new List<POCO.SakaiFile>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFile>(providerConfig, MongoTableNames.SakaiFiles);

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


                    foreach (var sakaifile in documents)
                    {
                        files.Add(sakaifile);
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

                    throw new NotImplementedException();

                    //CloudTable table = Utils.GetCloudTable(cpConfig, "stlprecords", _logger);

                    //string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
                    //string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, Utils.CleanTableKey(rowKey));
                    //string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

                    //TableQuery<RecordEntity> query = new TableQuery<RecordEntity>().Where(combinedFilter);

                    //TableContinuationToken token = null;

                    //var runningQuery = new TableQuery<RecordEntity>()
                    //{
                    //    FilterString = query.FilterString,
                    //    SelectColumns = query.SelectColumns
                    //};

                    //do
                    //{
                    //    runningQuery.TakeCount = query.TakeCount - recordEntities.Count;

                    //    Task<TableQuerySegment<RecordEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordEntity>(runningQuery, token);
                    //    tSeg.Wait();
                    //    token = tSeg.Result.ContinuationToken;
                    //    recordEntities.AddRange(tSeg.Result);

                    //} while (token != null && (query.TakeCount == null || recordEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFile>(providerConfig, MongoTableNames.SakaiFiles);

                    FilterDefinition<MongoSakaiFile> filter = Utils.GenerateMongoFilter<MongoSakaiFile>(filters);

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

        public static void UpdateSiteStatus(DataFactory.DataConfig providerConfig, POCO.System system, POCO.SakaiSite site, string cpStatus)
        {


            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<SakaiSiteEntity>(providerConfig, MongoTableNames.SakaiSites);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(system.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(site.SITE_ID), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<SakaiSiteEntity> filter = Utils.GenerateMongoFilter<SakaiSiteEntity>(filters);

                    string updateParam = "{$set: {TimeCreated: '" + site.CREATEDON.ToUniversalTime().ToString(Utils.ISODateFormat) + "', TimeLastModified: '" + site.MODIFIEDON.ToUniversalTime().ToString(Utils.ISODateFormat) + "', CPStatus: '" + cpStatus + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update the batch status
                    collection.UpdateOne(filter, updateDoc);
                    break;
            }
        }

        public static long PATCH003_UpdateSakaiFileItemUri(DataConfig providerConfig, POCO.SakaiFile file)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSakaiUpdatePatch003> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch003>(providerConfig, MongoTableNames.SakaiFiles);

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(file.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(file.RowKey), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch003> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch003>(filters);

                    //string updateParam = "{$set: {ItemUri: '" + file.SourceRelativeUrl.ToString() + "'}}";
                    //BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    var updateDoc = Builders<MongoSakaiUpdatePatch003>.Update
                                    .Set("ItemUri", file.SourceRelativeUrl.ToString());

                    // Update the batch status
                    UpdateResult result = collection.UpdateOne(filter, updateDoc);

                    if (result.IsAcknowledged && result.IsModifiedCountAvailable)
                    {
                        return result.ModifiedCount;
                    }
                    else
                    {
                        return 0;
                    }

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return 0;
        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = SakaiSite.AzureTableNames.SakaiFiles;

                    break;

                case "internal.mongodb":

                    tableName = SakaiSite.MongoTableNames.SakaiFiles;

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Call the update MIME type function
            File.UpdateMIMEType(providerConfig, mimeType, tableName);

            return;
        }

        public static string AddSite(DataConfig providerConfig, POCO.System system, POCO.SakaiSite sourceSite)
        {

            POCO.SakaiSiteEntity siteEntity = new SakaiSiteEntity(Utils.CleanTableKey(system.PartitionKey), Utils.CleanTableKey(sourceSite.SITE_ID));
            siteEntity.CPStatus = string.Empty;
            siteEntity.ItemCount = 0;
            siteEntity.Name = sourceSite.TITLE;
            siteEntity.ServerRelativeUrl = sourceSite.SITE_ID;
            siteEntity.TimeCreated = sourceSite.CREATEDON.ToUniversalTime();
            siteEntity.TimeLastModified = sourceSite.MODIFIEDON.ToUniversalTime();

            return SakaiSite.AddSite(providerConfig, siteEntity);
        }

        public static string AddSite(DataConfig providerConfig, POCO.SakaiSiteEntity siteEntity)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":


                    //try
                    //{
                    //    CloudTable table = Utils.GetCloudTable(cpConfig, "stlpntfsfolders", _logger);

                    //    //log.Info("Creating record entity");

                    //    // Create the TableOperation that inserts or merges the entry. 
                    //    //log.Verbose("Creating table operation");
                    //    TableOperation insertMergeOperation = TableOperation.InsertOrMerge(folderEntity);

                    //    // Execute the insert operation. 
                    //    //log.Verbose("Executing table operation");
                    //    Task tResult = table.ExecuteAsync(insertMergeOperation);
                    //    tResult.Wait();

                    //}
                    //catch (Microsoft.WindowsAzure.Storage.StorageException ex)
                    //{
                    //    var requestInformation = ex.RequestInformation;
                    //    Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

                    //    // get more details about the exception 
                    //    var information = requestInformation.ExtendedErrorInformation;
                    //    // if you have aditional information, you can use it for your logs
                    //    if (information != null)
                    //    {
                    //        var errorCode = information.ErrorCode;
                    //        var errMessage = string.Format("({0}) {1}",
                    //        errorCode,
                    //        information.ErrorMessage);
                    //        //    var errDetails = information
                    //        //.AdditionalDetails
                    //        //.Aggregate("", (s, pair) =>
                    //        //{
                    //        //    return s + string.Format("{0}={1},", pair.Key, pair.Value);
                    //        //});
                    //        _logger.LogInformation(errMessage);
                    //    }

                    //}
                    //catch (Exception ex)
                    //{
                    //    _logger.LogError("AddFolder - Error adding entry to table: " + ex.Message);
                    //}
                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoSakaiSite> collection = Utils.GetMongoCollection<MongoSakaiSite>(providerConfig, MongoTableNames.SakaiSites);
                    MongoSakaiSite mongoObject = Utils.ConvertType<MongoSakaiSite>(siteEntity);
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
                    //TODO - azure implementation
                    throw new NotImplementedException();
                    //CloudTable table = Utils.GetCloudTable(cpConfig, "ntfsfilesbatchstatus", _logger);

                    //// Create a default query
                    //string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Utils.CleanTableKey(this.PartitionKey));
                    //string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, Utils.CleanTableKey(this.RowKey));
                    //string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

                    //TableQuery<BatchService.Service.Files.FileBatchStatus> query = new TableQuery<BatchService.Service.Files.FileBatchStatus>().Where(combinedFilter);

                    //TableContinuationToken token = null;

                    //var runningQuery = new TableQuery<BatchService.Service.Files.FileBatchStatus>()
                    //{
                    //    FilterString = query.FilterString,
                    //    SelectColumns = query.SelectColumns
                    //};

                    //do
                    //{
                    //    runningQuery.TakeCount = query.TakeCount - fileBatchStatusList.Count;

                    //    Task<TableQuerySegment<Service.Files.FileBatchStatus>> tSeg = table.ExecuteQuerySegmentedAsync<Service.Files.FileBatchStatus>(runningQuery, token);
                    //    tSeg.Wait();
                    //    token = tSeg.Result.ContinuationToken;
                    //    fileBatchStatusList.AddRange(tSeg.Result);

                    //} while (token != null && (query.TakeCount == null || fileBatchStatusList.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

                    //if (fileBatchStatusList.Count == 1)
                    //{
                    //    return fileBatchStatusList[0].BatchStatus;
                    //}
                    //else
                    //{
                    //    return string.Empty;
                    //}

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoSakaiFileBatchStatus_Get>(providerConfig, MongoTableNames.SakaiFilesBatchStatus);

                    FilterDefinition<MongoSakaiFileBatchStatus_Get> filter = Utils.GenerateMongoFilter<MongoSakaiFileBatchStatus_Get>(filters);

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

        public static string AddFile(DataConfig providerConfig, POCO.System system, POCO.SakaiSite sourceSite, POCO.SakaiContentResource sourceDocument)
        {
            // Set the PartitionKey and RowKey
            string sakaiUIDocument = "/" + sourceSite.SITE_ID + sourceDocument.RESOURCE_ID;
            string pkey = Utils.CleanTableKey(sakaiUIDocument);
            string rkey = Utils.CleanTableKey(sourceDocument.TimeLastModified.ToString(Utils.ISODateFormat));

            // Get the file name from the file path
            string fileName = sourceDocument.RESOURCE_ID.Substring(sourceDocument.RESOURCE_ID.LastIndexOf("/") + 1);

            POCO.SakaiFile sakaifile = new SakaiFile(pkey, rkey);
            //sakaifile.BatchGuid = string.Empty;
            sakaifile.BatchStatus = string.Empty;
            sakaifile.CPFolderStatus = string.Empty;
            sakaifile.CreationTime = sourceDocument.TimeCreated;
            sakaifile.ItemCount = 0;
            sakaifile.ItemUri = sakaiUIDocument;
            sakaifile.LastModifiedTime = sourceDocument.TimeLastModified;
            sakaifile.Name = fileName;
            sakaifile.ServerRelativeUrl = sourceDocument.FILE_PATH;
            sakaifile.SizeInBytes = sourceDocument.FILE_SIZE;
            sakaifile.SourceFileName = fileName;
            sakaifile.SourceRelativeUrl= sakaiUIDocument;
            sakaifile.UniqueId = sourceDocument.RESOURCE_UUID;
            sakaifile.Version = 0;

            //POCO.SakaiDocumentEntity docEntity = new SakaiDocumentEntity(Utils.CleanTableKey(sourceSite.SITE_ID), Utils.CleanTableKey(sourceDocument.FILE_PATH));
            //docEntity.CPStatus = string.Empty;
            //docEntity.UniqueId = sourceDocument.RESOURCE_ID;
            //docEntity.ItemCount = 0;
            //docEntity.Name = fileName;
            //docEntity.ServerRelativeUrl = sourceDocument.FILE_PATH;
            //docEntity.TimeCreated = sourceDocument.TimeCreated;
            //docEntity.TimeLastModified = sourceDocument.TimeLastModified;
            //docEntity.Title = fileName;

            return SakaiSite.AddFile(providerConfig, sakaifile);
        }

        public static string AddFile(DataConfig providerConfig, POCO.SakaiFile document)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;                    


                case "internal.mongodb":
                    IMongoCollection<MongoSakaiFile> collection = Utils.GetMongoCollection<MongoSakaiFile>(providerConfig, MongoTableNames.SakaiFiles);
                    MongoSakaiFile mongoObject = Utils.ConvertType<MongoSakaiFile>(document);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static void Patch001_UpdateSakaiFile(DataConfig providerConfig, POCO.SakaiSite sakaiSite, POCO.SakaiContentResource sakaiResource, POCO.SakaiFile currentSakaiFile, string sakaiUIPath)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break; 


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", currentSakaiFile.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", currentSakaiFile.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSakaiUpdateSourceRelativeUrl> filter = Utils.GenerateMongoFilter<MongoSakaiUpdateSourceRelativeUrl>(filters);

                    // Create the update
                    string updateParam = "{$set: {RowKey: '" + Utils.CleanTableKey(sakaiUIPath) + "', SourceRelativeUrl: '" + sakaiUIPath + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdateSourceRelativeUrl> collection = Utils.GetMongoCollection<MongoSakaiUpdateSourceRelativeUrl>(providerConfig, MongoTableNames.SakaiFiles);
                    UpdateResult result = collection.UpdateOne(filter, updateDoc);
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void Patch001_UpdateRecordToRecordAssociation(DataConfig providerConfig, POCO.SakaiSite sakaiSite, POCO.SakaiContentResource sakaiResource, POCO.SakaiFile currentSakaiFile, string sakaiUIPath)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(sakaiSite.SITE_ID), "eq");
                    filters.Add(pkFilter);
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", currentSakaiFile.RowKey, "eq");
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch001> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch001>(filters);

                    // Create the update
                    string updateParam = "{$set: {RowKey: '" + Utils.CleanTableKey(sakaiUIPath) + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdatePatch001> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch001>(providerConfig, Record.MongoTableNames.RecordToRecordAssociation);
                    UpdateResult result = collection.UpdateOne(filter, updateDoc);
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void Patch001_UpdateFileHash(DataConfig providerConfig, POCO.SakaiSite sakaiSite, SakaiContentResource sakaiResource, SakaiFile currentSakaiFile, string sakaiUIPath)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(currentSakaiFile.RowKey), "eq");
                    filters.Add(pkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch001> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch001>(filters);

                    // Create the update
                    string updateParam = "{$set: {PartitionKey: '" + Utils.CleanTableKey(sakaiUIPath) + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdatePatch001> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch001>(providerConfig, POCO.TableNames.Mongo.FileHashTableNames.FileHash);
                    UpdateResult result = collection.UpdateOne(filter, updateDoc);
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void Patch001_UpdateRecordAssociationDataPrimaryKey(DataConfig providerConfig, POCO.SakaiSite sakaiSite, SakaiContentResource sakaiResource, SakaiFile currentSakaiFile, string sakaiUIPath, string recordAssociationSuffix)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(currentSakaiFile.RowKey), "eq");
                    filters.Add(pkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch001> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch001>(filters);

                    // Create the update
                    string updateParam = "{$set: {PartitionKey: '" + Utils.CleanTableKey(sakaiUIPath) + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdatePatch001> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch001>(providerConfig, "recordassociation" + recordAssociationSuffix);
                    UpdateResult result = collection.UpdateMany(filter, updateDoc);
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void Patch001_UpdateRecordAssociationDataRowKey(DataConfig providerConfig, POCO.SakaiSite sakaiSite, SakaiContentResource sakaiResource, SakaiFile currentSakaiFile, string sakaiUIPath, string recordAssociationSuffix)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(currentSakaiFile.RowKey), "eq");
                    filters.Add(pkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch001> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch001>(filters);

                    // Create the update
                    string updateParam = "{$set: {RowKey: '" + Utils.CleanTableKey(sakaiUIPath) + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdatePatch001> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch001>(providerConfig, "recordassociation" + recordAssociationSuffix);
                    UpdateResult result = collection.UpdateMany(filter, updateDoc);
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static long Patch002_UpdateRecordAssociationDataPartitionKey(DataConfig providerConfig, string oldPartitionKey, string newPartitionKey, string recordAssociationTable)
        {
            long rowsUpdated = 0;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(oldPartitionKey), "eq");
                    filters.Add(pkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch002> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch002>(filters);

                    // Create the update
                    string updateParam = "{$set: {PartitionKey: '" + Utils.CleanTableKey(newPartitionKey) + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdatePatch002> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch002>(providerConfig, recordAssociationTable);
                    UpdateResult result = collection.UpdateMany(filter, updateDoc);
                    if (result.IsAcknowledged)
                    {
                        if (result.IsModifiedCountAvailable)
                        {
                            rowsUpdated = result.ModifiedCount;
                        }
                    }
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return rowsUpdated;
        }

        public static long Patch002_UpdateRecordAssociationDataRowKey(DataConfig providerConfig, string oldRowKey, string newRowKey, string recordAssociationTable)
        {
            long rowsUpdated = 0;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    throw new NotImplementedException();
                    break;


                case "internal.mongodb":

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(oldRowKey), "eq");
                    filters.Add(rkFilter);
                    FilterDefinition<MongoSakaiUpdatePatch002> filter = Utils.GenerateMongoFilter<MongoSakaiUpdatePatch002>(filters);

                    // Create the update
                    string updateParam = "{$set: {RowKey: '" + Utils.CleanTableKey(newRowKey) + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update
                    IMongoCollection<MongoSakaiUpdatePatch002> collection = Utils.GetMongoCollection<MongoSakaiUpdatePatch002>(providerConfig, recordAssociationTable);
                    UpdateResult result = collection.UpdateMany(filter, updateDoc);
                    if (result.IsAcknowledged)
                    {
                        if (result.IsModifiedCountAvailable)
                        {
                            rowsUpdated = result.ModifiedCount;
                        }
                    }
                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return rowsUpdated;
        }
    }
}
