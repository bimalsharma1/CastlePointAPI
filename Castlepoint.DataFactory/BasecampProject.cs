using System;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    class MongoBasecampProject:POCO.BasecampProjectEntity
    {

    }
    class MongoBasecampDocument : POCO.BasecampDocumentEntity
    {

    }
    class MongoBasecampFileBatchStatus_Get : POCO.BasecampFileBatchStatus
    {
        public ObjectId _id { get; set; }
    }

    class MongoBasecampProject_Get : POCO.BasecampProjectEntity
    {
        public ObjectId _id { get; set; }
    }
    class MongoBasecampDocument_Get : POCO.BasecampDocumentEntity
    {
        public ObjectId _id { get; set; }

        public string BatchGuid { get; set; }
        public string BatchStatus { get; set; }
        public string JsonFileProcessResult { get; set; }
    }



    public static class BasecampProject
    {
        internal static class AzureTableNames
        {
            internal static string BaseCampFiles { get { return "basecampfiles"; } }
        }
        internal static class MongoTableNames
        {
            internal static string BaseCampFiles { get { return "basecampfiles"; } }
        }

        public static List<POCO.BasecampProjectEntity> GetProjects(DataConfig providerConfig, POCO.System system, POCO.BasecampEntity basecampProject)
        {
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", Utils.CleanTableKey(system.PartitionKey), "eq");
            Filter rkFilter = new Filter("RowKey", Utils.CleanTableKey(basecampProject.url), "eq");
            filters.Add(pkFilter);
            filters.Add(rkFilter);

            return BasecampProject.GetProjects(providerConfig, filters);
        }

        public static void AddFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            // Call the datafactory generic Add
            File.AddFileBatchStatus(providerConfig, fileBatchStatus, "basecampfilesbatchstatus");

            return;
        }

        public static void UpdateFileBatchStatus(DataConfig providerConfig, POCO.FileBatchStatus fileBatchStatus)
        {
            DataFactory.File.UpdateFileBatchStatus(providerConfig, fileBatchStatus, "basecampfiles");

            return;
        }

        public static List<POCO.BasecampProjectEntity> GetProjects(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.BasecampProjectEntity> projects = new List<POCO.BasecampProjectEntity>();


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
                    var collection = Utils.GetMongoCollection<MongoBasecampProject_Get>(providerConfig, "basecampprojects");
                    FilterDefinition<MongoBasecampProject_Get> filter = Utils.GenerateMongoFilter<MongoBasecampProject_Get>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var project in documents)
                    {
                        projects.Add(project);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return projects;

        }

        public static List<POCO.BasecampDocumentEntity> GetFiles(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.BasecampDocumentEntity> files = new List<POCO.BasecampDocumentEntity>();


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
                    var collection = Utils.GetMongoCollection<MongoBasecampDocument_Get>(providerConfig, "basecampfiles");

                    FilterDefinition<MongoBasecampDocument_Get> filter = Utils.GenerateMongoFilter<MongoBasecampDocument_Get>(filters);

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

        public static void UpdateProjectStatus(DataFactory.DataConfig providerConfig, POCO.System system, POCO.BasecampEntity basecampProject, string cpStatus)
        {


            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    throw new NotImplementedException();

                    break;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<BasecampProjectEntity>(providerConfig, "basecampprojects");

                    // Create the update filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(system.PartitionKey), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(basecampProject.url), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<BasecampProjectEntity> filter = Utils.GenerateMongoFilter<BasecampProjectEntity>(filters);

                    string updateParam = "{$set: {TimeCreated: '" + basecampProject.created_at.ToUniversalTime().ToString(Utils.ISODateFormat) + "', TimeLastModified: '" + basecampProject.updated_at.ToUniversalTime().ToString(Utils.ISODateFormat) + "', CPStatus: '" + cpStatus + "'}}";
                    BsonDocument updateDoc = BsonDocument.Parse(updateParam);

                    // Update the batch status
                    collection.UpdateOne(filter, updateDoc);
                    break;
            }
        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = BasecampProject.AzureTableNames.BaseCampFiles;

                    break;

                case "internal.mongodb":

                    tableName = BasecampProject.MongoTableNames.BaseCampFiles;

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Call the update MIME type function
            File.UpdateMIMEType(providerConfig, mimeType, tableName);

            return;
        }

        public static string AddProject(DataConfig providerConfig, POCO.System system, POCO.BasecampEntity sourceProject)
        {

            POCO.BasecampProjectEntity projectEntity = new BasecampProjectEntity(Utils.CleanTableKey(system.PartitionKey), Utils.CleanTableKey(sourceProject.url));
            projectEntity.CPStatus = string.Empty;
            projectEntity.ItemCount = 0;
            projectEntity.Name = sourceProject.name;
            projectEntity.ServerRelativeUrl = sourceProject.url;
            projectEntity.TimeCreated = sourceProject.created_at.ToUniversalTime();
            projectEntity.TimeLastModified = sourceProject.updated_at.ToUniversalTime();

            return BasecampProject.AddProject(providerConfig, projectEntity);
        }

        public static string AddProject(DataConfig providerConfig, POCO.BasecampProjectEntity project)
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
                    IMongoCollection<MongoBasecampProject> collection = Utils.GetMongoCollection<MongoBasecampProject>(providerConfig, "basecampprojects");
                    MongoBasecampProject mongoObject = Utils.ConvertType<MongoBasecampProject>(project);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.BasecampFileBatchStatus> GetFileBatchStatus(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.BasecampFileBatchStatus> fileBatchStatus = new List<POCO.BasecampFileBatchStatus>();

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
                    var collection = Utils.GetMongoCollection<MongoBasecampFileBatchStatus_Get>(providerConfig, "basecampfilesbatchstatus");

                    FilterDefinition<MongoBasecampFileBatchStatus_Get> filter = Utils.GenerateMongoFilter<MongoBasecampFileBatchStatus_Get>(filters);

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

        public static string GetBasecampBucketUrl(string basecampProjectUrl)
        {
            // URL format of the basecamp project is:  https://<basecamp api>/<account number>/projects/<project id>.json
            // all data for a project is in "buckets": https://<basecamp api>/<account number>/buckets/<project id>/<type of bucket>/<id>.json
            // any data matching should use the buckets/<project id>>
            string partitionKey = basecampProjectUrl.Replace("projects", "buckets");
            if (partitionKey.ToLower().EndsWith(".json")) { partitionKey = partitionKey.Substring(0, partitionKey.Length - 5); }
            if (!partitionKey.EndsWith("|")) { partitionKey += "|"; }

            return partitionKey;
        }

        public static string AddFile(DataConfig providerConfig, POCO.System system, POCO.BasecampEntity sourceProject, POCO.BasecampDocument sourceDocument)
        {
            string partitionKey = GetBasecampBucketUrl(sourceProject.url);

            POCO.BasecampDocumentEntity docEntity = new BasecampDocumentEntity(Utils.CleanTableKey(partitionKey), Utils.CleanTableKey(sourceDocument.url));
            docEntity.CPStatus = string.Empty;
            docEntity.ItemCount = 0;
            docEntity.Name = sourceDocument.title;
            docEntity.ServerRelativeUrl = sourceDocument.url;
            docEntity.TimeCreated = sourceDocument.created_at.ToUniversalTime();
            docEntity.TimeLastModified = sourceDocument.updated_at.ToUniversalTime();
            docEntity.Title = sourceDocument.title;

            return BasecampProject.AddFile(providerConfig, docEntity);
        }

        public static string AddFile(DataConfig providerConfig, POCO.BasecampDocumentEntity document)
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
                    IMongoCollection<MongoBasecampDocument> collection = Utils.GetMongoCollection<MongoBasecampDocument>(providerConfig, "basecampfiles");
                    MongoBasecampDocument mongoObject = Utils.ConvertType<MongoBasecampDocument>(document);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }
    }
}
