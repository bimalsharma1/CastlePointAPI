using System;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    class MongoGFIArchiverMailbox:POCO.GFIMailbox
    {

    }
    class MongoGFIArchiverMessage : POCO.GFIMessage
    {

    }
    class MongoGFIArchiverMessageBatchStatus_Get : POCO.GFIMessageBatchStatus
    {
        public ObjectId _id { get; set; }
    }

    class MongoGFIArchiverMailbox_Get : POCO.GFIMailbox
    {
        public ObjectId _id { get; set; }
    }
    class MongoGFIArchiverMessage_Get : POCO.GFIMessageEntity
    {
        public ObjectId _id { get; set; }

        public string BatchGuid { get; set; }
        public string BatchStatus { get; set; }
        public string JsonFileProcessResult { get; set; }
    }



    public static class GFIArchiverMailbox
    {
        internal static class AzureTableNames
        {
            internal static string GFIArchiverMessages { get { return "gfiarchivermessages"; } }
            internal static string GFIArchiverFolders { get { return "gfiarchiverfolders"; } }
        }
        internal static class MongoTableNames
        {
            internal static string GFIArchiverMessages { get { return "gfiarchivermessages"; } }
            internal static string GFIArchiverFolders { get { return "gfiarchiverfolders"; } }
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

        public static List<POCO.GFIMessageEntity> GetMessages(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.GFIMessageEntity> messages = new List<POCO.GFIMessageEntity>();

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
                    var collection = Utils.GetMongoCollection<MongoGFIArchiverMessage_Get>(providerConfig, MongoTableNames.GFIArchiverMessages);

                    FilterDefinition<MongoGFIArchiverMessage_Get> filter = Utils.GenerateMongoFilter<MongoGFIArchiverMessage_Get>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        messages.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return messages;

        }

        public static void UpdateMIMEType(DataConfig providerConfig, POCO.CPFileMIMEType mimeType)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    tableName = AzureTableNames.GFIArchiverMessages;

                    break;

                case "internal.mongodb":

                    tableName = MongoTableNames.GFIArchiverMessages;

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            // Call the update MIME type function
            File.UpdateMIMEType(providerConfig, mimeType, tableName);

            return;
        }

        public static List<POCO.GFIMessageBatchStatus> GetMessageBatchStatus(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.GFIMessageBatchStatus> fileBatchStatus = new List<POCO.GFIMessageBatchStatus>();

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
                    var collection = Utils.GetMongoCollection<MongoGFIArchiverMessageBatchStatus_Get>(providerConfig, MongoTableNames.GFIArchiverMessages);

                    FilterDefinition<MongoGFIArchiverMessageBatchStatus_Get> filter = Utils.GenerateMongoFilter<MongoGFIArchiverMessageBatchStatus_Get>(filters);

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

        public static string AddMessage(DataConfig providerConfig, POCO.System system, POCO.GFIMailbox mailbox, POCO.GFIMessage message)
        {
            string partitionKey = system.PartitionKey;

            POCO.GFIMessageEntity docEntity = new GFIMessageEntity(Utils.CleanTableKey(partitionKey), Utils.CleanTableKey(message.MarcId.ToString()));
            docEntity.CPStatus = string.Empty;
            docEntity.ItemCount = 0;
            docEntity.Name = message.Subject;
            docEntity.ServerRelativeUrl = string.Empty;
            docEntity.TimeCreated = message.SentDate.ToUniversalTime();
            docEntity.TimeLastModified = message.ReceivedDate.ToUniversalTime();

            return GFIArchiverMailbox.AddMessage(providerConfig, docEntity);
        }

        public static string AddMessage(DataConfig providerConfig, POCO.GFIMessageEntity document)
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
                    IMongoCollection<MongoGFIArchiverMessage> collection = Utils.GetMongoCollection<MongoGFIArchiverMessage>(providerConfig, MongoTableNames.GFIArchiverMessages);
                    MongoGFIArchiverMessage mongoObject = Utils.ConvertType<MongoGFIArchiverMessage>(document);
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
