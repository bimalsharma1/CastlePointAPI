using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Castlepoint.POCO;
using Castlepoint.Utilities;
using MongoDB.Bson.IO;

namespace Castlepoint.DataFactory
{

    class AzureFileProcessException : EntityAdapter<POCO.FileProcessException>
    {
        public AzureFileProcessException() { }
        public AzureFileProcessException(POCO.FileProcessException o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoFileProcessException : POCO.FileProcessException
    {
        //public ObjectId _id { get; }
    }

    class MongoFileProcessException_read : POCO.FileProcessException
    {
        public ObjectId _id { get; }
    }

    class AzureLogClassification : EntityAdapter<POCO.LogClassification>
    {
        public AzureLogClassification() { }
        public AzureLogClassification(POCO.LogClassification o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoLogClassification : POCO.LogClassification
    {
        //public ObjectId _id { get; }
    }

    class MongoLogClassification_Read : POCO.LogClassification
    {
        public ObjectId _id { get; }
    }

    class AzureLogError:EntityAdapter<POCO.LogError>
    {
        public AzureLogError() { }
        public AzureLogError(POCO.LogError o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoLogError: POCO.LogError
    {
        //public ObjectId _id { get; }
    }

    class AzureLastAction:EntityAdapter<POCO.LogLastAction>
    {
        public AzureLastAction() { }
        public AzureLastAction(POCO.LogLastAction o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoLastAction : POCO.LogLastAction
    {
        //public ObjectId _id { get; }
    }

    class AzureLogServiceEvent : EntityAdapter<POCO.LogServiceEvent>
    {
        public AzureLogServiceEvent() { }
        public AzureLogServiceEvent(POCO.LogServiceEvent o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoLogServiceEvent : POCO.LogServiceEvent
    {
        //public ObjectId _id { get; }
    }

    class AzureLogRecordProcessing : EntityAdapter<POCO.LogRecordProcessing>
    {
        public AzureLogRecordProcessing() { }
        public AzureLogRecordProcessing(POCO.LogRecordProcessing o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoLogRecordProcessing : POCO.LogRecordProcessing
    {
        //public ObjectId _id { get; }
    }
    class AzureLogFileProcessing : EntityAdapter<POCO.LogFileProcessing>
    {
        public AzureLogFileProcessing() { }
        public AzureLogFileProcessing(POCO.LogFileProcessing o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    class MongoLogFileProcessing : POCO.LogFileProcessing
    {
        //public ObjectId _id { get; }
    }

    class MongoLogEventProcessingTime : POCO.LogEventProcessingTime
    {
        //public ObjectId _id { get; }
    }
    class MongoLogEventProcessingTime_Read : POCO.LogEventProcessingTime
    {
        public ObjectId _id { get; }
    }
    class AzureLogEventProcessingTime : EntityAdapter<POCO.LogEventProcessingTime>
    {
        public AzureLogEventProcessingTime() { }
        public AzureLogEventProcessingTime(POCO.LogEventProcessingTime o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class Log
    {
        class AzureTableNames
        {
            public const string LogError = "stlplogerror";
            public const string LogLastAction = "stlploglastaction";
            public const string LogClassification = "stlplogclassification";
            public const string LogEventService = "stlplogeventservice";
            public const string DiagnosticsFileProcessing = "diagfileprocessing";
        }
        class MongoTableNames
        {
            public const string LogError = "logerror";
            public const string LogLastAction = "loglastaction";
            public const string LogClassification = "logclassification";
            public const string LogEventService = "logeventservice";
            public const string DiagnosticsFileProcessing = "diagfileprocessing";
        }
        public static string AddLastAction(DataConfig providerConfig, POCO.LogLastAction lastAction)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureLastAction az = new AzureLastAction(lastAction);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.LogLastAction);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoLastAction> collection = Utils.GetMongoCollection<MongoLastAction>(providerConfig, MongoTableNames.LogLastAction);
                    MongoLastAction mongoObject = Utils.ConvertType<MongoLastAction>(lastAction);
                    
                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey("LastAction"), "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(lastAction.EventName), "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoLastAction> filter = Utils.GenerateMongoFilter<MongoLastAction>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    string debugfilter = Newtonsoft.Json.JsonConvert.SerializeObject(filters);
                    string debugobject = Newtonsoft.Json.JsonConvert.SerializeObject(mongoObject);
                    collection.ReplaceOne(filter, mongoObject, options);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddLogClassification(DataConfig providerConfig, POCO.LogClassification logClass)

        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureLogClassification az = new AzureLogClassification(logClass);

                    string azureTableName = AzureTableNames.LogClassification + DateTime.Now.ToString(Utilities.Constants.TableSuffixDateFormatYM);
                    CloudTable table = Utils.GetCloudTable(providerConfig, azureTableName);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    string mongoTableName = MongoTableNames.LogClassification + DateTime.Now.ToString(Utilities.Constants.TableSuffixDateFormatYM);
                    IMongoCollection<MongoLogClassification> collection = Utils.GetMongoCollection<MongoLogClassification>(providerConfig, mongoTableName);
                    MongoLogClassification mongoObject = Utils.ConvertType<MongoLogClassification>(logClass);

                    // Upsert
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddRecordProcessingLog(DataConfig providerConfig, POCO.LogRecordProcessing log)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    AzureLogRecordProcessing az = new AzureLogRecordProcessing(log);

                    CloudTable table = Utils.GetCloudTable(providerConfig, "stlplogrecordprocessing");
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoLogRecordProcessing> collection = Utils.GetMongoCollection<MongoLogRecordProcessing>(providerConfig, "logrecordprocessing");
                    MongoLogRecordProcessing mongoObject = Utils.ConvertType<MongoLogRecordProcessing>(log);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", log.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", log.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoLogRecordProcessing> filter = Utils.GenerateMongoFilter<MongoLogRecordProcessing>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static string AddFileProcessingException(DataConfig providerConfig, POCO.System system, POCO.FileProcessException fileException)
        {
            string tableName = string.Empty;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Calcuate table name based on System Id and file process result (partitionkey)
                    tableName = "stlpfileexception-" + fileException.PartitionKey + "-" + system.SystemId.ToString();

                    AzureFileProcessException az = new AzureFileProcessException(fileException);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    // Calcuate table name based on System Id and file process result (partitionkey)
                    tableName = "fileexception-" + fileException.PartitionKey + "-" + system.SystemId.ToString();

                    IMongoCollection<MongoFileProcessException> collection = Utils.GetMongoCollection<MongoFileProcessException>(providerConfig, tableName);
                    MongoFileProcessException mongoObject = Utils.ConvertType<MongoFileProcessException>(fileException);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", fileException.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", fileException.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoFileProcessException> filter = Utils.GenerateMongoFilter<MongoFileProcessException>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.FileProcessException> GetFileProcessException(DataConfig providerConfig, POCO.System system, string exceptionType)
        {
            string tableName = string.Empty;

            List<POCO.FileProcessException> docs = new List<POCO.FileProcessException>();

            // Create filter
            List<Filter> filters = new List<Filter>();
            Filter pkfilt = new Filter("PartitionKey", Utilities.Converters.CleanTableKey(exceptionType), "eq");
            filters.Add(pkfilt);

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    // Calcuate table name based on System Id and file process result (partitionkey)
                    tableName = "stlpfileexception-" + Utilities.Converters.CleanTableKey(exceptionType) + "-" + system.SystemId.ToString();

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureFileProcessException> azdata = new List<AzureFileProcessException>();
                    AzureTableAdaptor<AzureFileProcessException> adaptor = new AzureTableAdaptor<AzureFileProcessException>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        docs.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:

                    // Calcuate table name based on System Id and file process result (partitionkey)
                    tableName = "fileexception-" + Utilities.Converters.CleanTableKey(exceptionType) + "-" + system.SystemId.ToString();

                    var collection = Utils.GetMongoCollection<MongoFileProcessException_read>(providerConfig, tableName);

                    FilterDefinition<MongoFileProcessException_read> filter = Utils.GenerateMongoFilter<MongoFileProcessException_read>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        docs.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return docs;
        }

        public static string AddFileProcessingLog(DataConfig providerConfig, POCO.LogFileProcessing log)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Cycle the tables each month to manage size
                    string tableNameSuffix = DateTime.UtcNow.ToString(Utils.TableSuffixDateFormatYM);
                    string tableName = "stlplogfileprocessresult" + tableNameSuffix;

                    AzureLogFileProcessing az = new AzureLogFileProcessing(log);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    IMongoCollection<MongoLogFileProcessing> collection = Utils.GetMongoCollection<MongoLogFileProcessing>(providerConfig, "logfileprocessing");
                    MongoLogFileProcessing mongoObject = Utils.ConvertType<MongoLogFileProcessing>(log);

                    // Create the upsert filter
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", log.PartitionKey, "eq");
                    DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", log.RowKey, "eq");
                    filters.Add(pkFilter);
                    filters.Add(rkFilter);
                    FilterDefinition<MongoLogFileProcessing> filter = Utils.GenerateMongoFilter<MongoLogFileProcessing>(filters);

                    // Create the upsert options
                    MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                    options.IsUpsert = true;

                    // Upsert
                    collection.ReplaceOne(filter, mongoObject, options);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return string.Empty;
        }

        public static List<POCO.LogEventProcessingTime> GetProcessingTime(DataConfig providerConfig, List<Filter> filters, string tableNameSuffix_yyyymm)
        {

            // Validate the YYYYMM suffix
            if (tableNameSuffix_yyyymm.Length!=6)
            {
                throw new ApplicationException("Invalid size for table suffix, must be YYYYMM");
            }

            string tableName = string.Empty;

            List<POCO.LogEventProcessingTime> docs = new List<POCO.LogEventProcessingTime>();

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    tableName = AzureTableNames.DiagnosticsFileProcessing + tableNameSuffix_yyyymm;

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureLogEventProcessingTime> azdata = new List<AzureLogEventProcessingTime>();
                    AzureTableAdaptor<AzureLogEventProcessingTime> adaptor = new AzureTableAdaptor<AzureLogEventProcessingTime>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        docs.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:

                    tableName = MongoTableNames.DiagnosticsFileProcessing + tableNameSuffix_yyyymm;

                    var collection = Utils.GetMongoCollection<MongoLogEventProcessingTime_Read>(providerConfig, tableName);

                    FilterDefinition<MongoLogEventProcessingTime_Read> filter = Utils.GenerateMongoFilter<MongoLogEventProcessingTime_Read>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        docs.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return docs;
        }

        public static List<POCO.LogClassification> GetLogClassification(DataConfig providerConfig, List<Filter> filters, string tableNameSuffix_yyyymm)
        {

            // Validate the YYYYMM suffix
            if (tableNameSuffix_yyyymm.Length != 6)
            {
                throw new ApplicationException("Invalid size for table suffix, must be YYYYMM");
            }

            string tableName = string.Empty;

            List<POCO.LogClassification> docs = new List<POCO.LogClassification>();

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    tableName = AzureTableNames.LogClassification + tableNameSuffix_yyyymm;

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureLogClassification> azdata = new List<AzureLogClassification>();
                    AzureTableAdaptor<AzureLogClassification> adaptor = new AzureTableAdaptor<AzureLogClassification>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        docs.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:

                    tableName = MongoTableNames.LogClassification + tableNameSuffix_yyyymm;

                    var collection = Utils.GetMongoCollection<MongoLogClassification_Read>(providerConfig, tableName);

                    FilterDefinition<MongoLogClassification_Read> filter = Utils.GenerateMongoFilter<MongoLogClassification_Read>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        docs.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return docs;
        }

        public static List<POCO.LogLastAction> GetLastAction(DataConfig providerConfig, List<Filter> filters)
        {

            List<POCO.LogLastAction> docs = new List<POCO.LogLastAction>();

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureLastAction> azdata = new List<AzureLastAction>();
                    AzureTableAdaptor<AzureLastAction> adaptor = new AzureTableAdaptor<AzureLastAction>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.LogLastAction, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        docs.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:
                    var collection = Utils.GetMongoCollection<MongoLastAction>(providerConfig, MongoTableNames.LogLastAction);

                    FilterDefinition<MongoLastAction> filter = Utils.GenerateMongoFilter<MongoLastAction>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        docs.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return docs;
        }

        public static void LogProcessingTime(DataFactory.DataConfig providerConfig, string itemUri, string processEvent, string mimeType, int numBytes, TimeSpan elapsed)
        {
            string tableNameSuffix = DateTime.UtcNow.ToString(Utils.TableSuffixDateFormatYM);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Cycle the tables each month to manage size
                    string tableName = AzureTableNames.DiagnosticsFileProcessing + tableNameSuffix;

                    // Create the data object
                    POCO.LogEventProcessingTime log = new POCO.LogEventProcessingTime();
                    log.PartitionKey = Utils.CleanTableKey(itemUri);
                    log.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));
                    log.ItemUri = itemUri;
                    log.Event = processEvent;
                    log.TotalMilliseconds = elapsed.TotalMilliseconds;
                    log.MIMEType = mimeType;
                    log.NumBytes = numBytes;

                    AzureLogEventProcessingTime az = new AzureLogEventProcessingTime(log);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    string mongoTableName = MongoTableNames.DiagnosticsFileProcessing + tableNameSuffix;
                    IMongoCollection<MongoLogEventProcessingTime> collection = Utils.GetMongoCollection<MongoLogEventProcessingTime>(providerConfig, mongoTableName);

                    // Create the data object
                    MongoLogEventProcessingTime mongoObject = new MongoLogEventProcessingTime();
                    mongoObject.PartitionKey = Utils.CleanTableKey(itemUri);
                    mongoObject.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));
                    mongoObject.ItemUri = itemUri;
                    mongoObject.Event = processEvent;
                    mongoObject.TotalMilliseconds = elapsed.TotalMilliseconds;
                    mongoObject.MIMEType = mimeType;
                    mongoObject.NumBytes = numBytes;

                    // Upsert
                    collection.InsertOne(mongoObject);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }

        public static void LogEventService(DataFactory.DataConfig providerConfig, string serviceModule, string serviceEvent)
        {
            // Store the table name suffix
            string tableNameSuffix = DateTime.UtcNow.ToString(Utils.TableSuffixDateFormatYM);

            // Create the data object
            DateTime currentDateTime = DateTime.UtcNow;
            string logid = Guid.NewGuid().ToString();
            POCO.LogServiceEvent log = new POCO.LogServiceEvent();
            log.PartitionKey = Utils.CleanTableKey(serviceModule);
            log.RowKey = Utils.CleanTableKey(logid);
            log.LogDateTime = currentDateTime.ToUniversalTime();
            log.LogId = logid;
            log.NodeIp = string.Empty;
            log.NodeName = string.Empty;
            log.ServiceEvent = serviceEvent;
            log.ServiceModule = serviceModule;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Cycle the tables each month to manage size
                    string tableName = AzureTableNames.DiagnosticsFileProcessing + tableNameSuffix;

                    AzureLogServiceEvent az = new AzureLogServiceEvent(log);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    string mongoTableName = MongoTableNames.DiagnosticsFileProcessing + tableNameSuffix;
                    IMongoCollection<MongoLogServiceEvent> collection = Utils.GetMongoCollection<MongoLogServiceEvent>(providerConfig, mongoTableName);

                    // Create the data object
                    MongoLogServiceEvent mongoObject = new MongoLogServiceEvent();
                    mongoObject.PartitionKey = Utils.CleanTableKey(serviceModule);
                    mongoObject.RowKey = Utils.CleanTableKey(logid);
                    mongoObject.LogDateTime = currentDateTime.ToUniversalTime();
                    mongoObject.LogId = logid;
                    mongoObject.NodeIp = string.Empty;
                    mongoObject.NodeName = string.Empty;
                    mongoObject.ServiceEvent = serviceEvent;
                    mongoObject.ServiceModule = serviceModule;

                    // Insert
                    collection.InsertOne(mongoObject);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }


        public static void LogError(DataFactory.DataConfig providerConfig, POCO.LogError logerr)
        {
            // Store the table name suffix
            string tableNameSuffix = DateTime.UtcNow.ToString(Utils.TableSuffixDateFormatYM);


            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Cycle the tables each month to manage size
                    string tableName = AzureTableNames.LogError + tableNameSuffix;

                    AzureLogError az = new AzureLogError(logerr);

                    CloudTable table = Utils.GetCloudTable(providerConfig, tableName);
                    TableOperation operation = TableOperation.InsertOrReplace(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":
                    string mongoTableName = MongoTableNames.LogError + tableNameSuffix;
                    IMongoCollection<MongoLogError> collection = Utils.GetMongoCollection<MongoLogError>(providerConfig, mongoTableName);

                    MongoLogError mongoObject = Utils.ConvertType<MongoLogError>(logerr);

                    // Insert
                    collection.InsertOne(mongoObject);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            //TODO return id of new object if supported
            return;
        }
    }
}
