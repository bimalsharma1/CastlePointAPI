using System;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO;
using MongoDB.Bson;
using System.Text.RegularExpressions;
using System.Web;
using System.IO;
using System.Threading.Tasks;

using MongoDB.Driver;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;

namespace Castlepoint.DataFactory
{
    public static class Utils
    {
        internal static readonly DateTime AzureTableMinDateTime = DateTime.Parse("1601-01-01T00:00:00+00:00").ToUniversalTime();
        internal const string TableSuffixDateFormatYMD = "yyyyMMdd";
        internal const string TableSuffixDateFormatYM = "yyyyMM";
        internal const string ISODateFormatNoTime = "yyyy-MM-dd";
        internal const string ISODateFormat = "yyyy-MM-ddTHH:mm:ssZ";

        internal const string FlagGuid = "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF";

        internal static string GetSecretOrEnvVar(string key)
        {
            try
            {

                const string DOCKER_SECRET_PATH = "/run/secrets/";
                if (Directory.Exists(DOCKER_SECRET_PATH))
                {
                    IFileProvider provider = new PhysicalFileProvider(DOCKER_SECRET_PATH);
                    IFileInfo fileInfo = provider.GetFileInfo(key);
                    if (fileInfo.Exists)
                    {
                        using (var stream = fileInfo.CreateReadStream())
                        using (var streamReader = new StreamReader(stream))
                        {
                            return streamReader.ReadToEnd();
                        }
                    }
                }
                else
                {
                    string folderPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Castlepoint\\cpdev1");
                    IFileProvider provider = new PhysicalFileProvider(folderPath);
                    IFileInfo fileInfo = provider.GetFileInfo(key);
                    if (fileInfo.Exists)
                    {
                        using (var stream = fileInfo.CreateReadStream())
                        using (var streamReader = new StreamReader(stream))
                        {
                            return streamReader.ReadToEnd();
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                return string.Empty;
            }

            return string.Empty;
        }
        internal static CloudTable GetCloudTable(DataConfig providerConfig, string cloudTableName)
        {
            //string storageAccountConnectionString = Utils.GetSecretOrEnvVar("cpapi_config_conn_azure").Trim();
            string storageAccountConnectionString = providerConfig.ConnectionString;

            // validate storage account address
            if (storageAccountConnectionString == "")
            {
                //_logger.LogWarning("Storage account connection string not set");
                throw new InvalidOperationException("Storage account connection string not set");
            }
            else
            {
                //_logger.LogDebug("Storage account connection string loaded");
            }

            // Process the records
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

            // Create the table client. 
            //_logger.Info("Creating cloud table client");
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // Create the table if it doesn't exist. 
            //_logger.Info("Getting table reference");
            CloudTable table = tableClient.GetTableReference(cloudTableName);
            Task tCreate = table.CreateIfNotExistsAsync();
            tCreate.Wait();

            return table;
        }

        internal static CloudTable GetCloudTableNoCreate(string cloudTableName, ILogger _logger)
        {
            string storageAccountConnectionString = Utils.GetSecretOrEnvVar("azure_storage_account_connection_string").Trim();
            // validate storage account address
            if (storageAccountConnectionString == "")
            {
                _logger.LogWarning("Storage account connection string not set");
                throw new InvalidOperationException("Storage account connection string not set");
            }
            else
            {
                _logger.LogDebug("Storage account connection string loaded");
            }

            // Configure the account connection
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

            // Create the table client. 
            //_logger.Info("Creating cloud table client");
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // Create the table if it doesn't exist. 
            //_logger.Info("Getting table reference");
            CloudTable table = tableClient.GetTableReference(cloudTableName);

            return table;
        }
        internal static string GetLessThanFilter(string filter)
        {
            string returnString = "";

            // Validate the provided string
            if (filter == null || filter.Length == 0) { return returnString; }

            string lastLetter = filter.Substring(filter.Length - 1, 1);

            char letter = char.Parse(lastLetter);
            char nextChar;

            if (letter == '/' || letter == '\\' || letter == ':')
            {
                returnString = filter + 'Z';
            }
            else
            {
                if (letter == 'z')
                    nextChar = 'a';
                else if (letter == 'Z')
                    nextChar = 'A';
                else
                    nextChar = (char)(((int)letter) + 1);

                returnString = filter.Substring(0, filter.Length - 1) + nextChar.ToString();
            }
            return returnString;
        }

        public static FilterDefinition<T> GenerateMongoFilter<T>(List<Filter> filters)
        {
            // Create the filter builder object
            var filterBuilder = Builders<T>.Filter;
            FilterDefinition<T> filter = null;

            if (filters==null || filters.Count==0)
            {
                // Return an empty filter (matches everything)
                return filterBuilder.Empty; ;
            }

            // Loop through the provided filters
            foreach(Filter f in filters)
            {
                string fieldValue = f.FieldValue;

                FilterDefinition<T> currentFilter = null;
                switch (f.Comparison)
                {
                    case "eq":
                        if (f.FieldName == "_id")
                        {
                            currentFilter = filterBuilder.Eq(f.FieldName, new ObjectId(fieldValue));
                        }
                        else
                        {
                            currentFilter = filterBuilder.Eq(f.FieldName, fieldValue);
                        }
                        break;
                    case "gt":
                        if (f.FieldName == "_id")
                        {
                            currentFilter = filterBuilder.Gt(f.FieldName, new ObjectId(fieldValue));
                        }
                        else
                        {
                            currentFilter = filterBuilder.Gt(f.FieldName, fieldValue);
                        }
                        break;
                    case "ge":
                        currentFilter = filterBuilder.Gte(f.FieldName, fieldValue);
                        break;
                    case "lt":
                        currentFilter = filterBuilder.Lt(f.FieldName, fieldValue);
                        break;
                    case "le":
                        currentFilter = filterBuilder.Lte(f.FieldName, fieldValue);
                        break;
                    default:
                        throw new ApplicationException("Unknown filter provided: " + f.Comparison);
                }
                if (filter != null)
                {
                    // Append the filter
                    filter = filter & currentFilter;
                }
                else
                {
                    // Init the filter
                    filter = currentFilter;
                }
            }

            return filter;
        }

        public static FilterDefinition<T> GenerateOrMongoFilter<T>(List<Filter> filters)
        {
            // Create the filter builder object
            var filterBuilder = Builders<T>.Filter;
            FilterDefinition<T> filter = null;
            
            // Loop through the provided filters
            foreach (Filter f in filters)
            {
                FilterDefinition<T> currentFilter = null;
                switch (f.Comparison)
                {
                    case "eq":
                        currentFilter = filterBuilder.Eq(f.FieldName, f.FieldValue);
                        break;
                    case "gt":
                        currentFilter = filterBuilder.Gt(f.FieldName, f.FieldValue);
                        break;
                    case "ge":
                        currentFilter = filterBuilder.Gte(f.FieldName, f.FieldValue);
                        break;
                    case "lt":
                        currentFilter = filterBuilder.Lt(f.FieldName, f.FieldValue);
                        break;
                    case "le":
                        currentFilter = filterBuilder.Lte(f.FieldName, f.FieldValue);
                        break;
                    default:
                        throw new ApplicationException("Unknown filter provided: " + f.Comparison);
                }
                if (filter != null)
                {
                    // Append the filter
                    filter = filter | currentFilter;
                }
                else
                {
                    // Init the filter
                    filter = currentFilter;
                }
            }

            // Make sure we don't return a null filter
            // NOTE Empty matches all
            if (filter==null) { filter = filterBuilder.Empty; }

            return filter;
        }

        public static string GenerateAzureFilter(List<Filter> filters)
        {
            string combinedFilter = string.Empty;

            // Loop through the provided filters
            foreach (Filter f in filters)
            {
                string currentFilter = string.Empty;

                // Check for PartitionKey or RowKey - we make sure these values are "cleaned"
                string filterValue = f.FieldValue;
                if (f.FieldName=="PartitionKey" || f.FieldName == "RowKey")
                {
                    filterValue = Utils.CleanTableKey(f.FieldValue);
                    // GM 20181110 ...and valid JSON format
                    //filterValue = JsonConvert.ToString(filterValue);
                }
                switch (f.Comparison)
                {
                    case "eq":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.Equal, filterValue);
                        break;
                    case "gt":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.GreaterThan, filterValue);
                        break;
                    case "ge":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.GreaterThanOrEqual, filterValue);
                        break;
                    case "lt":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.LessThan, filterValue);
                        break;
                    case "le":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.LessThanOrEqual, filterValue);
                        break;
                    default:
                        throw new ApplicationException("Unknown filter comparison provided: " + f.Comparison);
                }
                if (combinedFilter != "")
                {
                    // Append the filter
                    combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.And, currentFilter); ;
                }
                else
                {
                    // Init the filter
                    combinedFilter = currentFilter;
                }
            }

            return combinedFilter;
        }

        public static string GenerateAzureOrFilter(List<Filter> filters)
        {
            string combinedFilter = string.Empty;

            // Loop through the provided filters
            foreach (Filter f in filters)
            {
                string currentFilter = string.Empty;

                // Check for PartitionKey or RowKey - we make sure these values are "cleaned"
                string filterValue = f.FieldValue;
                if (f.FieldName == "PartitionKey" || f.FieldName == "RowKey")
                {
                    filterValue = Utils.CleanTableKey(f.FieldValue);
                }
                switch (f.Comparison)
                {
                    case "eq":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.Equal, filterValue);
                        break;
                    case "gt":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.GreaterThan, filterValue);
                        break;
                    case "ge":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.GreaterThanOrEqual, filterValue);
                        break;
                    case "lt":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.LessThan, filterValue);
                        break;
                    case "le":
                        currentFilter = TableQuery.GenerateFilterCondition(f.FieldName, QueryComparisons.LessThanOrEqual, filterValue);
                        break;
                    default:
                        throw new ApplicationException("Unknown filter comparison provided: " + f.Comparison);
                }
                if (combinedFilter != "")
                {
                    // Append the filter
                    combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.Or, currentFilter); ;
                }
                else
                {
                    // Init the filter
                    combinedFilter = currentFilter;
                }
            }

            return combinedFilter;
        }

        internal static void AzureBatchExecute(CloudTable table, List<TableOperation> ops)
        {
            int batchCount = 0;
            var batchOp = new TableBatchOperation();
            List<string> keys = new List<string>();

            foreach (TableOperation tableop in ops)
            {
                // Check if we need to run the batch
                if (batchCount >= 99)
                {

                    try
                    {
                        //_logger.LogDebug("Batch count: " + batchCount.ToString() + "," + batchOp.Count.ToString());
                        // Reset tracking variables
                        batchCount = 0;
                        keys = new List<string>();

                        Task tBatch = table.ExecuteBatchAsync(batchOp);
                        tBatch.Wait();

                        // Reset the batch operation
                        batchOp = new TableBatchOperation();
                    }
                    catch (StorageException ex)
                    {
                        var requestInformation = ex.RequestInformation;
                        //_logger.LogWarning("ERR http status msg: " + requestInformation.HttpStatusMessage);

                        // Reset the batch operation
                        batchOp = new TableBatchOperation();
                        batchCount = 0;
                        keys = new List<string>();

                        throw;
                    }

                }

                batchCount++;

                // Check if this element has been added already (duplicate partitionkey + rowkey in a batch causes an error)
                string k = tableop.Entity.PartitionKey + "|" + tableop.Entity.RowKey;
                if (!keys.Contains(k))
                {
                    // Add the key and the table operation to the batch
                    keys.Add(k);
                    batchOp.Add(tableop);
                }

            }

            // Check if any batch operations that haven't been run
            if (batchOp.Count > 0)
            {
                try
                {
                    //_logger.LogDebug("Batch remainder: " + batchOp.Count.ToString());
                    Task tBatch = table.ExecuteBatchAsync(batchOp);
                    tBatch.Wait();
                }
                catch (StorageException ex)
                {
                    var requestInformation = ex.RequestInformation;
                    //_logger.LogWarning("ERR http status msg: " + requestInformation.HttpStatusMessage);

                    // Reset the batch operation
                    batchOp = new TableBatchOperation();
                    batchCount = 0;
                    keys = new List<string>();

                    throw;
                }
                catch (Exception ex1)
                {
                    //_logger.LogError("ERR http status msg: " + ex1.Message);
                    throw;
                }
            }
        }

        internal static string CleanTableKey(string keyToClean)
        {

            /*
            The following characters are not allowed in PartitionKey and RowKey fields:
                The forward slash (/) character
                The backslash (\) character
                The number sign (#) character 
                The question mark (?) character
            */

            string cleanKey = "";

            // unescape html and url encoding (just in case)
            cleanKey = HttpUtility.HtmlDecode(keyToClean);
            cleanKey = HttpUtility.UrlDecode(cleanKey);
            
            string patternCarriageReturn = @"\r";
            Regex regCarriageReturn = new Regex(patternCarriageReturn);
            string patternLineFeed = @"\n";
            Regex regLineFeed = new Regex(patternLineFeed);
            string patternTab = @"\t";
            Regex regTab = new Regex(patternTab);

            // Remove carriage returns and line feeds and tabs
            cleanKey = regCarriageReturn.Replace(cleanKey.ToLower(), "");
            cleanKey = regLineFeed.Replace(cleanKey.ToLower(), "");
            cleanKey = regTab.Replace(cleanKey.ToLower(), "");

            string patternForwardSlash = @"\\";
            Regex regForwardSlash = new Regex(patternForwardSlash);
            string patternBackSlash = "/";
            Regex regBackSlash = new Regex(patternBackSlash);
            string patternHash = "#";
            Regex regHash = new Regex(patternHash);
            string patternQuestionMark = @"\?";
            Regex regQuestionMark = new Regex(patternQuestionMark);

            cleanKey = regForwardSlash.Replace(cleanKey.ToLower(), "|");
            cleanKey = regBackSlash.Replace(cleanKey, "|");
            cleanKey = regHash.Replace(cleanKey, "|");
            cleanKey = regQuestionMark.Replace(cleanKey, "|");

            return cleanKey;
        }

        internal static IMongoCollection<T> GetMongoCollection<T>(DataConfig providerConfig, string collectionName)
        {
            var client = new MongoClient(providerConfig.ConnectionString);
            var database = client.GetDatabase(providerConfig.DatabaseName);
            IMongoCollection<T> collection = database.GetCollection<T>(collectionName);
            return collection;
        }

        internal static T ConvertType<T>(Object o)
        {
            var serializedParent = JsonConvert.SerializeObject(o);
            T c = JsonConvert.DeserializeObject<T>(serializedParent);
            return c;
        }
    }
}
