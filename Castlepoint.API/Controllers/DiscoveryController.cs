using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.FileProviders;
using System.IO;

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("discovery")]
    [Authorize]
    public class DiscoveryController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<FileController> _logger;

        public DiscoveryController(IConfiguration config, ILogger<FileController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        [HttpPost("", Name = "Post")]
        public IActionResult Post([FromBody] string discovery)
        {
            _logger.LogInformation("CPAPI: Post");

            // Deserialize the discovery filter
            DiscoveryEntity entity = new DiscoveryEntity();
            if (discovery != null && discovery.Length > 0)
            {
                _logger.LogDebug("Deserializing Discovery of length: " + discovery.Length);
                entity = JsonConvert.DeserializeObject<DiscoveryEntity>(discovery);
            }

            // Clean the table and row keys
            entity.PartitionKey = Utils.CleanTableKey(entity.PartitionKey);
            entity.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));

            bool isAddedOK = AddDiscovery(entity);

            if (isAddedOK)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(entity);
                return result;
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        private bool AddDiscovery(DiscoveryEntity discoveryEntity)
        {
            bool isAddedOk = false;

            CloudTable table = Utils.GetCloudTable("stlpdiscover", _logger);

            //log.Info("Creating record entity");

            // Create the TableOperation that inserts or merges the entry. 
            //log.Verbose("Creating table operation");
            TableOperation insertReplaceOperation = TableOperation.InsertOrReplace(discoveryEntity);

            // Execute the insert operation. 
            //log.Verbose("Executing table operation");
            try
            {
                Task tResult = table.ExecuteAsync(insertReplaceOperation);
                tResult.Wait();
                //log.Verbose("Add keyphrase (" + keyPhrase + ") result: " + tResult.HttpStatusCode.ToString());
            }

            catch (StorageException ex)
            {
                var requestInformation = ex.RequestInformation;

                Console.WriteLine("http status msg: " + requestInformation.HttpStatusMessage);

                // get more details about the exception 
                var information = requestInformation.ExtendedErrorInformation;
                // if you have aditional information, you can use it for your logs
                if (information != null)
                {
                    var errorCode = information.ErrorCode;
                    var errMessage = string.Format("({0}) {1}",
                    errorCode,
                    information.ErrorMessage);
                    //    var errDetails = information
                    //.AdditionalDetails
                    //.Aggregate("", (s, pair) =>
                    //{
                    //    return s + string.Format("{0}={1},", pair.Key, pair.Value);
                    //});
                    _logger.LogInformation(errMessage);
                }

            }
            catch (Exception aex)
            {
                _logger.LogInformation("ERR exception: " + aex.Message);
            }



            isAddedOk = true;

            return isAddedOk;
        }

        [HttpGet("",Name = "GetDiscovery")]
        public IActionResult Get([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By Discovery Filter");

                // Deserialize the ontology filter
                DiscoveryFilters oFilter = new DiscoveryFilters();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association keyphrase filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<DiscoveryFilters>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlprecordassociationkeyphrasesreverse",_logger);

                // Create a default query
                TableQuery<RecordAssociationKeyPhraseReverseEntity> query = new TableQuery<RecordAssociationKeyPhraseReverseEntity>();
                if (oFilter.filters.Count > 0)
                {
                    string combinedFilter = "";

                    // *****************************
                    // Check for Keyphrase filters
                    // *****************************
                    foreach (DiscoveryFilter dfilter in oFilter.filters)
                    {
                        // Check for keyphrase filters
                        if (dfilter.Type==DiscoveryFilterType.keyphrase)
                        {
                            string cleanFilterPKey = Utils.CleanTableKey(dfilter.Value);
                            string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);

                            if (combinedFilter != "")
                            {
                                combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.Or, pkquery);
                            }
                            else
                            {
                                combinedFilter = pkquery;
                            }
                        }
                    }
                    // Create final combined query
                    query = new TableQuery<RecordAssociationKeyPhraseReverseEntity>().Where(combinedFilter);
                }
                List<RecordAssociationKeyPhraseReverseEntity> recordAssociationEntities = new List<RecordAssociationKeyPhraseReverseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordAssociationKeyPhraseReverseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordAssociationEntities.Count;

                    Task<TableQuerySegment<RecordAssociationKeyPhraseReverseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordAssociationKeyPhraseReverseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordAssociationEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordAssociationEntities.Count < query.TakeCount.Value) && recordAssociationEntities.Count < 500);    //!ct.IsCancellationRequested &&

                recordAssociationEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // **********************************
                // Check for file extension filters
                // **********************************
                List<string> filetypeFilters = new List<string>();
                foreach(DiscoveryFilter dfilter in oFilter.filters)
                {
                    if (dfilter.Type==DiscoveryFilterType.filetype)
                    {
                        filetypeFilters.Add(dfilter.Value);
                    }
                }
                if (filetypeFilters.Count>0)
                {
                    // Remove any file types not in the filter list
                    for (int i=recordAssociationEntities.Count-1; i >=0; i--)
                    {
                        string fileExtension = System.IO.Path.GetExtension(recordAssociationEntities[i].ItemUri);
                        if (fileExtension != null)
                        {
                            if (!filetypeFilters.Exists(e => e.Equals(fileExtension)))
                            {
                                // Remove the item
                                recordAssociationEntities.Remove(recordAssociationEntities[i]);
                            }
                        }
                    }
                }

                // **********************************
                // Check for system filters
                // **********************************
                List<string> systemFilters = new List<string>();
                foreach (DiscoveryFilter dfilter in oFilter.filters)
                {
                    if (dfilter.Type == DiscoveryFilterType.system)
                    {
                        systemFilters.Add(dfilter.Value.Replace("|","/"));
                    }
                }
                if (systemFilters.Count > 0)
                {
                    // Remove any files not in the filtered system(s)
                    for (int i = recordAssociationEntities.Count - 1; i >= 0; i--)
                    {
                        string itemUri = recordAssociationEntities[i].ItemUri;
                        bool isItemInSystem = false;
                        foreach(string system in systemFilters)
                        {
                            // Check if the item Uri starts with the system Uri
                            if (itemUri.StartsWith(system))
                            {
                                isItemInSystem = true;
                                break;
                            }
                        }
                        if (!isItemInSystem)
                        {
                            // Remove the item
                            recordAssociationEntities.Remove(recordAssociationEntities[i]);
                        }
                    }
                }

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);
            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Discovery Filter GET exception: " + ex.Message;
                //log.Info("Exception occurred extracting text from uploaded file \r\nError: " + ex.Message);
                if (ex.InnerException != null)
                {
                    exceptionMsg = exceptionMsg + "[" + ex.InnerException.Message + "]";
                }

                _logger.LogError(exceptionMsg);
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }
    

        [HttpGet("getsaved", Name = "GetSavedDiscovery")]
        public IActionResult GetSavedDiscovery([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the filter
                DiscoveryFilters oFilter = new DiscoveryFilters();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<DiscoveryFilters>(filter);
                }

                string storageAccountConnectionString = Utils.GetSecretOrEnvVar(ConfigurationProperties.AzureStorageAccountConnectionString, Configuration, _logger).Trim();
                // validate tika base address
                if (storageAccountConnectionString == "")
                {
                    _logger.LogWarning("Azure storage account connection string not set");
                    return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
                }
                else
                {
                    _logger.LogDebug("Azure storage account connection string loaded");
                }

                // Process the records
                CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

                // Create the table client. 
                //log.Info("Creating cloud table client");
                CloudTableClient tableClient = account.CreateCloudTableClient();

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = tableClient.GetTableReference("stlpdiscovery");
                Task tCreate = table.CreateIfNotExistsAsync();
                tCreate.Wait();

                // Create a default query
                TableQuery<DiscoveryEntity> query = new TableQuery<DiscoveryEntity>();
                if (oFilter.filters.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (DiscoveryFilter entry in oFilter.filters)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(entry.Value);
                        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);
                        combinedFilter = pkquery;
                    }
                    // Create final combined query
                    query = new TableQuery<DiscoveryEntity>().Where(combinedFilter);
                }
                List<DiscoveryEntity> spfileEntities = new List<DiscoveryEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<DiscoveryEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - spfileEntities.Count;

                    Task<TableQuerySegment<DiscoveryEntity>> tSeg = table.ExecuteQuerySegmentedAsync<DiscoveryEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    spfileEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || spfileEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&


                //no sorting
                //spfileEntities.Sort((x, y) => String.Compare(x.Label, y.Label));

                entityAsJson = JsonConvert.SerializeObject(spfileEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Discovery batch status GET exception: " + ex.Message;
                //log.Info("Exception occurred extracting text from uploaded file \r\nError: " + ex.Message);
                if (ex.InnerException != null)
                {
                    exceptionMsg = exceptionMsg + "[" + ex.InnerException.Message + "]";
                }

                _logger.LogError(exceptionMsg);
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }
    }

    class DiscoveryFilters
    {
        public DiscoveryFilters()
        {
            filters = new List<DiscoveryFilter>();
        }
        public List<DiscoveryFilter> filters;
    }

    class DiscoveryFilter
    {
        public string Type { get; set; }
        public DiscoveryOperator Operator { get; set; }
        public string Value { get; set; }
    }

    class DiscoveryEntity:TableEntity
    {
        public DiscoveryEntity() {
            Id = Guid.NewGuid();
            Name = "";
            Description = "";
            Version = 1;
            LastRunDate = Utils.AzureTableMinDateTime;
            NextRunDate = Utils.AzureTableMinDateTime;
            LastUpdated = Utils.AzureTableMinDateTime;
            LastUpdatedBy = "";
            SchedulesAsJson = "";
            FiltersAsJson = "";
        }

        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Version { get; set; }
        public DateTime LastRunDate { get; set; }
        public DateTime NextRunDate { get; set; }
        public DateTime LastUpdated { get; set; }
        public string LastUpdatedBy { get; set; }
        public string SchedulesAsJson { get; set; }
        public string FiltersAsJson { get; set; }
    }

    class DiscoverySchedule
    {
        public DiscoveryScheduleType Schedule { get; set; } 
        public DateTime RunOnceAt { get; set; }
    }

    enum DiscoveryScheduleType
    {
        RunOnce,
        Daily,
        Weekly,
        Monthly
    }

    internal static class DiscoveryFilterType
    {
        internal static string keyphrase = "keyphrase";
        internal static string filetype = "filetype";
        internal static string system = "system";
        internal static string metadata = "metadata";
    }

    enum DiscoveryOperator
    {
        eq
        ,ne
        ,gt
        ,ge
        ,lt
        ,le
    }
}
