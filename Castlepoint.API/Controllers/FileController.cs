using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

  // Namespace for CloudConfigurationManager
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
    [Route("file", Name = "File")]
    [Authorize]
    public class FileController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<FileController> _logger;

        public FileController(IConfiguration config, ILogger<FileController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        [HttpGet("filter", Name = "GetSPFileByFilter")]
        public IActionResult GetByFilter([FromHeader] string spfilefilter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Validate the filter
                if (spfilefilter == null || spfilefilter.Length==0)
                {
                    _logger.LogDebug("Filter not provided, returning blank");
                    return new ObjectResult("[]");
                }

                // Deserialize the filter
                SPFileFilter oFilter = new SPFileFilter();
                if (spfilefilter!=null && spfilefilter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + spfilefilter.Length);
                    oFilter = JsonConvert.DeserializeObject<SPFileFilter>(spfilefilter);
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
                CloudTable table = tableClient.GetTableReference("stlpo365spfiles");
                Task tCreate = table.CreateIfNotExistsAsync();
                tCreate.Wait();

                // Create a default query
                TableQuery<SPFileProcessingStatusEntity> query = new TableQuery<SPFileProcessingStatusEntity>();
                if (oFilter.spfiles.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (SPFileFilterEntry filterentry in oFilter.spfiles)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(filterentry.spfileabsoluteuri);
                        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);
                        combinedFilter = pkquery;
                    }
                    // Create final combined query
                    query = new TableQuery<SPFileProcessingStatusEntity>().Where(combinedFilter);
                }
                List<SPFileProcessingStatusEntity> spfileEntities = new List<SPFileProcessingStatusEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<SPFileProcessingStatusEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - spfileEntities.Count;

                    Task<TableQuerySegment<SPFileProcessingStatusEntity>> tSeg = table.ExecuteQuerySegmentedAsync<SPFileProcessingStatusEntity>(runningQuery, token);
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
                string exceptionMsg = "SPFile batch status GET exception: " + ex.Message;
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
    class SPFileFilter
    {
        public SPFileFilter()
        {
            spfiles = new List<SPFileFilterEntry>();
        }
        public List<SPFileFilterEntry> spfiles;
    }

    class SPFileFilterEntry
    {
        public SPFileFilterEntry()
        {
            spfileabsoluteuri = "";
        }
        public string spfileabsoluteuri;
    }

    class SPFileProcessingStatusEntity:TableEntity
    {
        public SPFileProcessingStatusEntity() { }
        public string BatchStatus { get; set; }
        public DateTime LastModifiedTime { get; set; }
    }
}
