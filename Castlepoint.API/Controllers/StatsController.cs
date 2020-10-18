using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

using System.Net;
using Newtonsoft.Json;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Net.Http;
using System.Collections.Specialized;

  // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("stats")]
    [Authorize]
    public class StatsController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<StatsController> _logger;

        public StatsController(IConfiguration config, ILogger<StatsController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        [HttpGet("items", Name = "GetItemStats")]
        public IActionResult GetItemStats()
        {
            // Create filters
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", "cpstat", "eq");
            filters.Add(pkFilter);
            DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", "fileslastupdated", "eq");
            filters.Add(rkFilter);

            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<POCO.Stat> stats = DataFactory.Stats.GetStats(datacfg, filters);

            //// Create the table if it doesn't exist. 
            ////log.Info("Getting table reference");
            //CloudTable table = Utils.GetCloudTable("stlpstats", _logger);

            //// Check for keys
            //string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "cpstat");
            //string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "fileslastupdated");
            //string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

            //TableQuery<StatsEntity> query = new TableQuery<StatsEntity>().Where(combinedFilter);
            //List<StatsEntity> allStats = new List<StatsEntity>();

            //TableContinuationToken token = null;

            //var runningQuery = new TableQuery<StatsEntity>()
            //{
            //    FilterString = query.FilterString,
            //    SelectColumns = query.SelectColumns
            //};

            //do
            //{
            //    runningQuery.TakeCount = query.TakeCount - allStats.Count;

            //    Task<TableQuerySegment<StatsEntity>> tSeg = table.ExecuteQuerySegmentedAsync<StatsEntity>(runningQuery, token);
            //    tSeg.Wait();
            //    token = tSeg.Result.ContinuationToken;
            //    allStats.AddRange(tSeg.Result);

            //} while (token != null && (query.TakeCount == null || allStats.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            // Check if a single entry has been returned
            string entityAsJson = "";
            if (stats.Count==1)
            {
                StatFilesLastUpdated statsSorted = JsonConvert.DeserializeObject<StatFilesLastUpdated>(stats[0].JsonStats);
                statsSorted.stat.Sort((x, y) => DateTime.Compare(y.LastUpdatedDateTime,x.LastUpdatedDateTime));

                entityAsJson = JsonConvert.SerializeObject(statsSorted);
            }

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }

        [HttpGet("records", Name = "GetRecordStats")]
        public IActionResult GetRecordStats()
        {

            // Create filters
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", "cpstat", "eq");
            filters.Add(pkFilter);
            DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", "recordslastupdated", "eq");
            filters.Add(rkFilter);

            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<POCO.Stat> stats = DataFactory.Stats.GetStats(datacfg, filters);

            //// Create the table if it doesn't exist. 
            ////log.Info("Getting table reference");
            //CloudTable table = Utils.GetCloudTable("stlpstats", _logger);

            //// Check for keys
            //string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "cpstat");
            //string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "recordslastupdated");
            //string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

            //TableQuery<StatsEntity> query = new TableQuery<StatsEntity>().Where(combinedFilter);
            //List<StatsEntity> allStats = new List<StatsEntity>();

            //TableContinuationToken token = null;

            //var runningQuery = new TableQuery<StatsEntity>()
            //{
            //    FilterString = query.FilterString,
            //    SelectColumns = query.SelectColumns
            //};

            //do
            //{
            //    runningQuery.TakeCount = query.TakeCount - allStats.Count;

            //    Task<TableQuerySegment<StatsEntity>> tSeg = table.ExecuteQuerySegmentedAsync<StatsEntity>(runningQuery, token);
            //    tSeg.Wait();
            //    token = tSeg.Result.ContinuationToken;
            //    allStats.AddRange(tSeg.Result);

            //} while (token != null && (query.TakeCount == null || allStats.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            // Check if a single entry has been returned
            string entityAsJson = "";
            if (stats.Count == 1)
            {
                StatFilesLastUpdated statsSorted = JsonConvert.DeserializeObject<StatFilesLastUpdated>(stats[0].JsonStats);
                statsSorted.stat.Sort((x, y) => DateTime.Compare(y.LastUpdatedDateTime, x.LastUpdatedDateTime));

                entityAsJson = JsonConvert.SerializeObject(statsSorted);
            }

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }

        internal class StatsEntity : TableEntity
        {
            public StatsEntity()
            {

            }
            [IgnoreProperty]
            public string StatsType { get { return this.RowKey; } }
            public string JsonStats { get; set; }
        }

        internal class StatFilesLastUpdated
        {
            public StatFilesLastUpdated()
            {
                stat = new List<StatFileLastUpdated>();
            }
            public List<StatFileLastUpdated> stat { get; set; }
        }

        internal class StatFileLastUpdated
        {
            public StatFileLastUpdated()
            {
                this.LastUpdatedDateTime = Utils.AzureTableMinDateTime;
                this.LastUpdatedFileUri = "[]";
            }

            public DateTime LastUpdatedDateTime { get; set; }
            public string LastUpdatedFileUri { get; set; }
            public string LastUpdatedFileUrl
            {
                get
                {
                    // Validate the uri we have
                    if (this.LastUpdatedFileUri != null && this.LastUpdatedFileUri.Length > 0)
                    {
                        return this.LastUpdatedFileUri.Replace("|", "/");
                    }
                    else { return ""; }
                }
            }
        }

        [HttpGet("keyphrase", Name = "GetKeyPhraseStats")]
        public string GetKeyPhraseStats()
        {
            // Process the records
            string storageAccountConnectionString = Configuration.GetConnectionString("AzureCloudStorage"); //Utils.GetStorageAccountConnectionString();
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

            // Create the table client. 
            //log.Info("Creating cloud table client");
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // Create the table if it doesn't exist. 
            //log.Info("Getting table reference");
            CloudTable table = tableClient.GetTableReference("stlpstats");
            Task tCreate = table.CreateIfNotExistsAsync();
            tCreate.Wait();

            // Check for keys
            string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "CountByKeyPhrase");
            TableQuery<RecordStats> query = new TableQuery<RecordStats>().Where(pkFilter);



            RecordStatsJson allStats = new RecordStatsJson();
            TableContinuationToken token = null;

            var runningQuery = new TableQuery<RecordStats>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - allStats.stats.Count;

                Task<TableQuerySegment<RecordStats>> tSeg = table.ExecuteQuerySegmentedAsync<RecordStats>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                allStats.stats.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || allStats.stats.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            // Top 10 order by descending
            var itemList = from t in allStats.stats
                           orderby t.StatsCount descending
                           select t;
            allStats.stats = itemList.Take(10).ToList();

            string jsonStats = JsonConvert.SerializeObject(allStats);

            return jsonStats;
        }

        // GET: api/Stats
        [HttpGet("aggregation", Name ="GetAggregationStats")]
        public string GetAggregationStats(string ontologyUri = "")
        {
            string storageAccountConnectionString = Configuration.GetConnectionString("AzureCloudStorage"); //Utils.GetStorageAccountConnectionString();
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

            // Create the table client. 
            //log.Info("Creating cloud table client");
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // Create the table if it doesn't exist. 
            //log.Info("Getting table reference");
            CloudTable table = tableClient.GetTableReference("stlpstats");
            Task tCreate = table.CreateIfNotExistsAsync();
            tCreate.Wait();

            string combinedFilter = "";
            string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "CountByAggregation");
            if (ontologyUri != "")
            {
                combinedFilter = pkFilter;
            }
            else
            {
                // Single filter on partition key
                combinedFilter = pkFilter;
            }
            TableQuery<RecordStats> query = new TableQuery<RecordStats>().Where(combinedFilter);



            RecordStatsJson allStats = new RecordStatsJson();
            TableContinuationToken token = null;

            var runningQuery = new TableQuery<RecordStats>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - allStats.stats.Count;

                Task<TableQuerySegment<RecordStats>> tSeg = table.ExecuteQuerySegmentedAsync<RecordStats>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                allStats.stats.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || allStats.stats.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            


            // Top 10 order by descending
            var itemList = from t in allStats.stats
                           orderby t.StatsCount descending
                           select t;
            allStats.stats = itemList.Take(10).ToList();

            string jsonStats = JsonConvert.SerializeObject(allStats);

            return jsonStats;
        }

        // GET: api/Stats/5
        [HttpGet("aggregation/ontology/{ontologyuri}", Name = "GetAggregationStatsByOntologyUri")]
        public string Get(string ontologyuri)
        {
            return GetAggregationStats(ontologyuri);
        }
        
        // POST: api/Stats
        [HttpPost]
        public void Post([FromBody]string value)
        {
        }
        
        // PUT: api/Stats/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }
        
        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }

    class RecordStatsJson
    {
        public RecordStatsJson()
        {
            stats = new List<RecordStats>();
        }
        public List<RecordStats> stats;
    }

    class RecordStats : TableEntity
    {
        public RecordStats(string sourceUri, string itemUri)
        {
            this.PartitionKey = sourceUri;
            this.RowKey = itemUri;
            this.LastUpdated = DateTime.Now;
        }

        public RecordStats()
        {

        }

        [IgnoreProperty]
        public string SourceUri { get { return this.PartitionKey; } }
        [IgnoreProperty]
        public string ItemUri { get { return this.RowKey; } }
        public DateTime LastUpdated { get; set; }
        public long StatsCount { get; set; }
        public string Label { get; set; }
        public string Type { get; set; }
    }
}
