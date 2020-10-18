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
    [Route("graph")]
    [Authorize]
    public class GraphController : Controller
    {


        public IConfiguration Configuration { get; set; }
        ILogger<GraphController> _logger;

        public GraphController(IConfiguration config, ILogger<GraphController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        class SystemFilter
        {
            public SystemFilter()
            {
                systems = new List<string>();
            }
            public List<string> systems;
        }

        [HttpGet("sentence/system", Name = "GetSentenceGraphBySystemFilter")]
        public IActionResult GetSentenceBySystemFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<SentenceStatsEntity> systemSentenceStats = new List<SentenceStatsEntity>();
            try
            {

                _logger.LogInformation("CPAPI: Get Sentence Graph By Record Filter");

                // Deserialize the filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
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

                // Get the key phrases for the record and for the record associations
                systemSentenceStats= GetSentenceStatsForSystem(oFilter);

                // Create the visualisation nodes
                grandparent gp = new grandparent("System");
                gp.label = "System";    //TODO: set as system label

                // Add sentence schema
                // TODO: support all RAs and/or ontologies
                parent p_sentence = new parent("AFDA");
                p_sentence.label = "AFDA";
                gp.children.Add(p_sentence);

                // Add sentence stats
                int i = 0;
                foreach (SentenceStatsEntity statEntity in systemSentenceStats)
                {
                    // Get the sentence stats for this system
                    List<SentenceStat> sentences = new List<SentenceStat>();
                    if (statEntity.JsonSentenceStats != null)
                    {
                        sentences = JsonConvert.DeserializeObject<List<SentenceStat>>(statEntity.JsonSentenceStats);
                    }
                    foreach(SentenceStat stat in sentences)
                    {
                        // Create a new child object for the class
                        child c = new child(stat.ClassNo, stat.NumEntries);
                        c.label = stat.Function;
                        p_sentence.children.Add(c);
                    }

                    i++;
                }


                // Serialize
                entityAsJson = JsonConvert.SerializeObject(gp, Formatting.None);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "KeyPhrase GET exception: " + ex.Message;
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


        class SentenceStatsEntity : TableEntity
        {
            public SentenceStatsEntity() { }
            public SentenceStatsEntity(string partitionKey, string rowKey)
            {
                this.PartitionKey = partitionKey;
                this.RowKey = rowKey;
                this.JsonSentenceStats = "[]";
            }
            public string JsonSentenceStats { get; set; }
        }

        internal class SentenceStat
        {
            public SentenceStat()
            {
                this.SchemaUri = "";
                this.Function = "";
                this.Activity = "";
                this.ClassNo = "";
                this.NumEntries = 0;
            }
            public string SchemaUri { get; set; }
            public string Function { get; set; }
            public string Activity { get; set; }
            public string ClassNo { get; set; }
            public int NumEntries { get; set; }
        }

        private List<SentenceStatsEntity> GetSentenceStatsForSystem(SystemFilter oFilter)
        {

            List<SentenceStatsEntity> systemSentences = new List<SentenceStatsEntity>();

            CloudTable tRecordAssociationKeyphrases = Utils.GetCloudTable("stlpsystems", _logger);

            // Create a default query
            TableQuery<SentenceStatsEntity> query = new TableQuery<SentenceStatsEntity>();

            string systemFilter = "";

            // Add any record association filters
            if (oFilter.systems.Count > 0)
            {
                foreach (string sif in oFilter.systems)
                {
                    if (sif != null && sif != "")
                    {
                        // Validate the record filter
                        string cleanFilterPKey = Utils.CleanTableKey(sif);
                        if (cleanFilterPKey.EndsWith("|"))
                        {
                            cleanFilterPKey = cleanFilterPKey + "|";
                        }

                        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);
                        if (systemFilter != "")
                        {
                            systemFilter = TableQuery.CombineFilters(systemFilter, TableOperators.Or, pkquery);
                        }
                        else
                        {
                            systemFilter = pkquery;
                        }
                    }
                }
            }


            

            // Create final combined query
            query = new TableQuery<SentenceStatsEntity>().Where(systemFilter);


            TableContinuationToken token = null;

            var runningQuery = new TableQuery<SentenceStatsEntity>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - systemSentences.Count;

                Task<TableQuerySegment<SentenceStatsEntity>> tSeg = tRecordAssociationKeyphrases.ExecuteQuerySegmentedAsync<SentenceStatsEntity>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                systemSentences.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || systemSentences.Count < query.TakeCount.Value) && systemSentences.Count < 20000);    //!ct.IsCancellationRequested &&


            

            return systemSentences;
        }

        public class grandparent
    {
        public grandparent(string name)
        {
            this.name = name;
            this.children = new List<parent>();
        }
        public string name { get; set; }
        public string label { get; set; }
        public int size { get; set; }
        public List<parent> children { get; set; }
    }

    public class parent
    {
        public parent(string name)
        {
            this.name = name;
            this.children = new List<child>();
        }
        public string name { get; set; }
        public string label { get; set; }
        public int size { get; set; }
        public List<child> children { get; set; }
    }

    public class child
    {
        public child(string name, int size)
        {
            this.name = name;
            this.size = size;
        }
        public string name { get; set; }
        public string label { get; set; }
        public int size { get; set; }
    }


















}
}