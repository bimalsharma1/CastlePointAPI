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
    [Route("visual")]
    [Authorize]
    public class VisualiseController : Controller
    {


        public IConfiguration Configuration { get; set; }
        ILogger<VisualiseController> _logger;

        public VisualiseController(IConfiguration config, ILogger<VisualiseController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        class KeyPhraseFilter
        {
            public KeyPhraseFilter()
            {
                records = new List<string>();
                recordassociations = new List<string>();
                keyphrases = new List<string>();
            }
            public List<string> records;
            public List<string> recordassociations;
            public List<string> keyphrases;
        }

        [HttpGet("record", Name = "GetKeyPhraseVisualiseByRecordFilter")]
        public IActionResult GetByRecordFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<KeyPhraseCount> recordKeyPhrases = new List<KeyPhraseCount>();
            List<KeyPhraseCount> recordAssociationKeyPhrases = new List<KeyPhraseCount>();
            try
            {

                _logger.LogInformation("CPAPI: Get By Record Filter");

                // Deserialize the filter
                KeyPhraseFilter oFilter = new KeyPhraseFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<KeyPhraseFilter>(filter);
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
                recordKeyPhrases= GetRecordKeyPhrasesForRecordAuthority(oFilter);
                recordAssociationKeyPhrases = GetRecordKeyPhrasesForRecordAssociations(oFilter);

                // Create the visualisation nodes
                grandparent gp = new grandparent(oFilter.records[0]);
                gp.label = "Record";

                // Add record authority key phrases
                // TODO: support all RAs and/or ontologies
                parent p_ra = new parent("AFDA");
                p_ra.label = "AFDA";
                gp.children.Add(p_ra);

                // Add record authority key phrases
                int i = 0;
                foreach (KeyPhraseCount kpcount in recordKeyPhrases)
                {
                    // Create a new child object for the keyphrase
                    child c = new child(kpcount.KeyPhrase, kpcount.Count);
                    c.label = kpcount.KeyPhrase;
                    p_ra.children.Add(c);

                    // Check if we have reached maximum number
                    i++;
                    if (i >= 20) { break; }
                }

                // Add record association key phrases
                parent p_recassociation = new parent("Default");
                p_recassociation.label = "Default";
                gp.children.Add(p_recassociation);

                // Add record association key phrases
                i = 0;
                foreach (KeyPhraseCount kpcount in recordAssociationKeyPhrases)
                {
                    // Create a new child object for the keyphrase
                    child c = new child(kpcount.KeyPhrase, kpcount.Count);
                    c.label = kpcount.KeyPhrase;
                    p_recassociation.children.Add(c);

                    // Check if we have reached maximum number
                    i++;
                    if (i >= 20) { break; }
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

        private List<KeyPhraseCount> GetRecordKeyPhrasesForRecordAuthority(KeyPhraseFilter oFilter)
        {

            List<KeyPhraseCount> keyPhrases = new List<KeyPhraseCount>();

            CloudTable tRecordAssociationKeyphrases = Utils.GetCloudTable("stlprecordkeyphrases", _logger);

            // Create a default query
            TableQuery<RecordKeyPhraseEntity> query = new TableQuery<RecordKeyPhraseEntity>();

            //string finalFilter = "";
            string recordFilter = "";
            //string recordAssociationFilter = "";
            string keyphraseFilter = "";
            string combinedFilter = "";

            // Add any record association filters
            if (oFilter.records.Count > 0)
            {
                foreach (string rif in oFilter.records)
                {
                    if (rif != null && rif != "")
                    {
                        // Validate the record filter
                        string cleanFilterPKey = Utils.CleanTableKey(rif);
                        if (!cleanFilterPKey.EndsWith("|"))
                        {
                            cleanFilterPKey = cleanFilterPKey + "|";
                        }

                        string pkqueryStart = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, cleanFilterPKey);
                        string pkqueryEnd = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterPKey));
                        string combinedRecordFilter = TableQuery.CombineFilters(pkqueryStart, TableOperators.And, pkqueryEnd);
                        if (recordFilter != "")
                        {
                            recordFilter = TableQuery.CombineFilters(recordFilter, TableOperators.Or, combinedRecordFilter);
                        }
                        else
                        {
                            recordFilter = combinedRecordFilter;
                        }
                    }
                }
            }

            // Add any keyphrase filters
            if (oFilter.keyphrases.Count > 0)
            {
                foreach (string rif in oFilter.keyphrases)
                {
                    if (rif != null && rif != "")
                    {
                        string cleanFilterRKey = Utils.CleanTableKey(rif);

                        string rkqueryStart = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, cleanFilterRKey);
                        string rkqueryEnd = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterRKey));

                        string rkqueryCombined = TableQuery.CombineFilters(rkqueryStart, TableOperators.And, rkqueryEnd);

                        if (keyphraseFilter != "")
                        {
                            keyphraseFilter = TableQuery.CombineFilters(keyphraseFilter, TableOperators.Or, rkqueryCombined);
                        }
                        else
                        {
                            keyphraseFilter = rkqueryCombined;
                        }
                    }
                }
            }

            // Combine querys if needed
            if (recordFilter.Length > 0)
            {
                if (keyphraseFilter.Length > 0)
                {
                    // Combine queries when both filters are set
                    combinedFilter = TableQuery.CombineFilters(recordFilter, TableOperators.And, keyphraseFilter);
                }
                else
                {
                    combinedFilter = recordFilter;
                }
            }
            else
            {
                if (keyphraseFilter.Length > 0)
                {
                    combinedFilter = keyphraseFilter;
                }
            }

            // Create final combined query
            query = new TableQuery<RecordKeyPhraseEntity>().Where(combinedFilter);


            List<RecordKeyPhraseEntity> keyphraseEntities = new List<RecordKeyPhraseEntity>();
            TableContinuationToken token = null;

            var runningQuery = new TableQuery<RecordKeyPhraseEntity>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - keyphraseEntities.Count;

                Task<TableQuerySegment<RecordKeyPhraseEntity>> tSeg = tRecordAssociationKeyphrases.ExecuteQuerySegmentedAsync<RecordKeyPhraseEntity>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                keyphraseEntities.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || keyphraseEntities.Count < query.TakeCount.Value) && keyphraseEntities.Count < 20000);    //!ct.IsCancellationRequested &&


            //keyphraseEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

            // Return only distinct keyphrases
            foreach (RecordKeyPhraseEntity kp in keyphraseEntities)
            {
                KeyPhraseCount foundKPCount = keyPhrases.Find(x => (x.KeyPhrase == kp.KeyPhrase));
                if (foundKPCount == null)
                {
                    KeyPhraseCount newKPCount = new KeyPhraseCount(kp.KeyPhrase, 1);
                    keyPhrases.Add(newKPCount);
                }
                else
                {
                    // Increment the number of keyphrases found
                    foundKPCount.Count++;
                }
            }

            // Sort by most common keyphrase in descending order
            keyPhrases.Sort((x, y) => y.Count.CompareTo(x.Count));

            return keyPhrases;
        }

        private List<KeyPhraseCount> GetRecordKeyPhrasesForRecordAssociations(KeyPhraseFilter oFilter)
        {

            List<KeyPhraseCount> keyPhrases = new List<KeyPhraseCount>();

            CloudTable tRecordAssociationKeyphrases = Utils.GetCloudTable("stlprecordassociationkeyphrases", _logger);

            // Create a default query
            TableQuery<RecordKeyPhraseEntity> query = new TableQuery<RecordKeyPhraseEntity>();

            //string finalFilter = "";
            string recordFilter = "";
            //string recordAssociationFilter = "";
            string keyphraseFilter = "";
            string combinedFilter = "";

            // Add any record association filters
            if (oFilter.records.Count > 0)
            {
                foreach (string rif in oFilter.records)
                {
                    if (rif != null && rif != "")
                    {
                        // Validate the record filter
                        string cleanFilterPKey = Utils.CleanTableKey(rif);
                        if (!cleanFilterPKey.EndsWith("|"))
                        {
                            cleanFilterPKey = cleanFilterPKey + "|";
                        }

                        string pkqueryStart = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, cleanFilterPKey);
                        string pkqueryEnd = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterPKey));
                        string combinedRecordFilter = TableQuery.CombineFilters(pkqueryStart, TableOperators.And, pkqueryEnd);
                        if (recordFilter != "")
                        {
                            recordFilter = TableQuery.CombineFilters(recordFilter, TableOperators.Or, combinedRecordFilter);
                        }
                        else
                        {
                            recordFilter = combinedRecordFilter;
                        }
                    }
                }
            }

            // Add any keyphrase filters
            if (oFilter.keyphrases.Count > 0)
            {
                foreach (string rif in oFilter.keyphrases)
                {
                    string cleanFilterRKey = Utils.CleanTableKey(rif);

                    string rkqueryStart = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, cleanFilterRKey);
                    string rkqueryEnd = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterRKey));

                    string rkqueryCombined = TableQuery.CombineFilters(rkqueryStart, TableOperators.And, rkqueryEnd);

                    if (keyphraseFilter != "")
                    {
                        keyphraseFilter = TableQuery.CombineFilters(keyphraseFilter, TableOperators.Or, rkqueryCombined);
                    }
                    else
                    {
                        keyphraseFilter = rkqueryCombined;
                    }

                }
            }

            // Combine querys if needed
            if (recordFilter.Length > 0)
            {
                if (keyphraseFilter.Length > 0)
                {
                    // Combine queries when both filters are set
                    combinedFilter = TableQuery.CombineFilters(recordFilter, TableOperators.And, keyphraseFilter);
                }
                else
                {
                    combinedFilter = recordFilter;
                }
            }
            else
            {
                if (keyphraseFilter.Length > 0)
                {
                    combinedFilter = keyphraseFilter;
                }
            }

            // Create final combined query
            query = new TableQuery<RecordKeyPhraseEntity>().Where(combinedFilter);


            List<RecordKeyPhraseEntity> keyphraseEntities = new List<RecordKeyPhraseEntity>();
            TableContinuationToken token = null;

            var runningQuery = new TableQuery<RecordKeyPhraseEntity>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - keyphraseEntities.Count;

                Task<TableQuerySegment<RecordKeyPhraseEntity>> tSeg = tRecordAssociationKeyphrases.ExecuteQuerySegmentedAsync<RecordKeyPhraseEntity>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                keyphraseEntities.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || keyphraseEntities.Count < query.TakeCount.Value) && keyphraseEntities.Count < 20000);    //!ct.IsCancellationRequested &&


            //keyphraseEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

            // Return only distinct keyphrases
            foreach (RecordKeyPhraseEntity kp in keyphraseEntities)
            {
                KeyPhraseCount foundKPCount = keyPhrases.Find(x => (x.KeyPhrase == kp.RowKey));
                if (foundKPCount == null)
                {
                    KeyPhraseCount newKPCount = new KeyPhraseCount(kp.RowKey, 1);
                    keyPhrases.Add(newKPCount);
                }
                else
                {
                    // Increment the number of keyphrases found
                    foundKPCount.Count++;
                }
            }

            // Sort by most common keyphrase in descending order
            keyPhrases.Sort((x, y) => y.Count.CompareTo(x.Count));

            return keyPhrases;
        }



        private class RecordKeyPhraseEntity : TableEntity
    {
        public RecordKeyPhraseEntity(string sourceUri, string itemUri)
        {
            this.PartitionKey = sourceUri;
            this.RowKey = itemUri;

        }

        public RecordKeyPhraseEntity()
        {

        }

        //public string Properties { get; set; } 
        [IgnoreProperty]
        public string SourceUri { get { return this.PartitionKey; } }
        [IgnoreProperty]
        public string ItemUriKeyPhrase { get { return this.RowKey; } }
        public string ItemUri { get; set; }
        public string KeyPhrase { get; set; }
        public string RASchemaUri { get; set; }

    }

    public class OntologyEntity : TableEntity
    {
        [IgnoreProperty]
        public string Version { get { return this.RowKey; } }
        public string Description { get; set; }
        public string Type { get; set; }
        public string Label { get; set; }
    }

    public class OntologyKeyPhraseCount
    {
        public OntologyKeyPhraseCount(string OntologyUri)
        {
            this.OntologyUri = OntologyUri;
            this.KPCount = new List<KeyPhraseCount>();
        }
        public string OntologyUri { get; set; }
        public List<KeyPhraseCount> KPCount { get; set; }
    }

    public class KeyPhraseCount
    {
        public KeyPhraseCount(string KeyPhrase, int initialCount)
        {
            this.KeyPhrase = KeyPhrase;
            this.Count = initialCount;
        }

        public string KeyPhrase { get; set; }
        public int Count { get; set; }
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