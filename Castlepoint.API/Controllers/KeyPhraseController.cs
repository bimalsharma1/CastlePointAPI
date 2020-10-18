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
using System.Diagnostics;

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("keyphrase", Name = "KeyPhrase")]
    [Authorize]
    public class KeyPhraseController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<KeyPhraseController> _logger;

        public KeyPhraseController(IConfiguration config, ILogger<KeyPhraseController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        // GET: record
        [HttpGet]
        public IActionResult Get()
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

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


                // Process the keyphrases
                CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

                // Create the table client. 
                //log.Info("Creating cloud table client");
                CloudTableClient tableClient = account.CreateCloudTableClient();

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = tableClient.GetTableReference("stlprecordassociationkeyphrases");
                Task tCreate = table.CreateIfNotExistsAsync();
                tCreate.Wait();

                // Create a default query
                TableQuery<KeyPhraseEntity> query = new TableQuery<KeyPhraseEntity>();

                List<KeyPhraseEntity> keyphraseEntities = new List<KeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<KeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - keyphraseEntities.Count;

                    Task<TableQuerySegment<KeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<KeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    keyphraseEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || keyphraseEntities.Count < query.TakeCount.Value) && keyphraseEntities.Count<200);    //!ct.IsCancellationRequested &&


                keyphraseEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(keyphraseEntities, Formatting.Indented);

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

        [HttpGet("filter", Name = "GetKeyPhraseByKeyPhraseFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<string> keyPhrases = new List<string>();
            try
            {

                _logger.LogInformation("CPAPI: Get By KeyPhrase Filter");

                // Deserialize the ontology filter
                KeyPhraseFilter oFilter = new KeyPhraseFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing KeyPhrase filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<KeyPhraseFilter>(filter);
                }

                // Get the table
                CloudTable table = Utils.GetCloudTable("stlprecordassociationkeyphrases", _logger);

                // Create a default query
                TableQuery<KeyPhraseEntity> query = new TableQuery<KeyPhraseEntity>();
                if (oFilter.keyphrases.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (string rif in oFilter.keyphrases)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif);

                        string pkqueryStart = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, cleanFilterPKey);
                        string pkqueryEnd = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterPKey));

                        string pkqueryCombined = TableQuery.CombineFilters(pkqueryStart, TableOperators.And, pkqueryEnd);

                        if (combinedFilter != "")
                        {
                            combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.And, pkqueryCombined);
                        }
                        else
                        {
                            combinedFilter = pkqueryCombined;
                        }

                        
                    }
                    // Create final combined query
                    query = new TableQuery<KeyPhraseEntity>().Where(combinedFilter);
                }
                List<KeyPhraseEntity> keyphraseEntities = new List<KeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<KeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - keyphraseEntities.Count;

                    Task<TableQuerySegment<KeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<KeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    keyphraseEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || keyphraseEntities.Count < query.TakeCount.Value) && keyphraseEntities.Count < 1000);    //!ct.IsCancellationRequested &&


                //keyphraseEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // Return only distinct keyphrases
                foreach(KeyPhraseEntity kp in keyphraseEntities)
                {
                    if (!keyPhrases.Contains(kp.RowKey))
                    {
                        keyPhrases.Add(kp.RowKey);
                    }
                }
                keyPhrases.Sort((x, y) => String.Compare(x, y));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(keyPhrases, Formatting.Indented);

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

        [HttpGet("reversefilter", Name = "GetKeyPhraseByReverseKeyPhraseFilter")]
        public IActionResult GetByReverseFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<string> keyPhrases = new List<string>();
            try
            {

                _logger.LogInformation("CPAPI: Get By Reverse KeyPhrase Filter");

                // Deserialize the ontology filter
                KeyPhraseFilter oFilter = new KeyPhraseFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing KeyPhrase filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<KeyPhraseFilter>(filter);
                }

                List<string> wordsToFind = new List<string>();

                // Create a default query
                List <DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.keyphrases.Count > 0)
                {
                    //string combinedFilter = "";
                    foreach (string rif in oFilter.keyphrases)
                    {

                        string cleanFilterPKey = Utils.CleanTableKey(rif);
                        //string cleanFilterRKey = Utils.CleanTableKey(rif);

                        DataFactory.Filter pkFilterGE = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "ge");
                        DataFactory.Filter pkFilterLT = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(cleanFilterPKey), "lt");
                        filters.Add(pkFilterGE);
                        filters.Add(pkFilterLT);

                        // Also split any words in this term for later word search
                        string[] splitwords = cleanFilterPKey.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                        wordsToFind.AddRange(splitwords);
                    }
                }

                _logger.LogInformation("GetByReverseFilter: getting keyphraseEntities for words=" + oFilter.keyphrases);
                Stopwatch st = new Stopwatch();
                st.Start();
                List<POCO.RecordAssociationKeyPhraseReverse> keyphraseEntities = new List<POCO.RecordAssociationKeyPhraseReverse>();
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                keyphraseEntities = DataFactory.KeyPhrase.GetReverseKeyPhrases(dataConfig, filters);
                st.Stop();
                _logger.LogInformation("GetByReverseFilter: keyphraseEntities=" + keyphraseEntities.Count.ToString() + " timer=" + st.ElapsedMilliseconds.ToString() + "ms");

                //// Now get reverse keyphrase words
                //List<POCO.RecordAssociationKeyPhraseReverseWord> words = new List<POCO.RecordAssociationKeyPhraseReverseWord>();
                //if (wordsToFind.Count>0)
                //{
                //    // Find matches for each word and add to our keyphrase set
                //    foreach (string w in wordsToFind)
                //    {
                //        // Create filter and search
                //        List<DataFactory.Filter> wordfilter = new List<DataFactory.Filter>();
                //        DataFactory.Filter word = new DataFactory.Filter("PartitionKey", w, "eq");
                //        wordfilter.Add(word);
                //        _logger.LogInformation("GetByReverseFilter: getting word matches for word=" + w);
                //        st.Restart();
                //        string nextPageId = string.Empty;
                //        List<POCO.RecordAssociationKeyPhraseReverseWord> wordMatches = DataFactory.KeyPhrase.GetReverseKeyPhraseWords(dataConfig, wordfilter, string.Empty, 2000, out nextPageId);
                //        st.Stop();
                //        _logger.LogInformation("GetByReverseFilter: word=" + w + " wordMatches=" + wordMatches.Count.ToString() + " timer=" + st.ElapsedMilliseconds.ToString() + "ms");
                //        words.AddRange(wordMatches);
                //    }
                //}

                //// Check if we have any word matches
                //if (words.Count>0)
                //{
                //    foreach(POCO.RecordAssociationKeyPhraseReverseWord word in words)
                //    {
                //        // Split the rowkey
                //        string[] key = word.RowKey.Split("|||");
                //        if (key.Length==3)
                //        {
                //            // Create a new keyphrase reverse object using this word match
                //            // multi-part key format is [record assoc uri] [key phrase] [key phrase location]
                //            POCO.RecordAssociationKeyPhraseReverse kpreverse = new POCO.RecordAssociationKeyPhraseReverse(key[1], key[0], key[2]);
                //            keyphraseEntities.Add(kpreverse);
                //        }
                //    }
                //}

                // Return only distinct keyphrases
                _logger.LogInformation("GetByReverseFilter: creating return data");
                st.Restart();
                foreach (POCO.RecordAssociationKeyPhraseReverse kp in keyphraseEntities)
                {
                    if (!keyPhrases.Contains(kp.PartitionKey))
                    {
                        keyPhrases.Add(kp.PartitionKey);
                    }
                }
                keyPhrases.Sort((x, y) => String.Compare(x, y));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(keyPhrases, Formatting.Indented);
                st.Stop();
                _logger.LogInformation("GetByReverseFilter: created return data timer=" + st.ElapsedMilliseconds.ToString() + "ms");

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

        [HttpGet("recordassociation", Name = "GetKeyPhraseByRecordAssocitiationFilter")]
        public IActionResult GetByRecordAssociationFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<string> keyPhrases = new List<string>();
            try
            {

                _logger.LogInformation("CPAPI: Get By Record Filter");

                // Deserialize the ontology filter
                KeyPhraseFilter oFilter = new KeyPhraseFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);

                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);

                    oFilter = JsonConvert.DeserializeObject<KeyPhraseFilter>(filterDecoded);
                }

                // TODO filters
                POCO.RecordAssociation filterRecAssoc = new POCO.RecordAssociation();
                filterRecAssoc.RowKey = oFilter.recordassociations[0];

                List<POCO.RecordAssociationKeyPhrase> kps = DataFactory.RecordAssociation.GetKeyPhrases(Utils.GetDataConfig(), filterRecAssoc);

                //// Get the table
                //CloudTable tRecordAssociationKeyphrases = Utils.GetCloudTable("stlprecordassociationkeyphrases", _logger);

                //// Create a default query
                //TableQuery <KeyPhraseEntity> query = new TableQuery<KeyPhraseEntity>();

                ////string finalFilter = "";
                //string recordAssociationFilter = "";
                //string keyphraseFilter = "";
                //string combinedFilter = "";

                //// Add any record association filters
                //if (oFilter.recordassociations.Count > 0)
                //{
                //    foreach (string rif in oFilter.recordassociations)
                //    {
                //        string cleanFilterPKey = Utils.CleanTableKey(rif);

                //        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);

                //        if (recordAssociationFilter != "")
                //        {
                //            recordAssociationFilter = TableQuery.CombineFilters(recordAssociationFilter, TableOperators.Or, pkquery);
                //        }
                //        else
                //        {
                //            recordAssociationFilter = pkquery;
                //        }
                //    }
                //}

                //// Add any keyphrase filters
                //if (oFilter.keyphrases.Count>0)
                //{ 
                //    foreach (string rif in oFilter.keyphrases)
                //    {
                //        string cleanFilterRKey = Utils.CleanTableKey(rif);

                //        string queryKeyPhrase = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, cleanFilterRKey);

                //        if (keyphraseFilter != "")
                //        {
                //            keyphraseFilter = TableQuery.CombineFilters(keyphraseFilter, TableOperators.Or, queryKeyPhrase);
                //        }
                //        else
                //        {
                //            keyphraseFilter = queryKeyPhrase;
                //        }

                //    }
                //}

                //// Combine querys if needed
                //if (recordAssociationFilter.Length > 0)
                //{
                //    if (keyphraseFilter.Length > 0)
                //    {
                //        // Combine queries when both filters are set
                //        combinedFilter = TableQuery.CombineFilters(recordAssociationFilter, TableOperators.And, keyphraseFilter);
                //    }
                //    else
                //    {
                //        combinedFilter = recordAssociationFilter;
                //    }
                //}
                //else
                //{
                //    if (keyphraseFilter.Length>0)
                //    {
                //        combinedFilter = keyphraseFilter;
                //    }
                //}

                //// Create final combined query
                //query = new TableQuery<KeyPhraseEntity>().Where(combinedFilter);


                //List<KeyPhraseEntity> keyphraseEntities = new List<KeyPhraseEntity>();
                //TableContinuationToken token = null;

                //var runningQuery = new TableQuery<KeyPhraseEntity>()
                //{
                //    FilterString = query.FilterString,
                //    SelectColumns = query.SelectColumns
                //};

                //do
                //{
                //    runningQuery.TakeCount = query.TakeCount - keyphraseEntities.Count;

                //    Task<TableQuerySegment<KeyPhraseEntity>> tSeg = tRecordAssociationKeyphrases.ExecuteQuerySegmentedAsync<KeyPhraseEntity>(runningQuery, token);
                //    tSeg.Wait();
                //    token = tSeg.Result.ContinuationToken;
                //    keyphraseEntities.AddRange(tSeg.Result);

                //} while (token != null && (query.TakeCount == null || keyphraseEntities.Count < query.TakeCount.Value) && keyphraseEntities.Count < 1000);    //!ct.IsCancellationRequested &&


                ////keyphraseEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // Return only distinct keyphrases
                foreach (POCO.RecordAssociationKeyPhrase kp in kps)
                {
                    if (!keyPhrases.Contains(kp.RowKey))
                    {
                        keyPhrases.Add(kp.RowKey);
                    }
                }
                keyPhrases.Sort((x, y) => String.Compare(x, y));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(keyPhrases, Formatting.Indented);

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

        [HttpGet("record", Name = "GetKeyPhraseByRecordFilter")]
        public IActionResult GetByRecordFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<KeyPhraseCount> keyPhrases = new List<KeyPhraseCount>();
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

                CloudTable tRecordAssociationKeyphrases = Utils.GetCloudTable("stlprecordassociationkeyphrases", _logger);

                // Create a default query
                TableQuery<KeyPhraseEntity> query = new TableQuery<KeyPhraseEntity>();

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
                        // Validate the record filter value
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
                query = new TableQuery<KeyPhraseEntity>().Where(combinedFilter);


                List<KeyPhraseEntity> keyphraseEntities = new List<KeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<KeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - keyphraseEntities.Count;

                    Task<TableQuerySegment<KeyPhraseEntity>> tSeg = tRecordAssociationKeyphrases.ExecuteQuerySegmentedAsync<KeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    keyphraseEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || keyphraseEntities.Count < query.TakeCount.Value) && keyphraseEntities.Count < 20000);    //!ct.IsCancellationRequested &&


                //keyphraseEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // Return only distinct keyphrases
                foreach (KeyPhraseEntity kp in keyphraseEntities)
                {
                    KeyPhraseCount foundKPCount = keyPhrases.Find(x => (x.KeyPhrase == kp.RowKey));
                    if (foundKPCount == null)
                    {
                        KeyPhraseCount newKPCount = new KeyPhraseCount(kp.RowKey);
                        keyPhrases.Add(newKPCount);
                    }
                    else
                    {
                        // Increment the number of keyphrases found
                        foundKPCount.Count++;
                    }
                }
                keyPhrases.Sort((x, y) => String.Compare(x.KeyPhrase, y.KeyPhrase));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(keyPhrases, Formatting.Indented);

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


        class KeyPhraseEntity : TableEntity
        {
            public KeyPhraseEntity(string sourceUri, string itemUri)
            {
                this.PartitionKey = sourceUri;
                this.RowKey = itemUri;

            }

            public KeyPhraseEntity()
            {

            }

            [IgnoreProperty]
            public string Keyphrase { get { return this.RowKey; } }

        }

        class KeyPhraseReverseEntity : TableEntity
        {


            public KeyPhraseReverseEntity()
            {

            }

            [IgnoreProperty]
            public string Keyphrase { get { return this.PartitionKey; } }
            [IgnoreProperty]
            public string ItemUri { get { return this.RowKey; } }

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


    }

    internal class KeyPhraseCount
    {
        public KeyPhraseCount() { this.Count = 0; }
        public KeyPhraseCount(string keyPhrase)
        {
            this.KeyPhrase = keyPhrase;
            this.Count = 1;
        }

        public string KeyPhrase { get; set; }
        public int Count { get; set; }
    }
}
