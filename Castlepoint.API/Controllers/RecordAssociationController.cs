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

using LazyCache;

using Newtonsoft.Json;

using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.FileProviders;
using System.IO;
using DocumentFormat.OpenXml.Spreadsheet;

namespace Castlepoint.REST.Controllers
{

    [Produces("application/json")]
    [Route("recordassociation", Name = "Record Association")]
    [Authorize]
    public class RecordAssociationController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<RecordAssociationController> _logger;
        private readonly IAppCache _cacheRecordAssociations;

        public RecordAssociationController(IConfiguration config, ILogger<RecordAssociationController> logger)
        {
            Configuration = config;
            _logger = logger;
            _cacheRecordAssociations = new CachingService();
        }


        // GET: api/RecordAssociations
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


                // Process the records
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
                TableQuery<RecordAssociationKeyPhraseEntity> query = new TableQuery<RecordAssociationKeyPhraseEntity>();

                List<RecordAssociationKeyPhraseEntity> recordAssociationEntities = new List<RecordAssociationKeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordAssociationKeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordAssociationEntities.Count;

                    Task<TableQuerySegment<RecordAssociationKeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordAssociationKeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordAssociationEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordAssociationEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&


                recordAssociationEntities.Sort((x, y) => String.Compare(x.PartitionKey, y.PartitionKey));

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);

            }
            catch(Exception ex)
            {
                string exceptionMsg = "Record Association GET exception: " + ex.Message;
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

        [HttpGet("ontologymatch", Name = "GetOntologyMatchByFilter")]
        public IActionResult GetOntologyMatchByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetOntologyMatchByFilter");

                // Deserialize the ontology filter
                RecordAssociationFilter oFilter = new RecordAssociationFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing RecordAssociation filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationFilter>(filter);
                }

                // Create the filters for the datafactory
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.recordassociations.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (RecordAssociation rif in oFilter.recordassociations)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.recorduri);    // Utils.CleanTableKey(rif.recorduri);
                        string cleanFilterRKey = Utils.CleanTableKey(rif.recordassociationuri);

                        //if (cleanFilterPKey != "")
                        //{
                        //    DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                        //    filters.Add(pkfilt);
                        //}
                        // Check if an item key has been provided
                        if (cleanFilterRKey != "")
                        {
                            DataFactory.Filter rkfilt = new DataFactory.Filter("PartitionKey", cleanFilterRKey, "eq");
                            filters.Add(rkfilt);
                        }
                    }

                }
                else
                {
                    _logger.LogInformation("RecordKeyPhraseFilter query BLANK");
                }

                DataFactory.DataConfig dataCfg = Utils.GetDataConfig();

                List<POCO.KeyPhraseToRecordLookup> keyphrases = DataFactory.Record.GetRecordKeyPhrases(dataCfg, filters);

                keyphrases.Sort((x, y) => String.Compare(x.KeyPhrase, y.KeyPhrase));

                entityAsJson = JsonConvert.SerializeObject(keyphrases, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record GET exception: " + ex.Message;
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


        [HttpGet("record", Name = "GetRecordByRecordAssociation")]
        public IActionResult GetRecordByRecordAssociation([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {
                _logger.LogInformation("CPAPI: GetRecordByRecordAssociation");

                // Deserialize the ontology filter
                RecordAssociationFilter oFilter = new RecordAssociationFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing RecordAssociation filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationFilter>(filter);
                }

                // Make sure a record association has been requested
                if (oFilter.recordassociations.Count==0)
                {
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Create the filters for the datafactory
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.recordassociations.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (RecordAssociation rif in oFilter.recordassociations)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.recorduri);    // Utils.CleanTableKey(rif.recorduri);
                        string cleanFilterRKey = Utils.CleanTableKey(rif.recordassociationuri);

                        // Check if a Record has been provided
                        if (cleanFilterPKey != "")
                        {
                            DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                            filters.Add(pkfilt);
                        }

                        // Check if an item key has been provided
                        if (cleanFilterRKey != "")
                        {
                            DataFactory.Filter rkfilt = new DataFactory.Filter("RowKey", cleanFilterRKey, "eq");
                            filters.Add(rkfilt);
                        }
                    }

                }
                else
                {
                    _logger.LogInformation("RecordAssociationFilter query BLANK");
                }

                DataFactory.DataConfig dataCfg = Utils.GetDataConfig();

                List<POCO.RecordToRecordAssociation> recordAssocs = DataFactory.Record.GetRecordToRecordsAssociations(dataCfg, filters);

                recordAssocs.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recordAssocs, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "RecordByRecordAssociation GET exception: " + ex.Message;
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

        [HttpGet("keyphrasefilter", Name = "GetRecordAssociationByKeyphraseFilter")]
        public IActionResult GetByKeyPhraseFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By KeyPhrase Filter");

                // Deserialize the ontology filter
                RecordAssociationKeyPhraseFilter oFilter = new RecordAssociationKeyPhraseFilter();
                if (filter!=null && filter.Length>0)
                {
                    _logger.LogDebug("Deserializing record association keyphrase filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationKeyPhraseFilter>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlprecordassociationkeyphrases", _logger);

                // Create a default query
                TableQuery<RecordAssociationKeyPhraseEntity> query = new TableQuery<RecordAssociationKeyPhraseEntity>();
                if (oFilter.keyphrases.Count>0)
                {
                    string combinedFilter = "";
                    foreach(string of in oFilter.keyphrases)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(of);
                        string pkquery = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, cleanFilterPKey);

                        if (combinedFilter!="")
                        {
                            combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.Or, pkquery);
                        }
                        else
                        {
                            combinedFilter = pkquery;
                        }
                    }
                    // Create final combined query
                    query = new TableQuery<RecordAssociationKeyPhraseEntity>().Where(combinedFilter);
                }
                List<RecordAssociationKeyPhraseEntity> recordAssociationEntities = new List<RecordAssociationKeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordAssociationKeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordAssociationEntities.Count;

                    Task<TableQuerySegment<RecordAssociationKeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordAssociationKeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordAssociationEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordAssociationEntities.Count < query.TakeCount.Value) && recordAssociationEntities.Count<200);    //!ct.IsCancellationRequested &&


                recordAssociationEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Key Phrase Filter GET exception: " + ex.Message;
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

        [HttpGet("keyphrasereversefilter", Name = "GetRecordAssociationByKeyphraseReverseFilter")]
        public IActionResult GetByKeyPhraseReverseFilter([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By KeyPhrase Reverse Filter");

                // Deserialize the ontology filter
                RecordAssociationKeyPhraseFilter oFilter = new RecordAssociationKeyPhraseFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association keyphrase filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationKeyPhraseFilter>(filter);
                }

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                string matchType = string.Empty;
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                    matchType = dataPaging.matchType;
                }

                // Create a default query
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.keyphrases.Count > 0)
                {
                    //string combinedFilter = "";
                    foreach (string of in oFilter.keyphrases)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(of);
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                        filters.Add(pkFilter);
                    }
                }

                List<POCO.RecordAssociationKeyPhraseReverse> recordAssociationEntities = new List<POCO.RecordAssociationKeyPhraseReverse>();

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                string nextPageId = string.Empty;
                recordAssociationEntities = DataFactory.RecordAssociation.GetByReverseKeyPhrases(dataConfig, filters, dataPaging.thisPageId, Utils.GetMaxRows(), out nextPageId);

                // Check if we are matching 'all' or 'any'
                List<POCO.RecordAssociationKeyPhraseReverse> recassoc = new List<POCO.RecordAssociationKeyPhraseReverse>();
                if (matchType=="all")
                {
                    // Only select matching documents that meet all the criteria provided
                    var grouped = from c in recordAssociationEntities
                                  group c by c.RowKey into grp
                                  where grp.Count() >= oFilter.keyphrases.Count
                                  select grp.Key;
                    foreach(POCO.RecordAssociationKeyPhraseReverse ra in recordAssociationEntities)
                    {
                        if (grouped.Contains<string>(ra.RowKey))
                        {
                            recassoc.Add(ra);
                        }
                    }
                }
                else
                {
                    recassoc = recordAssociationEntities;
                }

                // Filter by file type (if any have been provided)
                if (oFilter.filetypes!=null && oFilter.filetypes.Count>0)
                {
                    for (int i= recassoc.Count-1;i>=0;i--)
                    {
                        // Loop through each of the file type filters
                        bool extensionMatch = false;
                        foreach (string s in oFilter.filetypes)
                        {
                            // Get the file extension for this item
                            if (recassoc[i].RowKey.EndsWith(s.ToLower()))
                            {
                                extensionMatch = true;
                            }
                        }

                        if (!extensionMatch)
                        {
                            // Remove the entry from the results
                            recassoc.RemoveAt(i);
                        }
                    }
                }

                recassoc.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recassoc, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Key Phrase Filter GET exception: " + ex.Message;
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
        class NamedEntity : TableEntity
        {
            public NamedEntity() { }

            public string OriginalText { get; set; }
            public string Type { get; set; }
        }

        [HttpGet("namedentityreversefilter", Name = "GetRecordAssociationByNamedEntityReverseFilter")]
        public IActionResult GetByNamedEntityReverseFilter([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By Named Entity Reverse Filter");

                // Deserialize the ontology filter
                RecordAssociationNamedEntityFilter oFilter = new RecordAssociationNamedEntityFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association keyphrase filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationNamedEntityFilter>(filter);
                }

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                string matchType = string.Empty;
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                    matchType = dataPaging.matchType;
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.namedentities.Count > 0)
                {
                    foreach (NamedEntityFilter neFilt in oFilter.namedentities)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(neFilt.OriginalText.Trim() + "|" + neFilt.Type.Trim());
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                        filters.Add(pkFilter);
                    }
                }

                List<POCO.RecordAssociationNamedEntityReverse> recordAssociationEntities = new List<POCO.RecordAssociationNamedEntityReverse>();

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                recordAssociationEntities = DataFactory.RecordAssociation.GetByReverseNamedEntity(dataConfig, filters);

                // Check if we are matching 'all' or 'any'
                List<POCO.RecordAssociationNamedEntityReverse> recassoc = new List<POCO.RecordAssociationNamedEntityReverse>();
                if (matchType == "all")
                {
                    // Only select matching documents that meet all the criteria provided
                    var grouped = from c in recordAssociationEntities
                                  group c by c.RowKey into grp
                                  where grp.Count() >= oFilter.namedentities.Count
                                  select grp.Key;
                    foreach (POCO.RecordAssociationNamedEntityReverse ra in recordAssociationEntities)
                    {
                        if (grouped.Contains<string>(ra.RowKey))
                        {
                            recassoc.Add(ra);
                        }
                    }
                }
                else
                {
                    recassoc = recordAssociationEntities;
                }

                // Filter by file type (if any have been provided)
                if (oFilter.filetypes != null && oFilter.filetypes.Count > 0)
                {
                    for (int i = recassoc.Count - 1; i >= 0; i--)
                    {
                        // Loop through each of the file type filters
                        bool extensionMatch = false;
                        foreach (string s in oFilter.filetypes)
                        {
                            // Get the file extension for this item
                            if (recassoc[i].RowKey.EndsWith(s.ToLower()))
                            {
                                extensionMatch = true;
                            }
                        }

                        if (!extensionMatch)
                        {
                            // Remove the entry from the results
                            recassoc.RemoveAt(i);
                        }
                    }
                }


                recassoc.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recassoc, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Key Phrase Filter GET exception: " + ex.Message;
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

        [HttpGet("ontologytermfilter", Name = "GetRecordAssociationByOntologyTermFilter")]
        public IActionResult GetByOntologyTermFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By Ontology Term Filter");

                // Deserialize the ontology filter
                RecordAssociationOntologyTermFilter oFilter = new RecordAssociationOntologyTermFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association ontology term  filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationOntologyTermFilter>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                //CloudTable table = Utils.GetCloudTable("stlprecordassociationnamedentityreverse", _logger);

                List<POCO.OntologyTermMatch> recordAssociationEntities = new List<POCO.OntologyTermMatch>();

                if (oFilter.ontologyterms.Count > 0)
                {
                    foreach (OntologyTermFilter termFilt in oFilter.ontologyterms)
                    {
                        // Create a new set of filters for each term
                        List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

                        string cleanFilterPKey = Utils.CleanTableKey(termFilt.OntologyUri.Trim());
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                        filters.Add(pkFilter);
                        string rkey1 = Utils.CleanTableKey(termFilt.OntologyTermUri.Trim());
                        //if (!rkey1.EndsWith("|")) { rkey1+="|"; }   // Make sure rowkey ends with pipe to match the Term|RecordAssoc format
                        DataFactory.Filter rkfilter1 = new DataFactory.Filter("TermRowKey", rkey1, "ge");
                        filters.Add(rkfilter1);
                        DataFactory.Filter rkfilter2 = new DataFactory.Filter("TermRowKey", Utils.GetLessThanFilter(rkey1), "lt");
                        filters.Add(rkfilter2);

                        List<POCO.OntologyTermMatch> recordAssocMatches = new List<POCO.OntologyTermMatch>();

                        DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                        recordAssocMatches = DataFactory.RecordAssociation.GetByOntologyTerm(dataConfig, filters);

                        recordAssociationEntities.AddRange(recordAssocMatches);
                    }
                }

                recordAssociationEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Ontology Term Filter GET exception: " + ex.Message;
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

        [HttpGet("ontologytermreversefilter", Name = "GetRecordAssociationByOntologyTermReverseFilter")]
        public IActionResult GetByOntologyTermReverseFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By Named Entity Reverse Filter");

                // Deserialize the ontology filter
                RecordAssociationOntologyTermFilter oFilter = new RecordAssociationOntologyTermFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association ontology term  filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationOntologyTermFilter>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                //CloudTable table = Utils.GetCloudTable("stlprecordassociationnamedentityreverse", _logger);

                List<POCO.RecordToRecordAssociationWithMatchInfo> recordAssociationEntities = new List<POCO.RecordToRecordAssociationWithMatchInfo>();

                if (oFilter.ontologyterms.Count > 0)
                {
                    foreach (OntologyTermFilter termFilt in oFilter.ontologyterms)
                    {
                        // Create a new set of filters for each term
                        List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

                        string cleanFilterPKey = Utils.CleanTableKey(termFilt.OntologyUri.Trim());
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                        filters.Add(pkFilter);
                        string rkey1 = Utils.CleanTableKey(termFilt.OntologyTermUri.Trim());
                        DataFactory.Filter rkfilter1 = new DataFactory.Filter("RowKey", rkey1, "ge");
                        filters.Add(rkfilter1);
                        DataFactory.Filter rkfilter2 = new DataFactory.Filter("RowKey", Utils.GetLessThanFilter(rkey1), "lt");
                        filters.Add(rkfilter2);

                        List<POCO.RecordToRecordAssociationWithMatchInfo> recordAssocMatches = new List<POCO.RecordToRecordAssociationWithMatchInfo>();

                        DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                        recordAssocMatches = DataFactory.RecordAssociation.GetByReverseOntologyTerm(dataConfig, filters);

                        recordAssociationEntities.AddRange(recordAssocMatches);
                    }
                }

                recordAssociationEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Ontology Term Filter GET exception: " + ex.Message;
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

        [HttpGet("comparerecordassociation", Name = "CompareRecordsAssociationsByRecordAssociationFilter")]
        public IActionResult CompareByRecordAssociationFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Compare RecordAssociations By Record Association Filter");

                // Deserialize the ontology filter
                RecordAssociationFilter oFilter = new RecordAssociationFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationFilter>(filter);

                    // Check that at least one record association has been returned
                    if (oFilter.recordassociations.Count < 1)
                    {
                        _logger.LogWarning("No Record Associations were found in the filter");
                        return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
                    }
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlprecordassociationkeyphrases", _logger);

                // Create a default query
                TableQuery<RecordAssociationKeyPhraseEntity> query = new TableQuery<RecordAssociationKeyPhraseEntity>();
                if (oFilter.recordassociations.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (RecordAssociation of in oFilter.recordassociations)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(of.recordassociationuri);
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
                    // Create final combined query
                    query = new TableQuery<RecordAssociationKeyPhraseEntity>().Where(combinedFilter);
                }
                List<RecordAssociationKeyPhraseEntity> recordAssociationEntities = new List<RecordAssociationKeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordAssociationKeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordAssociationEntities.Count;

                    Task<TableQuerySegment<RecordAssociationKeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordAssociationKeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordAssociationEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordAssociationEntities.Count < query.TakeCount.Value) && recordAssociationEntities.Count < 200);    //!ct.IsCancellationRequested &&


                // Look for matches between the key phrases
                int currentElement = 0;
                while(currentElement<=recordAssociationEntities.Count)
                {
                    string sourceNgrams = "";
                    string targetNgrams = "";


                }

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Compare Record Association By Record Filter GET exception: " + ex.Message;
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

        private List<POCO.RecordToRecordAssociation> GetRecordAssociationsFromDatabase(List<DataFactory.Filter> filters)
        {
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();

            DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
            List<POCO.RecordToRecordAssociation> recordAssocs = DataFactory.Record.GetRecordToRecordsAssociations(dataConfig, filters);

            return recordAssocs;
        }

        [HttpGet("recordfilter", Name = "GetRecordAssociationsByRecordFilter")]
        public IActionResult GetByRecordFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By Record Filter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing record association record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

                // Create a default query
                string cacheKey = string.Empty;
                if (oFilter.records.Count > 0)
                {
                    //string combinedFilter = "";
                    foreach (string of in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(of);
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                        filters.Add(pkFilter);

                        cacheKey += cleanFilterPKey;

                    }

                    Func<List<POCO.RecordToRecordAssociation>> dataGetter = () => GetRecordAssociationsFromDatabase(filters);

                    var recordAssocs = _cacheRecordAssociations.GetOrAdd(cacheKey, dataGetter);

                    entityAsJson = JsonConvert.SerializeObject(recordAssocs, Formatting.Indented);

                }


            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Association By Record Filter GET exception: " + ex.Message;
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

        [HttpGet("recordassociationfilter", Name = "GetRecordAssociationsByRecordAssociationFilter")]
        public IActionResult GetByRecordAssociationFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get RecordAssociations By RecordAssociation Filter");

                // Deserialize the ontology filter
                RecordAssociationFilter oFilter = new RecordAssociationFilter();
                if (filter != null && filter.Length > 0)
                {
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);

                    _logger.LogDebug("Deserializing record association filter of length: " + filterDecoded.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAssociationFilter>(filterDecoded);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlprecordassociations", _logger);

                // Create a default query
                TableQuery<RecordAssociationEntity> query = new TableQuery<RecordAssociationEntity>();
                string combinedFilter = "";
                if (oFilter.recordassociations.Count > 0)
                {
                    foreach (RecordAssociation raFilter in oFilter.recordassociations)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(raFilter.recordassociationuri);
                        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);
                        if (combinedFilter.Length == 0)
                        {
                            combinedFilter = pkquery;
                        }
                        else
                        {
                            combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.And, pkquery);
                        }
                    }

                    // Create final combined query
                    query = new TableQuery<RecordAssociationEntity>().Where(combinedFilter);
                }
                List<RecordAssociationEntity> recordAssociationEntities = new List<RecordAssociationEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordAssociationEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordAssociationEntities.Count;

                    Task<TableQuerySegment<RecordAssociationEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordAssociationEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordAssociationEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordAssociationEntities.Count < query.TakeCount.Value) && recordAssociationEntities.Count < 200);    //!ct.IsCancellationRequested &&

                entityAsJson = JsonConvert.SerializeObject(recordAssociationEntities, Formatting.Indented);


            }
            catch (Exception ex)

            {
                string exceptionMsg = "Record Association By Record Association Filter GET exception: " + ex.Message;
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


        private RecordAssociationEntity GetRecordAssociationEntity(string recordAssociationUri)
        {
            // Create the table if it doesn't exist. 
            //log.Info("Getting table reference");
            CloudTable table = Utils.GetCloudTable("stlprecordassociations", _logger);

            // Create a default query
            TableQuery<RecordAssociationEntity> query = new TableQuery<RecordAssociationEntity>();
            if (recordAssociationUri != null && recordAssociationUri != "")
            {
                    string cleanFilterPKey = Utils.CleanTableKey(recordAssociationUri);
                    string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);

                // Create final combined query
                query = new TableQuery<RecordAssociationEntity>().Where(pkquery);
            }
            List<RecordAssociationEntity> recordAssociationEntities = new List<RecordAssociationEntity>();
            TableContinuationToken token = null;

            var runningQuery = new TableQuery<RecordAssociationEntity>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - recordAssociationEntities.Count;

                Task<TableQuerySegment<RecordAssociationEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordAssociationEntity>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                recordAssociationEntities.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || recordAssociationEntities.Count < query.TakeCount.Value) && recordAssociationEntities.Count < 200);    //!ct.IsCancellationRequested &&

            if (recordAssociationEntities.Count>0)
            {
                return recordAssociationEntities[0];
            }
            else
            {
                return null;
            }
        }

        public class ForceRescanParam
        {
            public string systemuri { get; set; }
            public string itemuri { get; set; }
        }

        [HttpPost("forcerescan", Name = "ForceRescan")]
        public IActionResult ForceRescan([FromBody]ForceRescanParam items)
        {
            _logger.LogInformation("CPAPI: ForceRescan");

            bool isRescanOK = false;

            if (items==null)
            {
                // Throw error
                _logger.LogError("ForceRescan: no data provided");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            //ForceRescanParam items = JsonConvert.DeserializeObject<ForceRescanParam>(data);

            // Verify the POST info
            if (string.IsNullOrEmpty(items.systemuri))
            {
                // Throw error
                _logger.LogError("ForceRescan: System URI is null or empty");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }
            if (string.IsNullOrEmpty(items.itemuri))
            {
                // Throw error
                _logger.LogError("ForceRescan: Item URI is null or empty");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            string systemUri = System.Net.WebUtility.HtmlDecode(items.systemuri);
            systemUri = System.Net.WebUtility.UrlDecode(systemUri);
            string itemUri = System.Net.WebUtility.HtmlDecode(items.itemuri);
            itemUri = System.Net.WebUtility.UrlDecode(itemUri);

            // Load the system details
            List<DataFactory.Filter> systemFilter = new List<DataFactory.Filter>();
            DataFactory.Filter systempkfilt = new DataFactory.Filter("PartitionKey", Utilities.Converters.CleanTableKey(items.systemuri), "eq");
            systemFilter.Add(systempkfilt);

            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<POCO.System> systems = DataFactory.System.GetSystems(datacfg, systemFilter);
            if (systems.Count!=1)
            {
                // Throw error
                _logger.LogError("ForceRescan: GetSystems returned invalid number of systems=" + systems.Count.ToString());
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // URL decode the system and record
            //string systemFilter = System.Net.WebUtility.HtmlDecode(systemuri);
            //systemFilter = System.Net.WebUtility.UrlDecode(systemFilter);
            //string recordFilter = System.Net.WebUtility.HtmlDecode(recorduri);
            //recordFilter = System.Net.WebUtility.UrlDecode(recordFilter);
            //string itemFilter = System.Net.WebUtility.HtmlDecode(itemuri);
            //itemFilter = System.Net.WebUtility.UrlDecode(itemFilter);

            //// Load the Record Association details
            //DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            //List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            //DataFactory.Filter pkfilter = new DataFactory.Filter("PartitionKey", recorduri, "eq");
            //filters.Add(pkfilter);
            //DataFactory.Filter rkfilter = new DataFactory.Filter("RowKey", itemuri, "eq");
            //filters.Add(rkfilter);

            //List<POCO.RecordToRecordAssociation> recordAssociations = DataFactory.Record.GetRecordToRecordsAssociations(datacfg, filters);
            //// Make sure we only got one Record Association returned
            //if (recordAssociations.Count != 1)
            //{
            //    // Throw error
            //    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            //}

            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkfilter;

            switch (systems[0].Type)
            {
                case "sharepoint.2013":
                    {
                        // Set filter
                        filters = new List<DataFactory.Filter>();
                        pkfilter = new DataFactory.Filter("PartitionKey", itemUri, "eq");
                        filters.Add(pkfilter);

                        string nextPageId = string.Empty;
                        List<POCO.SharePoint.SPFile> spFiles = DataFactory.SharePoint.GetFiles(datacfg, filters, string.Empty, 1000, out nextPageId);
                        if (spFiles.Count != 1)
                        {
                            // Throw error
                            return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                        }

                        isRescanOK = DataFactory.SharePoint.ForceRescan(datacfg, spFiles[0]);
                        break;
                    }

                case "ntfs.share":
                    {
                        // Set filter
                        filters = new List<DataFactory.Filter>();
                        pkfilter = new DataFactory.Filter("PartitionKey", Utilities.Converters.CleanTableKey(itemUri), "eq");
                        filters.Add(pkfilter);

                        List<POCO.NTFSFile> ntfsFiles = DataFactory.NTFS.GetFiles(datacfg, filters);
                        if (ntfsFiles.Count == 0)
                        {
                            // Throw error
                            _logger.LogError("ForceRescan: no matching files found partitionkey=" + itemUri);
                            return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                        }

                        // Sort by most recent (in case there is more than one entry)
                        ntfsFiles = ntfsFiles.OrderByDescending(f => f.RowKey).ToList();
                        isRescanOK = DataFactory.NTFS.ForceRescan(datacfg, ntfsFiles[0]);
                        break;
                    }
                default:
                    _logger.LogError("ForceRescan: unsupported system type=" + systems[0].Type);
                    return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }


            if (isRescanOK)
            {
                _logger.LogInformation("ForceRescan: completed successfully");
                return StatusCode((int)System.Net.HttpStatusCode.NoContent);
            }
            else
            {
                _logger.LogError("ForceRescan: returned fail response");
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        // PUT: api/Ontology/5
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
    
    class RecordAssociationAllData
    {
        public string RecordUri { get; set; }
        public string ItemUri { get; set; }
        public string Name { get; set; }
        public DateTime Created { get; set; }
        public DateTime LastUpdated { get; set; }
        public string RecordAuthorityMatches { get; set; }
        public string DLM { get; set; }
        public string SecurityClassification { get; set; }
    }

    class RecordToRecordAssociationEntity:TableEntity
    {
        public RecordToRecordAssociationEntity() { }
        public DateTime Created { get; set; }
        public DateTime LastUpdated { get; set; }
    }

    class RecordAssociationEntity:TableEntity
    {
        public RecordAssociationEntity() { }
        public string RecordAuthorityMatches { get; set; }
        public string DLM { get; set; }
        public string SecurityClassification { get; set; }
    }

    class RecordAssociationKeyPhraseEntity : TableEntity
    {
        public RecordAssociationKeyPhraseEntity() { }

        [IgnoreProperty]
        public string ItemUri { get { return this.PartitionKey.Replace("|", "/"); } }
    }

    class RecordAssociationKeyPhraseReverseEntity : TableEntity
    {
        public RecordAssociationKeyPhraseReverseEntity() { }

        [IgnoreProperty]
        public string Keyphrase { get { return this.PartitionKey; } }
        [IgnoreProperty]
        public string ItemUri { get { return this.RowKey.Replace("|", "/"); } }
    }

    class RecordAssociationKeyPhraseFilter
    {
        public RecordAssociationKeyPhraseFilter()
        {
            keyphrases = new List<string>();
            systems = new List<string>();
            filetypes = new List<string>();
            records = new List<string>();
            recordassociations = new List<string>();
        }
        public List<string> filetypes;
        public List<string> keyphrases;
        public List<string> systems;
        public List<string> records;
        public List<string> recordassociations;
    }

    class RecordAssociationNamedEntityFilter
    {
        public RecordAssociationNamedEntityFilter()
        {
            namedentities = new List<NamedEntityFilter>();
            filetypes = new List<string>();
        }
        public List<NamedEntityFilter> namedentities;
        public List<string> filetypes;
    }

    class NamedEntityFilter
    {
        public string OriginalText { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Type { get; set; }
    }

    class RecordAssociationOntologyTermFilter
    {
        public RecordAssociationOntologyTermFilter()
        {
            ontologyterms = new List<OntologyTermFilter>();
        }
        public List<OntologyTermFilter> ontologyterms;
        public List<string> filetypes { get; set; }
    }

    class OntologyTermFilter
    {
        public string OntologyUri { get; set; }
        public string OntologyTermUri { get; set; }
    }

    class RecordFilter
    {
        public RecordFilter()
        {
            records = new List<string>();
        }
        public List<string> records;
    }

    class RecordAssociationFilter
    {
        public RecordAssociationFilter()
        {
            recordassociations = new List<RecordAssociation>();
        }
        public List<RecordAssociation> recordassociations { get; set; }
    }

    class RecordAssociation
    {
        public string recorduri { get; set; }
        public string recordassociationuri { get; set; }

    }
}
