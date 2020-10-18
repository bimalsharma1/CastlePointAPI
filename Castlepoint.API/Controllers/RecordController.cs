using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using System.Web;

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
    [Route("record", Name = "Record")]
    [Authorize]
    public class RecordController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<RecordController> _logger;

        public RecordController(IConfiguration config, ILogger<RecordController> logger)
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

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlprecords", _logger);

                // Create a default query
                TableQuery<RecordEntity> query = new TableQuery<RecordEntity>();

                List<RecordEntity> recordEntities = new List<RecordEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordEntities.Count;

                    Task<TableQuerySegment<RecordEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&


                recordEntities.Sort((x, y) => String.Compare(x.Label, y.Label));

                entityAsJson = JsonConvert.SerializeObject(recordEntities, Formatting.Indented);

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

        [HttpGet("graph/force", Name ="GetRecordAssocationGraphNodes")]
        public IActionResult GetRecordAssociationGraphNodes()
        {
            string entityAsJson = "";

            // DEMO
            entityAsJson = "[{ id: 1, name: 'my node 1' }]";

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }

        // GET: Function Summary
        [HttpGet("recordauthority/matchsummary", Name = "GetRecordAuthorityMatchSummary")]
        public IActionResult GetRecordAuthorityMatchSummary([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetRecordAuthorityMatchSummary");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create a Record for filtering
                POCO.Record record = new POCO.Record(oFilter.records[0].systemuri, oFilter.records[0].recorduri);

                // Get the data config to use
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();

                // Get keyphrase matches for the Record
                List<POCO.KeyPhraseToRecordLookup> keyphrases = DataFactory.Record.GetRecordKeyPhrases(dataConfig, record);

                // Get named entity matches for the Record
                List<POCO.NamedEntityToRecordLookup> namedents = DataFactory.Record.GetRecordNamedEntitys(dataConfig, record);

                // Combine results
                List<string> functions = new List<string>();
                List<POCO.RecordAuthorityMatchResult> matchResults = new List<POCO.RecordAuthorityMatchResult>();

                foreach(POCO.KeyPhraseToRecordLookup kp in keyphrases)
                {
                    if (kp.RAFunction!=null && kp.RAFunction!=string.Empty)
                    {
                        // Check if an entry exists for this function
                        if (matchResults.Exists(f => f.Function==kp.RAFunction))
                        {
                            // Increment the number of matches
                            matchResults.Find(f => f.Function == kp.RAFunction).NumMatches++;
                        }
                        else

                        {
                            // Create a new match result object and add
                            POCO.RecordAuthorityMatchResult ramatchnew = new POCO.RecordAuthorityMatchResult();
                            ramatchnew.Activity = kp.RAActivity;
                            ramatchnew.ClassNo = kp.RAClass;
                            ramatchnew.Function = kp.RAFunction;
                            ramatchnew.NumMatches = 1;
                            ramatchnew.RASchemaUri = kp.RASchemaUri;

                            matchResults.Add(ramatchnew);

                        }
                    }
                }

                foreach (POCO.NamedEntityToRecordLookup ne in namedents)
                {
                    if (ne.RAFunction != null && ne.RAFunction != string.Empty)
                    {
                        // Check if an entry exists for this function
                        if (matchResults.Exists(f => f.Function == ne.RAFunction))
                        {
                            // Increment the number of matches
                            matchResults.Find(f => f.Function == ne.RAFunction).NumMatches++;
                        }
                        else

                        {
                            // Create a new match result object and add
                            POCO.RecordAuthorityMatchResult ramatchnew = new POCO.RecordAuthorityMatchResult();
                            ramatchnew.Activity = ne.RAActivity;
                            ramatchnew.ClassNo = ne.RAClass;
                            ramatchnew.Function = ne.RAFunction;
                            ramatchnew.NumMatches = 1;
                            ramatchnew.RASchemaUri = ne.RASchemaUri;

                            matchResults.Add(ramatchnew);

                        }
                    }
                }


                // Sort by function count descend
                matchResults = matchResults.OrderByDescending(x => x.NumMatches).ToList();

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(matchResults, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record GET Record Functions exception: " + ex.Message;
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


        // GET: Function Summary
        [HttpGet("function/summary", Name = "GetFunctionSummary")]
        public IActionResult GetFunctionSummary()
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.Record> records = DataFactory.Record.GetRecords(dataConfig, filters);

                // Sort by function name
                records.Sort((x, y) => String.Compare(x.Function, y.Function));

                // Count the functions in the records
                List<FunctionCount> funcCount = new List<FunctionCount>();
                foreach(POCO.Record ent in records)
                {
                    if (ent==null || ent.Function==null || ent.Function=="")
                    {
                        // Skip this entry
                        continue;
                    }
                    // Try and find by the function name
                    FunctionCount fCount = funcCount.Find(x => x.FunctionName == ent.Function);
                    if (fCount!=null)
                    {
                        // Increment the function count
                        fCount.Count++;
                    }
                    else
                    {
                        // Add the function count entry
                        fCount = new FunctionCount();
                        fCount.FunctionName = ent.Function;
                        fCount.Count = 1;
                        funcCount.Add(fCount);
                    }

                }

                entityAsJson = JsonConvert.SerializeObject(funcCount, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record GET Record Functions exception: " + ex.Message;
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

        // GET: Class Summary
        [HttpGet("class/summary", Name = "GetClassSummary")]
        public IActionResult GetClassSummary()
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetClassSummary");

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.Record> records = DataFactory.Record.GetRecords(dataConfig, filters);

                // Sort by function name
                records.Sort((x, y) => String.Compare(x.ClassNo, y.ClassNo));

                // Count the functions in the records
                List<ClassCount> classCount = new List<ClassCount>();
                foreach (POCO.Record ent in records)
                {
                    if (ent == null || ent.ClassNo == null || ent.ClassNo == "" || ent.ClassNo == "0")
                    {
                        // Skip this entry
                        continue;
                    }
                    // Try and find by the function name
                    ClassCount cCount = classCount.Find(x => x.ClassNo == ent.ClassNo);
                    if (cCount != null)
                    {
                        // Increment the function count
                        cCount.Count++;
                    }
                    else
                    {
                        // Add the function count entry
                        cCount = new ClassCount();
                        cCount.Function = ent.Function;
                        cCount.ClassNo = ent.ClassNo;
                        cCount.Count = 1;
                        classCount.Add(cCount);
                    }

                }

                entityAsJson = JsonConvert.SerializeObject(classCount, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record GET Record Classes exception: " + ex.Message;
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

        class FunctionCount
        {
            public FunctionCount()
            {
                this.FunctionName = "";
                this.Count = 0;
            }
            public string SchemaUri { get { return "http://www.naa.gov.au/afda"; } }
            public string SchemaLabel { get { return "AFDA Express"; } }
            public string FunctionName { get; set; }
            public int Count { get; set; }
        }

        class ClassCount
        {
            public ClassCount()
            {
                this.Function = string.Empty;
                this.ClassNo = string.Empty;
                this.Count = 0;
            }
            public string SchemaUri { get { return "http://www.naa.gov.au/afda"; } }
            public string SchemaLabel { get { return "AFDA Express"; } }
            public string Function { get; set; }
            public string ClassNo { get; set; }
            public int Count { get; set; }
        }

        [HttpGet("filter", Name = "GetByRecordFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter !=null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                //CloudTable table = Utils.GetCloudTable("stlprecords", _logger);
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                // Create a default query
                //TableQuery<RecordEntity> query = new TableQuery<RecordEntity>();
                if (oFilter.records.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.systemuri);
                        string cleanFilterRKey = Utils.CleanTableKey(rif.itemuri);

                        if (cleanFilterPKey!="" || cleanFilterRKey!="" || rif.function!= "" || rif.classno != "")
                        {
                            // Check if there is a system key
                            if (cleanFilterPKey != "")
                            {
                                // Make sure system URIs end with a pipe (|)
                                if (!cleanFilterPKey.EndsWith("|"))
                                {
                                    cleanFilterPKey += "|";
                                }

                                DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                                filters.Add(pkFilter);
                            }

                            // Check if an item key has been provided
                            if (cleanFilterRKey != "")
                            {
                                DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", cleanFilterRKey, "eq");
                                filters.Add(rkFilter);

                            }

                            // Check if a function has been provided
                            if (rif.function != null && rif.function != "")
                            {
                                DataFactory.Filter functionFilter = new DataFactory.Filter("Function", rif.function, "eq");
                                filters.Add(functionFilter);
                            }
                            // Check if a class no has been provided
                            if (rif.classno != null && rif.classno != "")
                            {
                                DataFactory.Filter classNoFilter = new DataFactory.Filter("ClassNo", rif.classno, "eq");
                                filters.Add(classNoFilter);
                            }
                        }
                    }

                    // Create final combined query
                    _logger.LogInformation("RecordFilter query: " + combinedFilter);
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }

                // Get the data
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.Record> recordEntities = DataFactory.Record.GetRecords(dataConfig, filters);

                recordEntities.Sort((x, y) => String.Compare(x.Label, y.Label));

                entityAsJson = JsonConvert.SerializeObject(recordEntities, Formatting.Indented);

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

        [HttpGet("namedentitys/filter", Name = "GetRecordNamedEntitysByRecordFilter")]
        public IActionResult GetRecordNamedEntitysByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetRecordNamedEntitysByFilter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create a filter to pass to the data factory
                string systemUri = oFilter.records[0].systemuri;
                if (!systemUri.EndsWith("|")) { systemUri += "/"; }
                string itemUri = oFilter.records[0].itemuri;
                POCO.Record recordFilter = new POCO.Record(systemUri, itemUri);

                // Call the data factory
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.NamedEntityToRecordLookup> recordNamedEntities = DataFactory.Record.GetRecordNamedEntitys(dataConfig, recordFilter);

                // Select distinct only
                recordNamedEntities = recordNamedEntities
                    .GroupBy(m => new { m.PartitionKey, m.RowKey, m.RAClass })
                    .Select(group => group.First())  // instead of First you can also apply your logic here what you want to take, for example an OrderBy
                    .ToList();


                // Get the Record Authority data to overlay
                List<POCO.RecordAuthorityFunctionActivityEntry> raitems = DataFactory.RecordAuthority.GetEntries(dataConfig, new List<POCO.RecordAuthorityFilter>());
                foreach (POCO.NamedEntityToRecordLookup nerecordlookup in recordNamedEntities)
                {
                    int foundIndex = raitems.FindIndex(ralookup => ralookup.Function == nerecordlookup.RAFunction && ralookup.EntryNo.ToString() == nerecordlookup.RAClass);
                    if (foundIndex > -1)
                    {
                        nerecordlookup.RARetention = raitems[foundIndex].Retention;
                        nerecordlookup.RATrigger = raitems[foundIndex].Trigger;
                        nerecordlookup.RARetainPermanent = raitems[foundIndex].RetainPermanent;
                    }
                }

                // Sort by retention descending
                //recordNamedEntities.Sort((x, y) => { return y.RARetention.CompareTo(x.RARetention); });
                recordNamedEntities = recordNamedEntities.OrderByDescending(x => x.RARetention).ThenBy(y => y.RAClass).ToList();

                entityAsJson = JsonConvert.SerializeObject(recordNamedEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Named Entity GET exception: " + ex.Message;
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


        [HttpGet("keyphrases/filter", Name = "GetRecordKeyPhrasesByRecordFilter")]
        public IActionResult GetRecordKeyPhrasesByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetRecordKeyPhrasesByFilter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create a filter to pass to the data factory
                string systemUri = oFilter.records[0].systemuri;
                if (!systemUri.EndsWith("|")) { systemUri += "/"; }
                string recorduri = oFilter.records[0].recorduri;
                string itemUri = oFilter.records[0].itemuri;

                // Validate the filter data
                if (recorduri==null || recorduri==string.Empty)
                {
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(recorduri), "eq");
                filters.Add(pkfilt);

                // Check if a record association (itemuri) has been supplied
                if (itemUri!=null && itemUri!=string.Empty)
                {
                    DataFactory.Filter rkfilt1 = new DataFactory.Filter("RowKey", Utils.CleanTableKey(itemUri), "ge");
                    filters.Add(rkfilt1);
                    DataFactory.Filter rkfilt2 = new DataFactory.Filter("RowKey", Utils.GetLessThanFilter(Utils.CleanTableKey(itemUri)), "lt");
                    filters.Add(rkfilt2);
                }

                // Call the data factory
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.KeyPhraseToRecordLookup> recordKeyPhrases = DataFactory.Record.GetRecordKeyPhrases(dataConfig, filters);

                // Select distinct only
                recordKeyPhrases = recordKeyPhrases
                    .GroupBy(m => new { m.PartitionKey, m.RowKey, m.RAClass })
                    .Select(group => group.First())  // instead of First you can also apply your logic here what you want to take, for example an OrderBy
                    .ToList();

                // Get the Record Authority data to overlay
                List<POCO.RecordAuthorityFunctionActivityEntry> raitems = DataFactory.RecordAuthority.GetEntries(dataConfig, new List<POCO.RecordAuthorityFilter>());
                foreach(POCO.KeyPhraseToRecordLookup kprecordlookup in recordKeyPhrases)
                {
                    int foundIndex = raitems.FindIndex(ralookup => ralookup.Function == kprecordlookup.RAFunction && ralookup.EntryNo.ToString() == kprecordlookup.RAClass);
                    if (foundIndex>-1)
                    {
                        kprecordlookup.RARetention = raitems[foundIndex].Retention;
                        kprecordlookup.RATrigger = raitems[foundIndex].Trigger;
                        kprecordlookup.RARetainPermanent = raitems[foundIndex].RetainPermanent;
                    }
                }

                // Sort by retention + classno descending
                //recordKeyPhrases.Sort((x, y) => { return y.RARetention.CompareTo(x.RARetention); });
                recordKeyPhrases = recordKeyPhrases.OrderByDescending(x => x.RARetention).ThenBy(y => y.RAClass).ToList();

                //var sorted = from element in raitems
                //             orderby element.Retention descending, element.Function ascending
                //              select element;

                entityAsJson = JsonConvert.SerializeObject(recordKeyPhrases, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record KeyPhrases GET exception: " + ex.Message;
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

        [HttpGet("sentence/filter", Name = "GetSentenceHistoryByRecordFilter")]
        public IActionResult GetSentenceHistoryByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create the filter
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.records.Count > 0)
                {
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.systemuri);

                        if (cleanFilterPKey != "")
                        {

                            DataFactory.Filter pkFilt = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                            filters.Add(pkFilt);
                        }
                        string cleanFilterRKey = Utils.CleanTableKey(rif.itemuri);

                        if (cleanFilterRKey != "")
                        {

                            DataFactory.Filter rkFilt1 = new DataFactory.Filter("RowKey", cleanFilterRKey, "ge");
                            filters.Add(rkFilt1);
                            DataFactory.Filter rkFilt2 = new DataFactory.Filter("RowKey", Utils.GetLessThanFilter(cleanFilterRKey), "lt");
                            filters.Add(rkFilt2);
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.RecordSentenceHistory> sentenceHistory = DataFactory.SentenceHistory.GetForRecord(dataConfig, filters);

                // Sort sentence history entities in descending date order (most recent first)
                sentenceHistory.Sort((x, y) => DateTime.Compare(y.LastCategorised, x.LastCategorised));

                entityAsJson = JsonConvert.SerializeObject(sentenceHistory, Formatting.Indented);

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

        [HttpGet("processing/filter", Name = "GetProcessingHistoryByRecordFilter")]
        public IActionResult GetProcessingHistoryByRecordFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetProcessingHistoryByRecordFilter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create the filter
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.records.Count > 0)
                {
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterKey = System.Web.HttpUtility.UrlDecode(rif.recorduri);
                        cleanFilterKey = Utils.CleanTableKey(cleanFilterKey);

                        if (cleanFilterKey != "")
                        {

                            DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", cleanFilterKey, "eq");
                            filters.Add(pkfilt);
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.RecordProcessingHistory> processingHistory = DataFactory.ProcessingHistory.GetForRecord(dataConfig, filters);

                // Sort sentence history entities in descending date order (most recent first)
                processingHistory.Sort((x, y) => String.Compare(y.RowKey, x.RowKey));

                entityAsJson = JsonConvert.SerializeObject(processingHistory, Formatting.Indented);

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


        [HttpGet("keyphrase", Name = "GetKeyPhraseByFilter")]
        public IActionResult GetKeyPhraseByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetKeyPhraseByFilter");

                // Deserialize the ontology filter
                RecordKeyPhraseFilter oFilter = new RecordKeyPhraseFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordKeyPhraseFilter>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlprecordkeyphrases", _logger);

                // Create a default query
                TableQuery<RecordKeyPhraseEntity> query = new TableQuery<RecordKeyPhraseEntity>();
                if (oFilter.records.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (RecordKeyPhraseItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.recorduri);
                        string cleanFilterRKey = Utils.CleanTableKey(rif.schemauri);

                        if (cleanFilterPKey!="" || cleanFilterRKey!="")
                        {
                            string pkQuery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, cleanFilterPKey);
                            //string pkqueryEnd = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterPKey));

                            //string pkqueryCombined = TableQuery.CombineFilters(pkqueryStart, TableOperators.And, pkqueryEnd);

                            if (combinedFilter != "")
                            {
                                combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.And, pkQuery);
                            }
                            else
                            {
                                combinedFilter = pkQuery;
                            }

                            // Check if an item key has been provided
                            if (cleanFilterRKey != "")
                            {
                                string rkQuery = TableQuery.GenerateFilterCondition("RASchemaUri", QueryComparisons.Equal, cleanFilterRKey);
                                //string rkqueryEnd = TableQuery.GenerateFilterCondition("RASchemaUri", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterRKey));

                                //string rkqueryCombined = TableQuery.CombineFilters(rkqueryStart, TableOperators.And, rkqueryEnd);

                                if (combinedFilter != "")
                                {
                                    combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.And, rkQuery);
                                }
                                else
                                {
                                    combinedFilter = rkQuery;
                                }
                            }
                        }
                    }

                    // Create final combined query
                    _logger.LogInformation("RecordKeyPhraseFilter query: " + combinedFilter);
                    query = new TableQuery<RecordKeyPhraseEntity>().Where(combinedFilter);
                }
                else
                {
                    _logger.LogInformation("RecordKeyPhraseFilter query BLANK");
                }
                List<RecordKeyPhraseEntity> recordEntities = new List<RecordKeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordKeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordEntities.Count;

                    Task<TableQuerySegment<RecordKeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordKeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordEntities.Count < query.TakeCount.Value) && recordEntities.Count < 100);    //!ct.IsCancellationRequested &&


                recordEntities.Sort((x, y) => String.Compare(x.KeyPhrase, y.KeyPhrase));

                entityAsJson = JsonConvert.SerializeObject(recordEntities, Formatting.Indented);

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

        class SentenceControlRecordEntity : TableEntity
        {
            //{"RecordUri":"http://1.2.3.com/abc","Action":"Review","Comments":"test sentence control record","DestructionMethod":"","AuthorisingActionOfficer":"Gavin McKay"}
            public string RecordUri { get; set; }
            public DateTime ActionDate { get; set; }
            public string Action { get; set; }
            public string Comments { get; set; }
            public string DestructionMethod { get; set; }
            public string AuthorisingActionOfficer { get; set; }
    }

        [HttpGet("graph/ontology", Name = "GraphRecordOntology")]
        public IActionResult GraphRecordOntology([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get Record Ontology Graph Data by Record Filter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }


                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                //CloudTable table = Utils.GetCloudTable("stlprecordkeyphrases", _logger);

                // Create a default query
                //TableQuery<RecordKeyPhraseEntity> query = new TableQuery<RecordKeyPhraseEntity>();

                List<POCO.KeyPhraseToRecordLookup> recordKeyPhraseEntities = new List<POCO.KeyPhraseToRecordLookup>();
                List<POCO.NamedEntityToRecordLookup> recordNamedEntityEntities = new List<POCO.NamedEntityToRecordLookup>();

                DataFactory.DataConfig datacfg = Utils.GetDataConfig();

                if (oFilter.records.Count > 0)
                {
                    POCO.Record record = new POCO.Record();

                    //TODO: iterate through all record filters
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.itemuri);

                        if (cleanFilterPKey != "")
                        {
                            record.RowKey = cleanFilterPKey;
                            List<POCO.KeyPhraseToRecordLookup> kpEntities = DataFactory.Record.GetRecordKeyPhrases(datacfg, record);
                            if (kpEntities.Count>0)
                            {
                                recordKeyPhraseEntities.AddRange(kpEntities);
                            }
                            List<POCO.NamedEntityToRecordLookup> neEntities = DataFactory.Record.GetRecordNamedEntitys(datacfg, record);
                            if (neEntities.Count > 0)
                            {
                                recordNamedEntityEntities.AddRange(neEntities);
                            }
                        }
                    }

                    // Create final combined query
                    //_logger.LogInformation("RecordFilter query: " + combinedFilter);
                    //query = new TableQuery<RecordKeyPhraseEntity>().Where(combinedFilter);
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }

                //TableContinuationToken token = null;

                //var runningQuery = new TableQuery<RecordKeyPhraseEntity>()
                //{
                //    FilterString = query.FilterString,
                //    SelectColumns = query.SelectColumns
                //};

                //do
                //{
                //    runningQuery.TakeCount = query.TakeCount - recordKeyPhraseEntities.Count;

                //    Task<TableQuerySegment<RecordKeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordKeyPhraseEntity>(runningQuery, token);
                //    tSeg.Wait();
                //    token = tSeg.Result.ContinuationToken;
                //    recordKeyPhraseEntities.AddRange(tSeg.Result);

                //} while (token != null && (query.TakeCount == null || recordKeyPhraseEntities.Count < query.TakeCount.Value) && recordKeyPhraseEntities.Count < 100);    //!ct.IsCancellationRequested &&


                //recordKeyPhraseEntities.Sort((x, y) => String.Compare(y.RowKey, x.RowKey));

                // Find all the unique ontologies
                Dictionary<string, int> countOntologies = new Dictionary<string, int>();
                Dictionary<string, string> ontologyToFunction = new Dictionary<string, string>();
                foreach (POCO.KeyPhraseToRecordLookup kp in recordKeyPhraseEntities)
                {
                    // Get the ontology schema
                    string schemaUri = kp.RASchemaUri;
                    // Check if the ontology schema has been seen before
                    if (countOntologies.ContainsKey(schemaUri))
                    {
                        countOntologies[schemaUri]++;
                    }
                    else
                    {
                        countOntologies.Add(schemaUri, 1);
                    }
                    // Check if the ontology and function has been seen before
                    string function = kp.RAFunction;
                    string functionKey = schemaUri + "_" + function;
                    if (!ontologyToFunction.ContainsKey(functionKey))
                    {
                        ontologyToFunction.Add(functionKey, function);
                    }
                }

                foreach (POCO.NamedEntityToRecordLookup kp in recordNamedEntityEntities)
                {
                    // Get the ontology schema
                    string schemaUri = kp.RASchemaUri;
                    // Check if the ontology schema has been seen before
                    if (countOntologies.ContainsKey(schemaUri))
                    {
                        countOntologies[schemaUri]++;
                    }
                    else
                    {
                        countOntologies.Add(schemaUri, 1);
                    }
                    // Check if the ontology and function has been seen before
                    string function = kp.RAFunction;
                    string functionKey = schemaUri + "_" + function;
                    if (!ontologyToFunction.ContainsKey(functionKey))
                    {
                        ontologyToFunction.Add(functionKey, function);
                    }
                }

                // Create the nodes and links
                string nodesJson = "{\"id\": 1, \"name\": \"Ontology\", \"_size\": 40, \"_color\": \"green\"}";
                string linksJson = "";
                int indexNode = 2;
                int indexLink = 1;
                foreach (string schema in countOntologies.Keys)
                {

                    // Add the ontology node and link to the root not
                    nodesJson += ",{\"id\": " + indexNode + ", \"name\": \"" + schema + "\", \"_size\": 40, \"_color\": \"blue\"}";

                    // Prepend with , if not the first item
                    if (indexLink > 1) { linksJson += ","; }
                    linksJson += "{ \"sid\": 1, \"tid\": " + indexNode + ", \"_color\": \"red\" }";

                    // Store this ontology node id as the parent
                    int parentNode = indexNode;

                    indexNode++;
                    indexLink++;

                    // Find and add any matching function nodes and links
                    foreach (string functionKey in ontologyToFunction.Keys)
                    {
                        // Check if this function is a match for the ontology schema
                        if (functionKey.StartsWith(schema))
                        {
                            // Found a matching function - add node and link
                            nodesJson += ",{\"id\": " + indexNode + ", \"name\": \"" + ontologyToFunction[functionKey] + "\", \"_size\": 40, \"_color\": \"orange\"}";

                            // Prepend with , if not the first item
                            if (indexLink > 1) { linksJson += ","; }
                            // Add a link from the ontology node to the function node
                            linksJson += "{ \"sid\": " + parentNode + ", \"tid\": " + indexNode + ", \"_color\": \"black\" }";

                            indexNode++;
                            indexLink++;
                        }
                    }

                }

                // Join the nodes and links data together
                string finalJson = "{\"nodes\": [" + nodesJson + "], \"links\": [" + linksJson + "]}";

                entityAsJson = finalJson;

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

        [HttpGet("sentencecontrolrecord", Name = "GetSentenceControlRecordByRecordFilter")]
        public IActionResult GetSentenceControlRecordByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get Sentence Control Record by Record Filter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Create the table if it doesn't exist. 
                //log.Info("Getting table reference");
                CloudTable table = Utils.GetCloudTable("stlpsentencecontrolrecord", _logger);

                // Create a default query
                TableQuery<SentenceControlRecordEntity> query = new TableQuery<SentenceControlRecordEntity>();
                if (oFilter.records.Count > 0)
                {
                    string combinedFilter = "";
                    //TODO: iterate through all record filters
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.itemuri);

                        if (cleanFilterPKey != "")
                        {
                            string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);
                            combinedFilter = pkquery;
                        }
                    }

                    // Create final combined query
                    _logger.LogInformation("RecordFilter query: " + combinedFilter);
                    query = new TableQuery<SentenceControlRecordEntity>().Where(combinedFilter);
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }
                List<SentenceControlRecordEntity> recordEntities = new List<SentenceControlRecordEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<SentenceControlRecordEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - recordEntities.Count;

                    Task<TableQuerySegment<SentenceControlRecordEntity>> tSeg = table.ExecuteQuerySegmentedAsync<SentenceControlRecordEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    recordEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || recordEntities.Count < query.TakeCount.Value) && recordEntities.Count < 100);    //!ct.IsCancellationRequested &&


                recordEntities.Sort((x, y) => String.Compare(y.RowKey, x.RowKey));

                entityAsJson = JsonConvert.SerializeObject(recordEntities, Formatting.Indented);

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

        [HttpPost("sentencecontrolrecord", Name ="AddSentenceControlRecord")]
        public IActionResult AddSentenceControlRecord(string sentencecontrolrecord)
        {
            _logger.LogInformation("CPAPI: Add Sentence Control Record");

            bool isAddedOK = false;

            // Deserialize the record configuration
            SentenceControlRecordEntity sentenceControlRecord = new SentenceControlRecordEntity();
            if (sentencecontrolrecord != null && sentencecontrolrecord.Length > 0)
            {
                _logger.LogDebug("Deserializing sentence control record of length: " + sentencecontrolrecord.Length);
                // HTTP decode the submitted data
                string scrDecoded = System.Net.WebUtility.HtmlDecode(sentencecontrolrecord);
                scrDecoded = System.Net.WebUtility.UrlDecode(scrDecoded);
                sentenceControlRecord = JsonConvert.DeserializeObject<SentenceControlRecordEntity>(scrDecoded);
            }
            else
            {
                _logger.LogDebug("Method called without sentence control record information");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Set automated values
            Guid scrId = Guid.NewGuid();
            sentenceControlRecord.ActionDate = DateTime.UtcNow;

            // Clean the table and row keys
            //string recordUri = 
            sentenceControlRecord.PartitionKey = Utils.CleanTableKey(sentenceControlRecord.RecordUri);
            sentenceControlRecord.RowKey = Utils.CleanTableKey(scrId.ToString());

            CloudTable table = Utils.GetCloudTable("stlpsentencecontrolrecord", _logger);

            // Create the TableOperation that inserts or merges the entry. 
            //log.Verbose("Creating table operation");
            TableOperation insertReplaceOperation = TableOperation.Insert(sentenceControlRecord);

            // Execute the insert operation. 
            //log.Verbose("Executing table operation");
            try
            {
                Task tResult = table.ExecuteAsync(insertReplaceOperation);
                tResult.Wait();
                isAddedOK = true;
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



            if (isAddedOK)
            {
                // Return the updated entity
                ObjectResult result = new ObjectResult(sentenceControlRecord);
                return result;
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        public class ForceChangeFlagBody
        {
            public string systemuri;
            public string recorduri;
        }

        [HttpPost("forcechangeflag", Name = "ForceChangeFlag")]
        public IActionResult ForceChangeFlag([FromBody]ForceChangeFlagBody forceChangeFlagBody)
        {
            _logger.LogInformation("CPAPI: ForceChangeFlag");

            // Verify the POST info
            if (forceChangeFlagBody==null)
            {
                // Throw error
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            //string bodyDecoded= System.Net.WebUtility.HtmlDecode(forceChangeFlagBody);
            //ForceChangeFlagBody flagbody = JsonConvert.DeserializeObject<ForceChangeFlagBody>(bodyDecoded);

            // Load the Record details
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkfilter = new DataFactory.Filter("PartitionKey", forceChangeFlagBody.systemuri, "eq");
            filters.Add(pkfilter);
            DataFactory.Filter rkfilter = new DataFactory.Filter("RowKey", forceChangeFlagBody.recorduri, "eq");
            filters.Add(rkfilter);

            List<POCO.Record> records = DataFactory.Record.GetRecords(datacfg, filters);

            // Make sure we only got one Record returned
            if (records.Count!=1)
            {
                // Throw error
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            bool isMatchStatusCleared = DataFactory.Record.ResetMatchStatus(datacfg, records[0]);

            DataFactory.Record.DeleteRecordAuthorityMatches(datacfg, records[0]);

            bool isResetOK = DataFactory.Record.ForceChangeFlag(datacfg, records[0]);

            if (isResetOK)
            {
                return StatusCode((int)System.Net.HttpStatusCode.NoContent);
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        [HttpPost]
        public IActionResult Configure(string recordconfig)
        {
            _logger.LogInformation("CPAPI: Configure");

            // Deserialize the record configuration
            RecordConfigEntity recordConfigEntity = new RecordConfigEntity();
            if (recordconfig != null && recordconfig.Length > 0)
            {
                _logger.LogDebug("Deserializing config of length: " + recordconfig.Length);
                recordConfigEntity = JsonConvert.DeserializeObject<RecordConfigEntity>(recordconfig);
            }

            // Clean the table and row keys
            recordConfigEntity.PartitionKey = Utils.CleanTableKey(recordConfigEntity.PartitionKey);
            recordConfigEntity.RowKey = Utils.CleanTableKey(recordConfigEntity.RowKey);

            bool isUpdatedOK = UpdateRecordConfiguration(recordConfigEntity);

            if (isUpdatedOK)
            {
                // Return the updated entity
                ObjectResult result = new ObjectResult(recordConfigEntity);
                return result;
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        private bool UpdateRecordConfiguration(RecordConfigEntity recordConfigEntity)
        {

            bool isRecordSentenceUpdated = false;

            CloudTable table = Utils.GetCloudTable("stlprecords", _logger);

            //log.Info("Creating record entity");

            // Create the TableOperation that inserts or merges the entry. 
            //log.Verbose("Creating table operation");
            TableOperation insertReplaceOperation = TableOperation.InsertOrReplace(recordConfigEntity);

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

            isRecordSentenceUpdated = true;

            return isRecordSentenceUpdated;
        }

        

        internal class RecordKeyPhraseEntity : TableEntity
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
            public string RAFunction { get; set; }
            public string RAClass { get; set; }

        }

        class RecordEntity : TableEntity
        {
            public RecordEntity(string sourceUri, string itemUri)
            {
                this.PartitionKey = sourceUri;
                this.RowKey = itemUri;

            }

            public RecordEntity()
            {

            }

            //public string Properties { get; set; } 
            [IgnoreProperty]
            public string SourceUri { get { return this.PartitionKey; } }
            [IgnoreProperty]
            public string ItemUri { get { return this.RowKey; } }
            [IgnoreProperty]
            public string ItemUrl { get { return this.RowKey.Replace("|", "/"); } }
            [IgnoreProperty]
            public string WebUrl { get { return this.PartitionKey.Replace("|", "/"); } }
            public string UniqueId { get; set; }
            public string CPId { get; set; }
            public string Function { get; set; }
            public string ClassNo { get; set; }
            public string Activity { get; set; }
            public string Label { get; set; }
            public string Type { get; set; }
            public DateTime Created { get; set; }
            public DateTime LastUpdated { get; set; }
            public DateTime SentenceDate { get; set; }
            public DateTime NextSentenceDate { get; set; }
            public DateTime LastCategorised { get; set; }
            public string Stats { get; set; }
        }

        class RecordSentenceHistoryEntity : TableEntity
        {
            public RecordSentenceHistoryEntity() { }

            [IgnoreProperty]
            public string ItemUri { get { return this.PartitionKey; } }
            [IgnoreProperty]
            public string HistoryDate { get { return this.RowKey; } }
            public string ItemUrl { get { return this.PartitionKey.Replace("|", "/"); } }
            [IgnoreProperty]
            public Guid UniqueId { get; set; }
            public string Function { get; set; }
            public string ClassNo { get; set; }
            public string Activity { get; set; }
            public string Label { get; set; }
            public string Type { get; set; }
            public DateTime Created { get; set; }
            public DateTime LastUpdated { get; set; }
            public DateTime SentenceDate { get; set; }
            public DateTime NextSentenceDate { get; set; }
            public DateTime LastCategorised { get; set; }
        }

        class RecordFilter
        {
            public RecordFilter()
            {
                records = new List<RecordItemFilter>();
            }
            public List<RecordItemFilter> records;
        }

        class RecordItemFilter
        {
            public string systemuri { get; set; }
            public string recorduri { get; set; }
            public string itemuri { get; set; }
            public string function { get; set; }
            public string classno { get; set; }
        }

        class RecordKeyPhraseFilter
        {
            public RecordKeyPhraseFilter()
            {
                records = new List<RecordKeyPhraseItemFilter>();
            }
            public List<RecordKeyPhraseItemFilter> records;
        }

        class RecordKeyPhraseItemFilter
        {
            public string recorduri { get; set; }
            public string schemauri { get; set; }
        }

    }

    internal class RecordConfigEntity:TableEntity
    {
        public RecordConfigEntity()
        {
            this.DoNotSentence = false;
        }
        public bool DoNotProcess { get; set; }
        public bool DoNotSentence { get; set; }
    }
}
