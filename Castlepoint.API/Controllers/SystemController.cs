using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

  // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Authorization;

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("system", Name = "System")]
    [Authorize]
    public class SystemController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<SystemController> _logger;

        public SystemController(IConfiguration config, ILogger<SystemController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        /// <summary>
        /// GET all systems
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public IActionResult Get([FromHeader] string CPDataPaging)
        {
            string entityAsJson = "";
            int pageCount = 0;
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                // Call the data factorty to get all the systems
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.System> systems = DataFactory.System.GetAllSystems(dataConfig);

                // Sort by Label
                systems.Sort((x, y) => String.Compare(x.Label, y.Label));

                // Check if a data page has been specified
                PagedData paged = new PagedData();
                paged.data = systems;
                paged.totalRecords = systems.Count;
                //TODO set paging
                paged.nextPageId = string.Empty;

                entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System GET exception: " + ex.Message;
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

        [HttpGet("filter", Name = "GetBySystemFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.systems.Count>0)
                {
                    foreach (SystemItemFilter rif in oFilter.systems)
                    {
                        DataFactory.Filter sysfilter = new DataFactory.Filter("PartitionKey", rif.systemuri, "eq");
                        filters.Add(sysfilter);
                    }
                }

                // Get the systems
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.System> systems = DataFactory.System.GetSystems(datacfg, filters);

                // Sort by Label
                systems.Sort((x, y) => String.Compare(x.Label, y.Label));

                entityAsJson = JsonConvert.SerializeObject(systems, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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

        [HttpGet("processingstatus", Name = "GetProcessingStatusByFilter")]
        public IActionResult GetProcessingStatusByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // TODO add filter
                //// Deserialize the system filter
                //SystemFilter oFilter = new SystemFilter();
                //if (filter != null && filter.Length > 0)
                //{
                //    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                //    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                //}

                //// Create the filters
                //List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                //if (oFilter.systems.Count > 0)
                //{
                //    foreach (SystemItemFilter rif in oFilter.systems)
                //    {
                //        DataFactory.Filter sysfilter = new DataFactory.Filter("PartitionKey", rif.systemuri, "eq");
                //        filters.Add(sysfilter);
                //    }
                //}

                // Get the system processing data
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.ProcessingBatchStatus> processingStatus = DataFactory.ProcessingBatchStatus.GetProcessingStatus(datacfg);

                // Sort by Label
                processingStatus.Sort((x, y) => String.Compare(x.SystemUri, y.SystemUri));

                entityAsJson = JsonConvert.SerializeObject(processingStatus, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "SystemProcessingStatus GET exception: " + ex.Message;
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

        [HttpGet("finditem", Name = "FindByItemFilter")]
        public IActionResult FindByItemFilter([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            string entityAsJson = "";
            int pageCount = 0;
            string thisPageId = string.Empty;

            List<POCO.Files.CPFile> foundItems = new List<POCO.Files.CPFile>();
            try
            {

                _logger.LogInformation("CPAPI: FindByItemFilter");

                // Deserialize the item filter
                ItemFilter oFilter = new ItemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);
                    oFilter = JsonConvert.DeserializeObject<ItemFilter>(filterDecoded);
                }

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging!=null && CPDataPaging!=string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.itemuri != string.Empty)
                {
                    string cleanPK = Utils.CleanTableKey(oFilter.itemuri);
                    DataFactory.Filter urifilter = new DataFactory.Filter("PartitionKey", cleanPK, "ge");
                    filters.Add(urifilter);
                    urifilter = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(cleanPK), "lt");
                    filters.Add(urifilter);
                }
                if (oFilter.filename != string.Empty)
                {
                    DataFactory.Filter filenamefilter = new DataFactory.Filter("SourceFileName", oFilter.filename, "ge");
                    filters.Add(filenamefilter);
                    filenamefilter = new DataFactory.Filter("SourceFileName", Utils.GetLessThanFilter(oFilter.filename), "lt");
                    filters.Add(filenamefilter);
                }

                if (filters.Count==0)
                {
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Get all the systems
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.System> systems = DataFactory.System.GetAllSystems(datacfg);

                // File search is per system type, so only keep one of each type to reduce the searches required
                List<string> systemTypes = new List<string>();
                for (int i=systems.Count-1;i>=0; i--)
                {
                    //string systemType = string.Empty;
                    //switch(systems[i].Type.ToLower().Trim())
                    //{
                    //    case POCO.SystemType.Basecamp:
                    //        {
                    //            systemType = "basecamp";
                    //            break;
                    //        }
                    //    case POCO.SystemType.GoogleDrive:
                    //        {
                    //            systemType = "googledrive";
                    //            break;
                    //        }
                    //    case POCO.SystemType.SharePoint2010:
                    //        {
                    //            systemType = "sharepoint2010";
                    //            break;
                    //        }
                    //    case POCO.SystemType.SharePoint2013:
                    //        {
                    //            systemType = "sharepoint2013";
                    //            break;
                    //        }
                    //    case POCO.SystemType.NTFSShare:
                    //        {
                    //            systemType = "ntfsshare";
                    //            break;
                    //        }
                    //    case POCO.SystemType.SharePointTeam:
                    //    case POCO.SystemType.SharePointOneDrive:
                    //    case POCO.SystemType.SharePointOnline:
                    //        {
                    //            systemType = "sharepointonline";
                    //            break;
                    //        }
                    //    case POCO.SystemType.SakaiAlliance:
                    //        {
                    //            systemType = "sakai";
                    //            break;
                    //        }
                    //    default:
                    //        throw new ApplicationException("The SystemType is not recognised: " + systems[i].Type);
                    //        break;
                    //}

                    // TODO
                    // GM to search only one system, which in reality will search all files for all systems of the same type
                    // SUCH HACK. MUCH FILE. WOW. AMAZE.
                    // Check if this System.Type is already in our list
                    if (!systemTypes.Contains(systems[i].Type))
                    {
                        systemTypes.Add(systems[i].Type);
                    }
                    else
                    {
                        systems.RemoveAt(i);
                    }
                }

                if (dataPaging!=null && dataPaging.thisPageId!=null && dataPaging.thisPageId!=string.Empty)
                {
                    thisPageId = dataPaging.thisPageId;
                }

                string nextPageId = string.Empty;
                foreach(POCO.System sys in systems)
                {
                    int rowsToRequest = Utils.GetMaxRows() - foundItems.Count;
                    List<POCO.Files.CPFile> filesForSystem = DataFactory.System.GetFiles(datacfg, sys, filters, thisPageId, rowsToRequest, out nextPageId);
                    foundItems.AddRange(filesForSystem);
                }

                // Sort by Item Uri
                foundItems.Sort((x, y) => String.Compare(x.ItemUri, y.ItemUri));

                // Check if a data page has been specified
                PagedData paged = new PagedData();
                paged.data = foundItems;
                paged.totalRecords = foundItems.Count;
                paged.nextPageId = nextPageId;

                entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
                //log.Info("Exception occurred extracting text from uploaded file \r\nError: " + ex.Message);
                if (ex.InnerException != null)
                {
                    exceptionMsg = exceptionMsg + "[" + ex.InnerException.Message + "]";
                }

                _logger.LogError(exceptionMsg);
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            // Create the Result object
            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }

        [HttpGet("stats/filetype/filter", Name = "GetFileTypeStatsByFilter")]
        public IActionResult GetFileTypeStatsByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetFileTypeStatsByFilter");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.systems.Count > 0)
                {
                    foreach (SystemItemFilter rif in oFilter.systems)
                    {
                        DataFactory.Filter sysfilter = new DataFactory.Filter("PartitionKey", rif.systemuri, "eq");
                        filters.Add(sysfilter);
                    }
                }

                // Get the filetype stats
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.Stat> filetypestats = DataFactory.Stats.GetFileType(datacfg, filters);

                entityAsJson = JsonConvert.SerializeObject(filetypestats, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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

        [HttpGet("stats/filemetadata/filter", Name = "GetFileMetadataStatsByFilter")]
        public IActionResult GetFileMetadataStatsByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetFileMetadataStatsByFilter");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.systems.Count > 0)
                {
                    foreach (SystemItemFilter rif in oFilter.systems)
                    {
                        DataFactory.Filter sysfilter = new DataFactory.Filter("PartitionKey", rif.systemuri, "eq");
                        filters.Add(sysfilter);
                    }
                }

                // Get the filetype stats
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.Stat> filetypestats = DataFactory.Stats.GetFileMetadata(datacfg, filters);

                entityAsJson = JsonConvert.SerializeObject(filetypestats, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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

        [HttpGet("stats/filesize/filter", Name = "GetFileSizeStatsByFilter")]
        public IActionResult GetFileSizeStatsByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetFileSizeStatsByFilter");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.systems.Count > 0)
                {
                    foreach (SystemItemFilter rif in oFilter.systems)
                    {
                        DataFactory.Filter sysfilter = new DataFactory.Filter("PartitionKey", rif.systemuri, "eq");
                        filters.Add(sysfilter);
                    }
                }

                // Get the filesize stats
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.Stat> fileSizeStats = DataFactory.Stats.GetFileSize(datacfg, filters);

                entityAsJson = JsonConvert.SerializeObject(fileSizeStats, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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

        [HttpGet("stats/fileprocessing/filter", Name = "GetFileProcessingStatsByFilter")]
        public IActionResult GetFileProcessingStatsByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetFileProcessingStatsByFilter");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.systems.Count > 0)
                {
                    foreach (SystemItemFilter rif in oFilter.systems)
                    {
                        DataFactory.Filter sysfilter = new DataFactory.Filter("PartitionKey", rif.systemuri, "eq");
                        filters.Add(sysfilter);
                    }
                }

                // Get the filesize stats
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.Stat> fileProcessingStats = DataFactory.Stats.GetFileProcessing(datacfg, filters);

                entityAsJson = JsonConvert.SerializeObject(fileProcessingStats, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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

        [HttpGet("graph/ontology", Name = "GraphOntology")]
        public IActionResult GraphOntology([FromHeader] string filter)
        {
            throw new NotImplementedException();

            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Graph Ontology");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                CloudTable table = Utils.GetCloudTable("stlprecordkeyphrases", _logger);

                // Create a default query
                TableQuery<RecordKeyPhraseEntity> query = new TableQuery<RecordKeyPhraseEntity>();
                if (oFilter.systems.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (SystemItemFilter rif in oFilter.systems)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.systemuri);

                        if (cleanFilterPKey != "")
                        {
                            string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);

                            combinedFilter = pkquery;

                        }
                    }

                    // Create final combined query
                    _logger.LogInformation("SystemFilter query: " + combinedFilter);
                    query = new TableQuery<RecordKeyPhraseEntity>().Where(combinedFilter);
                }
                else
                {
                    _logger.LogInformation("SystemFilter query BLANK");
                }
                List<RecordKeyPhraseEntity> systemEntities = new List<RecordKeyPhraseEntity>();
                TableContinuationToken token = null;

                var runningQuery = new TableQuery<RecordKeyPhraseEntity>()
                {
                    FilterString = query.FilterString,
                    SelectColumns = query.SelectColumns
                };

                do
                {
                    runningQuery.TakeCount = query.TakeCount - systemEntities.Count;

                    Task<TableQuerySegment<RecordKeyPhraseEntity>> tSeg = table.ExecuteQuerySegmentedAsync<RecordKeyPhraseEntity>(runningQuery, token);
                    tSeg.Wait();
                    token = tSeg.Result.ContinuationToken;
                    systemEntities.AddRange(tSeg.Result);

                } while (token != null && (query.TakeCount == null || systemEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

                
                entityAsJson = JsonConvert.SerializeObject(systemEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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
            public string RAFunction { get; set; }
            public string RAClass { get; set; }

        }

        [HttpGet("stats", Name = "GetSystemStats")]
        public IActionResult GetStats([FromHeader] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("User: " + User.Identity.Name);

                _logger.LogInformation("CPAPI: Get System Stats");

                // Deserialize the system filter
                SystemFilter oFilter = new SystemFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing System filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<SystemFilter>(filter);
                }

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.System> systems = DataFactory.System.GetAllSystems(dataConfig);

                // Create a system summary entity
                SystemStatsSummary statsSummary = new SystemStatsSummary();
                foreach(POCO.System sysent in systems)
                {
                    // Validate the entity
                    if (sysent!=null)
                    {
                        // Increment total counts for each system
                        statsSummary.TotalSystems++;
                        if (sysent.JsonSystemStats!=null && sysent.JsonSystemStats!="")
                        {
                            SystemStat stat = JsonConvert.DeserializeObject<SystemStat>(sysent.JsonSystemStats);
                            statsSummary.TotalRecords += stat.NumRecords;
                            statsSummary.TotalItems += stat.NumItems;
                            statsSummary.TotalKeyPhrases += stat.NumFileKeyPhrases;
                        }
                    }
                }

                entityAsJson = JsonConvert.SerializeObject(statsSummary, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "System Filter GET exception: " + ex.Message;
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

        public class SystemConfigurationModel
        {
            public string partitionkey;
            public string rowkey;
            public string systemconfiguration;
        }

        public class ConnectionConfigurationModel
        {
            public string partitionkey;
            public string rowkey;
            public string connectionconfiguration;
        }

        public class RecordConfigurationModel
        {
            public string partitionkey;
            public string rowkey;
            public string recordconfiguration;
        }

        [HttpPost("updatesystemconfiguration", Name = "UpdateSystemConfiguration")]
        public IActionResult UpdateSystemConfiguration([FromBody]SystemConfigurationModel sysconfig)
        {
            _logger.LogInformation("CPAPI: UpdateSystemConfiguration");

            // Verify the POST info
            if (sysconfig.partitionkey == null || sysconfig.partitionkey == string.Empty || sysconfig.rowkey == null || sysconfig.rowkey == string.Empty || sysconfig.systemconfiguration == null || sysconfig.systemconfiguration == string.Empty)
            {
                // Throw error
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // URL decode the system and system configuration
            string pk = System.Net.WebUtility.HtmlDecode(sysconfig.partitionkey);
            pk = System.Net.WebUtility.UrlDecode(pk);
            string rk = System.Net.WebUtility.HtmlDecode(sysconfig.rowkey);
            rk = System.Net.WebUtility.UrlDecode(rk);
            string newSysConfig = System.Net.WebUtility.HtmlDecode(sysconfig.systemconfiguration);
            newSysConfig = System.Net.WebUtility.UrlDecode(newSysConfig);

            // Make sure the provided update can be converted into a POCO SystemConfiguration
            _logger.LogInformation("UpdateSystemConfiguration: validating provided configuration...");
            POCO.SystemConfig newSysCfgPOCO = new POCO.SystemConfig();
            try
            {
                newSysCfgPOCO = JsonConvert.DeserializeObject<POCO.SystemConfig>(newSysConfig);
            }
            catch (Exception ex)
            {
                _logger.LogError("UpdateSystemConfiguration: error deserializing sys configuration=" + newSysConfig + " [" + ex.Message + "]");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Find the System to update
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkfilter = new DataFactory.Filter("PartitionKey", Castlepoint.Utilities.Converters.CleanTableKey(sysconfig.partitionkey), "eq");
            filters.Add(pkfilter);
            DataFactory.Filter rkfilter = new DataFactory.Filter("RowKey", Castlepoint.Utilities.Converters.CleanTableKey(sysconfig.rowkey), "eq");
            filters.Add(rkfilter);

            List<POCO.System> sys = DataFactory.System.GetSystems(datacfg, filters);

            // Make sure we only got one Record returned
            if (sys.Count != 1)
            {
                // Throw error
                _logger.LogError("UpdateSystemConfiguration: expect 1 system, got count=" + sys.Count.ToString());
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            bool isUpdateOK = DataFactory.System.UpdateSystemConfig(datacfg, sys[0], newSysCfgPOCO);

            if (isUpdateOK)
            {
                return StatusCode((int)System.Net.HttpStatusCode.NoContent);
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        [HttpPost("updateconnectionconfiguration", Name = "UpdateConnectionConfiguration")]
        public IActionResult UpdateConnectionConfiguration([FromBody]ConnectionConfigurationModel connconfig)
        {
            _logger.LogInformation("CPAPI: UpdateSystemConfiguration");

            // Verify the POST info
            if (connconfig==null
                || string.IsNullOrEmpty(connconfig.partitionkey) || string.IsNullOrEmpty(connconfig.rowkey)
                || string.IsNullOrEmpty(connconfig.connectionconfiguration))
            {
                // Throw error
                _logger.LogWarning("UpdateConnectionConfiguration: null or blank values provided in connection configuration");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // URL decode the system and system configuration
            string pk = System.Net.WebUtility.HtmlDecode(connconfig.partitionkey);
            pk = System.Net.WebUtility.UrlDecode(pk);
            string rk = System.Net.WebUtility.HtmlDecode(connconfig.rowkey);
            rk = System.Net.WebUtility.UrlDecode(rk);
            string newConnConfig = System.Net.WebUtility.HtmlDecode(connconfig.connectionconfiguration);
            newConnConfig = System.Net.WebUtility.UrlDecode(newConnConfig);

            // Find the System to update
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkfilter = new DataFactory.Filter("PartitionKey", Castlepoint.Utilities.Converters.CleanTableKey(connconfig.partitionkey), "eq");
            filters.Add(pkfilter);
            DataFactory.Filter rkfilter = new DataFactory.Filter("RowKey", Castlepoint.Utilities.Converters.CleanTableKey(connconfig.rowkey), "eq");
            filters.Add(rkfilter);

            List<POCO.System> sys = DataFactory.System.GetSystems(datacfg, filters);

            // Make sure we only got one Record returned
            if (sys.Count != 1)
            {
                // Throw error
                _logger.LogWarning("UpdateConnectionConfiguration: expected 1 system, got: " + sys.Count.ToString());
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            bool isUpdateOK = DataFactory.System.UpdateConnectionConfig(datacfg, sys[0], newConnConfig);

            if (isUpdateOK)
            {
                _logger.LogInformation("UpdateConnectionConfiguration: connection configuration updated successfully");
                return StatusCode((int)System.Net.HttpStatusCode.NoContent);
            }
            else
            {
                _logger.LogWarning("UpdateConnectionConfiguration: update to data store return NOT OK");
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }


        [HttpPost("updaterecordconfiguration", Name = "UpdateRecordConfiguration")]
        public IActionResult UpdateRecordConfiguration([FromBody]RecordConfigurationModel recordconfig)
        {
            _logger.LogInformation("CPAPI: UpdateRecordConfiguration");

            // Verify the POST info
            if (recordconfig == null
                || string.IsNullOrEmpty(recordconfig.partitionkey) || string.IsNullOrEmpty(recordconfig.rowkey)
                || string.IsNullOrEmpty(recordconfig.recordconfiguration))
            {
                // Throw error
                _logger.LogWarning("UpdateRecordConfiguration: blank value provided in RecordConfigurationModel");
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // URL decode the system and system configuration
            string pk = System.Net.WebUtility.HtmlDecode(recordconfig.partitionkey);
            pk = System.Net.WebUtility.UrlDecode(pk);
            string rk = System.Net.WebUtility.HtmlDecode(recordconfig.rowkey);
            rk = System.Net.WebUtility.UrlDecode(rk);
            string newRecordConfig = System.Net.WebUtility.HtmlDecode(recordconfig.recordconfiguration);
            newRecordConfig = System.Net.WebUtility.UrlDecode(newRecordConfig);

            // Check the record config is JSON compatible
            try
            {
                // Check that the record config data deserialzes to json
                POCO.SystemRecordIdentificationConfig cfgupdate = Newtonsoft.Json.JsonConvert.DeserializeObject<POCO.SystemRecordIdentificationConfig>(newRecordConfig);
                if (cfgupdate==null
                    || string.IsNullOrEmpty(cfgupdate.ContextLevel)) // ContextLevel must always be set to be valid
                {
                    _logger.LogWarning("UpdateRecordConfiguration: JSON deserialize resulted in blank value for ContextLevel");
                    throw new ApplicationException("Record Configuration JSON deserialize resulted in null or blank string for ContextLevel");
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            // Find the System to update
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkfilter = new DataFactory.Filter("PartitionKey", Castlepoint.Utilities.Converters.CleanTableKey(recordconfig.partitionkey), "eq");
            filters.Add(pkfilter);
            DataFactory.Filter rkfilter = new DataFactory.Filter("RowKey", Castlepoint.Utilities.Converters.CleanTableKey(recordconfig.rowkey), "eq");
            filters.Add(rkfilter);

            List<POCO.System> sys = DataFactory.System.GetSystems(datacfg, filters);

            // Make sure we only got one Record returned
            if (sys.Count != 1)
            {
                // Throw error
                _logger.LogWarning("UpdateRecordConfiguration: expected 1 System to be returned, got: " + sys.Count.ToString());
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

            bool isUpdateOK = DataFactory.System.UpdateRecordConfig(datacfg, sys[0], newRecordConfig);

            if (isUpdateOK)
            {
                _logger.LogInformation("UpdateConnectionConfiguration: record configuration updated successfully");
                return StatusCode((int)System.Net.HttpStatusCode.NoContent);
            }
            else
            {
                _logger.LogWarning("UpdateRecordConfiguration: DataFactory.System.UpdateRecordConfig returned false");
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        private bool SetSystemEnable(string systemuri, bool enabledState)
        {
            bool isUpdated = false;

            // Clean the key
            //if (!systemuri.EndsWith("/") && !systemuri.EndsWith("|")) { systemuri += "/"; }

            // Call the data factory to get this system
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
            DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(systemuri), "eq");
            filters.Add(pkfilt);

            DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
            List<POCO.System> systems = DataFactory.System.GetSystems(dataConfig, filters);

            // Make sure we only have one system
            if (systems.Count != 1)
            {
                return false;
            }

            // Check current state of the system - no need to change if it is already set
            if (systems[0].Enabled == enabledState)
            {
                // Nothing to do - return success
                return true;
            }

            // Set enabled state of system
            isUpdated = DataFactory.System.SetConfigEnabled(dataConfig, systems[0].PartitionKey, systems[0].RowKey, enabledState);

            return isUpdated;
        }


        [HttpPost("forceresentence", Name = "ForceResentence")]
        public IActionResult ForceResentence([FromHeader] string systems)
        {
            _logger.LogInformation("CPAPI: ForceResentence");

            bool isResentenceOK = false;

            // Check provided parameter
            if (systems == null || systems == string.Empty || systems.Length > 10000)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // deserialize the param
            string decodedSystems = System.Web.HttpUtility.UrlDecode(systems);
            Systems systemsToResentence = JsonConvert.DeserializeObject<Systems>(decodedSystems);
            if (systemsToResentence.systems.Count == 0)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            DataFactory.DataConfig datacfg = Utils.GetDataConfig();

            foreach (string s in systemsToResentence.systems)
            {
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter pkfilter = new DataFactory.Filter("PartitionKey", s, "eq");

                List<POCO.Record> recordsInSystem = DataFactory.Record.GetRecords(datacfg, filters);
                foreach(POCO.Record r in recordsInSystem)
                {
                    // Delete any record match status entries for the record
                    DataFactory.Record.DeleteRecordMatchStatus(datacfg, r);

                    // Delete any record authority matches for the record
                    DataFactory.Record.DeleteRecordAuthorityMatches(datacfg, r);

                    // Set the change flag for the Record to force a resentence
                    DataFactory.Record.ForceChangeFlag(datacfg, r);
                }
            }

            //TODO check that resentence completed successfully
            isResentenceOK = true;

            // Return result
            if (isResentenceOK)
            {
                return StatusCode((int)System.Net.HttpStatusCode.NoContent);
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        /// <summary>
        /// Enable system
        /// </summary>
        /// <param name="systems"></param>
        /// <returns></returns>
        [HttpPost("enable", Name = "SystemEnable")]
        public IActionResult SystemEnable([FromHeader] string systems)
        {
            _logger.LogInformation("CPAPI: SystemEnable");

            bool enableSystem = true;

            // Check provided parameter
            if (systems == null || systems == string.Empty || systems.Length>10000)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // deserialize the param
            string decodedSystems = System.Web.HttpUtility.UrlDecode(systems);
            SystemEnableDisable systemsToEnable = JsonConvert.DeserializeObject<SystemEnableDisable>(decodedSystems);
            if (systemsToEnable.systems.Count==0)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            string entityAsJson = string.Empty;
            SystemEnableDisableResult results = new SystemEnableDisableResult();

            foreach(string sys in systemsToEnable.systems)
            {
                string decodeSystemUri = System.Net.WebUtility.HtmlDecode(sys);
                decodeSystemUri = System.Net.WebUtility.UrlDecode(decodeSystemUri);
                bool isSystemUpdated = SetSystemEnable(decodeSystemUri, enableSystem);

                if (isSystemUpdated)
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult();
                    result.systemuri = decodeSystemUri;
                    result.result = "enabled";
                    results.systems.Add(result);
                }
                else
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult();
                    result.systemuri = decodeSystemUri;
                    result.result = "updated";
                    results.systems.Add(result);
                }

            }

            // Return the added system entity
            entityAsJson = JsonConvert.SerializeObject(results, Formatting.Indented);
            ObjectResult objectresult = new ObjectResult(entityAsJson);
            return objectresult;

        }

        /// <summary>
        /// Enable system
        /// </summary>
        /// <param name="systems"></param>
        /// <returns></returns>
        [HttpPost("v2/enable", Name = "SystemEnableV2")]
        public IActionResult SystemEnableV2([FromBody] SystemEnableDisable systemsToEnable)
        {
            _logger.LogInformation("CPAPI: SystemEnable");

            bool enableSystem = true;
            // Check provided parameter
            if (systemsToEnable == null || systemsToEnable.systems.Count > 10000)
            {
                return BadRequest("The data is invalid");
            }

            SystemEnableDisableResult results = new SystemEnableDisableResult();

            foreach (string sys in systemsToEnable.systems)
            {
                string decodeSystemUri = System.Net.WebUtility.HtmlDecode(sys);
                decodeSystemUri = System.Net.WebUtility.UrlDecode(decodeSystemUri);
                bool isSystemUpdated = SetSystemEnable(decodeSystemUri, enableSystem);

                if (isSystemUpdated)
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult
                    {
                        systemuri = decodeSystemUri,
                        result = "enabled"
                    };
                    results.systems.Add(result);
                }
                else
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult
                    {
                        systemuri = decodeSystemUri,
                        result = "updated"
                    };
                    results.systems.Add(result);
                }
            }

            // Return the added system entity
            string entityAsJson = JsonConvert.SerializeObject(results, Formatting.Indented);
            ObjectResult objectresult = new ObjectResult(entityAsJson);
            return objectresult;
        }

        /// <summary>
        /// Enable system
        /// </summary>
        /// <param name="systemuri"></param>
        /// <returns></returns>
        [HttpPost("disable", Name = "SystemDisable")]
        public IActionResult SystemDisable([FromHeader] string systems)
        {
            _logger.LogInformation("CPAPI: SystemEnable");

            bool enableSystem = false;

            // Check provided parameter
            if (systems == null || systems == string.Empty)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // deserialize the param
            string decodedSystems = System.Web.HttpUtility.UrlDecode(systems);
            SystemEnableDisable systemsToEnable = JsonConvert.DeserializeObject<SystemEnableDisable>(decodedSystems);
            if (systemsToEnable.systems.Count == 0)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            string entityAsJson = string.Empty;
            SystemEnableDisableResult results = new SystemEnableDisableResult();

            foreach (string sys in systemsToEnable.systems)
            {
                string decodeSystemUri = System.Net.WebUtility.HtmlDecode(sys);
                decodeSystemUri = System.Net.WebUtility.UrlDecode(decodeSystemUri);
                bool isSystemUpdated = SetSystemEnable(decodeSystemUri, enableSystem);

                if (isSystemUpdated)
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult();
                    result.systemuri = decodeSystemUri;
                    result.result = "disabled";
                    results.systems.Add(result);
                }
                else
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult();
                    result.systemuri = decodeSystemUri;
                    result.result = "not updated";
                    results.systems.Add(result);
                }

            }

            // Return the added system entity
            entityAsJson = JsonConvert.SerializeObject(results, Formatting.Indented);
            ObjectResult objectresult = new ObjectResult(entityAsJson);
            return objectresult;

        }

        /// <summary>
        /// Enable system
        /// </summary>
        /// <param name="systemuri"></param>
        /// <returns></returns>
        [HttpPost("v2/disable", Name = "SystemDisableV2")]
        public IActionResult SystemDisableV2([FromBody] SystemEnableDisable systemsToDisable)
        {
            _logger.LogInformation("CPAPI: SystemEnable");

            bool enableSystem = false;

            // Check provided parameter
            if (systemsToDisable == null || systemsToDisable.systems.Count == 0)
            {
                return BadRequest("The disable data is invalid");
            }

            SystemEnableDisableResult results = new SystemEnableDisableResult();

            foreach (string sys in systemsToDisable.systems)
            {
                string decodeSystemUri = System.Net.WebUtility.HtmlDecode(sys);
                decodeSystemUri = System.Net.WebUtility.UrlDecode(decodeSystemUri);
                bool isSystemUpdated = SetSystemEnable(decodeSystemUri, enableSystem);

                if (isSystemUpdated)
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult();
                    result.systemuri = decodeSystemUri;
                    result.result = "disabled";
                    results.systems.Add(result);
                }
                else
                {
                    SystemEnableDisableItemResult result = new SystemEnableDisableItemResult();
                    result.systemuri = decodeSystemUri;
                    result.result = "not updated";
                    results.systems.Add(result);
                }

            }

            // Return the added system entity
            string entityAsJson = JsonConvert.SerializeObject(results, Formatting.Indented);
            ObjectResult objectresult = new ObjectResult(entityAsJson);
            return objectresult;

        }

        /// <summary>
        /// Add a new system
        /// </summary>
        /// <param name="system"></param>
        /// <returns></returns>
        [HttpPost("add", Name = "SystemAdd")]
        public IActionResult SystemAdd([FromHeader] string system)
        {
            _logger.LogInformation("CPAPI: SystemAdd");

            // Deserialize the system
            POCO.System newSystem = new POCO.System();
            if (system != null && system.Length > 0)
            {
                _logger.LogDebug("Deserializing System of length: " + system.Length);
                newSystem = JsonConvert.DeserializeObject<POCO.System>(system);
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Validate the system
            if (newSystem.PartitionKey == null || newSystem.PartitionKey == string.Empty)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            if (newSystem.Label == null || newSystem.Label == string.Empty)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            if (newSystem.SystemUri == null || newSystem.SystemUri == string.Empty)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            if (string.IsNullOrEmpty(newSystem.Type))
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Check if the System Id has been set
            if (newSystem.SystemId==null || newSystem.SystemId==Guid.Empty)
            {
                // Set to a new Guid
                newSystem.SystemId = Guid.NewGuid();
            }

            // Clean the table and row keys
            if (!newSystem.PartitionKey.EndsWith("/")) { newSystem.PartitionKey += "/"; }
            newSystem.PartitionKey = Utils.CleanTableKey(newSystem.PartitionKey);
            newSystem.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));

            POCO.System addedSystem = AddSystem(newSystem);

            if (addedSystem!=null)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(addedSystem);
                return result;
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        /// <summary>
        /// Add a new system
        /// </summary>
        /// <param name="system"></param>
        /// <returns></returns>
        [HttpPost("v2/add", Name = "SystemAddV2")]
        public IActionResult SystemAddV2([FromBody] POCO.System system)
        {
            _logger.LogInformation("CPAPI: SystemAdd");
            if (system == null)
            {
                return BadRequest("No data in the request");
            }

            // Validate the system
            if (system.PartitionKey == null || system.PartitionKey == string.Empty)
            {
                return BadRequest("PartitionKey is invalid");
            }

            if (system.Label == null || system.Label == string.Empty)
            {
                return BadRequest("System label is invalid or empty");
            }

            if (system.SystemUri == null || system.SystemUri == string.Empty)
            {
                return BadRequest("System uri is invalid or empty");
            }

            if (string.IsNullOrEmpty(system.Type))
            {
                return BadRequest("System type is invalid or empty");
            }

            var newSystem = new POCO.System
            {
                PartitionKey = system.PartitionKey,
                Label = system.Label,
                SystemUri = system.SystemUri,
                Type = system.Type
            };
            //// Check if the System Id has been set
            if (newSystem.SystemId == null || newSystem.SystemId == Guid.Empty)
            {
                // Set to a new Guid
                newSystem.SystemId = Guid.NewGuid();
            }

            // Clean the table and row keys
            if (!newSystem.PartitionKey.EndsWith("/")) { newSystem.PartitionKey += "/"; }
            newSystem.PartitionKey = Utils.CleanTableKey(newSystem.PartitionKey);
            newSystem.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));

            POCO.System addedSystem = AddSystem(newSystem);

            if (addedSystem != null)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(addedSystem);
                return result;
            }
            else
            {
                return new UnprocessableEntityObjectResult("Internal server error has caused the system save to fail");
            }

        }

        private POCO.System AddSystem(POCO.System systemEntity)
        {
            bool isAddedOk = false;

            // Execute the insert operation. 
            //log.Verbose("Executing table operation");
            try
            {
                // Check if an SystemId has been set
                if (systemEntity.SystemId == null)
                {
                    systemEntity.SystemId = Guid.NewGuid();
                }

                // Call the Add datafactory method
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                DataFactory.System.Add(datacfg, systemEntity);
            }

            catch (Exception aex)
            {
                _logger.LogError("ERR exception: " + aex.Message);
            }

            isAddedOk = true;

            return systemEntity;
        }

        public class SystemEntity : TableEntity
        {
            public SystemEntity(string sourceUri, string itemUri)
            {
                this.PartitionKey = sourceUri;
                this.RowKey = itemUri;

            }

            public SystemEntity()
            {

            }

            //public string Properties { get; set; } 
            [IgnoreProperty]
            public string SystemUri { get { return this.PartitionKey; } }
            public Guid SystemId { get; set; }
            public string Label { get; set; }
            public bool ProcessRecords { get; set; }
            public string Type { get; set; }
            public string JsonConnectionConfig { get; set; }
            public string JsonRecordIdentificationConfig { get; set; }
            public string JsonSentenceStats { get; set; }
            public string JsonSystemStats { get; set; }
            public string JsonSystemConfig { get; set; }
            public string JsonSecurityClassConfig { get; set; }

        }

        class ItemFilter
        {
            public ItemFilter()
            {
                itemuri = string.Empty;
                filename = string.Empty;
            }
            public string itemuri;
            public string filename;
        }

        class SystemFilter
        {
            public SystemFilter()
            {
                systems = new List<SystemItemFilter>();
            }
            public List<SystemItemFilter> systems;
        }

        class SystemItemFilter
        {
            public string systemuri { get; set; }
        }
    }

    internal class Systems
    {
        public List<string> systems { get; set; }
    }

    public class SystemEnableDisable
    {
        public List<string> systems { get; set; }
    }

    internal class SystemEnableDisableResult
    {
        public SystemEnableDisableResult()
        {
            this.systems = new List<SystemEnableDisableItemResult>();
        }
        public List<SystemEnableDisableItemResult> systems { get; set; }
    }

    internal class SystemEnableDisableItemResult
    {
        public string systemuri { get; set; }
        public string result { get; set; }
    }

    internal class SystemStatsSummary
    {
        public SystemStatsSummary()
        {
            this.TotalItems = 0;
            this.TotalKeyPhrases = 0;
            this.TotalRecords = 0;
            this.TotalSystems = 0;
        }
        public int TotalSystems { get; set; }
        public long TotalRecords { get; set; }
        public long TotalItems { get; set; }
        public long TotalKeyPhrases { get; set; }
    }

    internal class SystemStat
    {
        public SystemStat()
        {
            this.NumFileKeyPhrases = 0;
            this.NumRecordAuthorityKeyPhrases = 0;
            this.NumItems = 0;
            this.NumRecords = 0;
        }
        public int NumFileKeyPhrases { get; set; }
        public int NumRecordAuthorityKeyPhrases { get; set; }
        public int NumItems { get; set; }
        public int NumRecords { get; set; }
    }
}
