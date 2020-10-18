using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

using Newtonsoft.Json;

using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.FileProviders;
using System.IO;

namespace Castlepoint.REST
{
    [Produces("application/json")]
    [Route("audit", Name = "Audit")]
    [Authorize]
    public class AuditController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<AuditController> _logger;

        public AuditController(IConfiguration config, ILogger<AuditController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        [HttpGet("sensitive/filter", Name = "GetBySensitiveFilter")]
        public IActionResult GetBySensitiveFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetBySensitiveFilter");

                // Deserialize the sensitive data filter
                SensitiveFilter oFilter = new SensitiveFilter();
                if (filter != null && filter.Length > 0)
                {
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);

                    _logger.LogDebug("Deserializing filter of length: " + filterDecoded.Length);
                    oFilter = JsonConvert.DeserializeObject<SensitiveFilter>(filterDecoded);
                }

                // Create filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.types.Count > 0)
                {
                    foreach (string type in oFilter.types)
                    {
                        string typeKey = Utils.CleanTableKey(type);
                        //cleanFilterRKey = System.Web.HttpUtility.UrlEncode(cleanFilterRKey);
                        //cleanFilterRKey = cleanFilterRKey.Replace("&", "&amp;");
                        if (typeKey != "")
                        {
                            DataFactory.Filter f1 = new DataFactory.Filter("CaptureType", typeKey, "eq");
                            filters.Add(f1);
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("Sensitive Data query BLANK");
                }

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.CaptureRegexMatch> captureMatches = DataFactory.Ontology.GetRegexMatches(dataConfig, filters);

                // Sort the data by descending score
                captureMatches = captureMatches.OrderByDescending(x => x.ConfidenceScore).ToList();

                entityAsJson = JsonConvert.SerializeObject(captureMatches, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Capture GET exception: " + ex.Message;
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

        [HttpGet("filter/item", Name = "GetByItemFilter")]
        public IActionResult GetByItemFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetByItemFilter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);

                    _logger.LogDebug("Deserializing Item filter of length: " + filterDecoded.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filterDecoded);
                }

                // Create filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.records.Count > 0)
                {
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.itemuri);
                        //cleanFilterRKey = System.Web.HttpUtility.UrlEncode(cleanFilterRKey);
                        //cleanFilterRKey = cleanFilterRKey.Replace("&", "&amp;");
                        if (cleanFilterPKey != "")
                        {
                            DataFactory.Filter f1 = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                            filters.Add(f1);
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();

                // Get the last 3 months of log data by default
                List<POCO.O365.AuditLogEntry> logEntries = new List<POCO.O365.AuditLogEntry>();
                DateTime today = DateTime.UtcNow;
                for (int i = 0; i < 2; i++)
                {
                    // Calculate the suffix of the table
                    DateTime tableSuffixDate = today.AddMonths(-1 * i);
                    string tableSuffix = tableSuffixDate.ToString(Utils.TableSuffixDateFormatYM);

                    // Add the range for the month to our final collection
                    List<POCO.O365.AuditLogEntry> monthEntries = DataFactory.O365.AuditLog.GetForItem(dataConfig, filters, tableSuffix);
                    logEntries.AddRange(monthEntries);

                }

                // Sort the data by descending date
                logEntries.Sort((x, y) => DateTime.Compare(y.CreationDateTime, x.CreationDateTime));

                entityAsJson = JsonConvert.SerializeObject(logEntries, Formatting.Indented);

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

        [HttpGet("filter/deleted", Name = "GetDeletedByItemFilter")]
        public IActionResult GetDeletedByItemFilter([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            int pageCount = 0;
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetDeletedByItemFilter");

                // Deserialize the ontology filter
                RecordFilter oFilter = new RecordFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Item filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordFilter>(filter);
                }

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                // Create filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter deleteFilter = new DataFactory.Filter("Operation", "FileDeleted", "eq");

                if (oFilter.records.Count > 0)
                {
                    foreach (RecordItemFilter rif in oFilter.records)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(rif.itemuri);
                        //cleanFilterRKey = System.Web.HttpUtility.UrlEncode(cleanFilterRKey);
                        //cleanFilterRKey = cleanFilterRKey.Replace("&", "&amp;");
                        if (cleanFilterPKey != "")
                        {
                            DataFactory.Filter f1 = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "ge");
                            filters.Add(f1);
                            DataFactory.Filter f2 = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(cleanFilterPKey), "lt");
                            filters.Add(f2);
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("RecordFilter query BLANK");
                }


                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();

                List<POCO.O365.AuditLogEntry> logEntries = new List<POCO.O365.AuditLogEntry>();
                string nextPageId = string.Empty;
                logEntries = DataFactory.O365.AuditLog.GetActionableEvents(dataConfig, string.Empty, filters, dataPaging.thisPageId, Utils.GetMaxRows(), out nextPageId);

                // Get the last 3 months of log data by default
                //DateTime today = DateTime.UtcNow;
                //for (int i = 0; i < 2; i++)
                //{
                //    // Calculate the suffix of the table
                //    DateTime tableSuffixDate = today.AddMonths(-1 * i);
                //    string tableSuffix = tableSuffixDate.ToString(Utils.TableSuffixDateFormatYM);

                //    // Add the range for the month to our final collection
                //    List<POCO.O365.AuditLogEntry> monthEntries = DataFactory.O365.AuditLog.GetActionableEventsForItem(dataConfig, filters, tableSuffix);
                //    logEntries.AddRange(monthEntries);

                //}

                // Sort the data by descending date
                logEntries.Sort((x, y) => DateTime.Compare(y.CreationDateTime, x.CreationDateTime));

                // Check if a data page has been specified
                PagedData paged = new PagedData();
                if (logEntries.Count > 0 )
                {
                    List<POCO.O365.AuditLogEntry> pageOfData = new List<POCO.O365.AuditLogEntry>();

                    // Check that the requested data is in range
                    dataPaging.page--;  // pages are zero-based to calculate the correct range
                    int startOfPage = (dataPaging.page * dataPaging.perPage);
                    if (logEntries.Count > startOfPage + dataPaging.perPage - 1)
                    {
                        pageOfData = logEntries.GetRange(startOfPage, dataPaging.perPage);
                    }
                    else
                    {
                        _logger.LogError("Data paging request out of range");
                        return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                    }

                    // Set the PageCount Response Header
                    //decimal totalPages = (decimal)(logEntries.Count / dataPaging.perPage);
                    //pageCount = (int)Math.Ceiling(totalPages);

                    paged.data = pageOfData;
                    paged.totalRecords = logEntries.Count;

                    entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);
                }
                else
                {
                    paged.data = logEntries;
                    paged.totalRecords = logEntries.Count;

                    entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);
                }
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

        [HttpGet("filter/deleted/all", Name = "GetDeleted")]
        public IActionResult GetDeleted([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            int pageCount = 0;
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetDeleted");

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();

                List<POCO.O365.AuditLogEntry> logEntries = new List<POCO.O365.AuditLogEntry>();
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                string nextPageId = string.Empty;
                DataFactory.Filter deleteFilter = new DataFactory.Filter("Operation", "FileDeleted", "eq");
                logEntries = DataFactory.O365.AuditLog.GetActionableEvents(dataConfig, string.Empty, filters, string.Empty, Utils.GetMaxRows(), out nextPageId);

                // Get the last 3 months of log data by default
                //DateTime today = DateTime.UtcNow;
                //for (int i = 0; i < 2; i++)
                //{
                //    // Calculate the suffix of the table
                //    DateTime tableSuffixDate = today.AddMonths(-1 * i);
                //    string tableSuffix = tableSuffixDate.ToString(Utils.TableSuffixDateFormatYM);

                //    // Add the range for the month to our final collection
                //    List<POCO.O365.AuditLogEntry> monthEntries = DataFactory.O365.AuditLog.GetActionableEvents(dataConfig, tableSuffix);
                //    logEntries.AddRange(monthEntries);

                //}

                // Sort the data by descending date
                logEntries.Sort((x, y) => DateTime.Compare(y.CreationDateTime, x.CreationDateTime));

                // Check if a data page has been specified
                PagedData paged = new PagedData();
                if (logEntries.Count > 0 && dataPaging.page > 0 && dataPaging.perPage > 0)
                {
                    List<POCO.O365.AuditLogEntry> pageOfData = new List<POCO.O365.AuditLogEntry>();

                    // Check that the requested data is in range
                    dataPaging.page--;  // pages are zero-based to calculate the correct range
                    int startOfPage = (dataPaging.page * dataPaging.perPage);
                    if (logEntries.Count > startOfPage + dataPaging.perPage - 1)
                    {
                        pageOfData = logEntries.GetRange(startOfPage, dataPaging.perPage);
                    }
                    else
                    {
                        _logger.LogError("Data paging request out of range");
                        return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                    }

                    paged.data = pageOfData;
                    paged.totalRecords = logEntries.Count;

                    entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);
                }
                else
                {
                    paged.data = logEntries;
                    paged.totalRecords = logEntries.Count;

                    entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);
                }
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

        [HttpGet("actionable/filter", Name = "GetActionableEvents")]
        public IActionResult GetActionableEvents([FromHeader] string filter, [FromHeader] string CPDataPaging)
        {
            int pageCount = 0;
            string thisPageId = string.Empty;
            string nextPageId = string.Empty;
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetActionableEvents");

                // Get filter requests
                ActionableEventsFilter eventsFilter = new ActionableEventsFilter();
                if (filter != null && filter != string.Empty)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);
                    eventsFilter = JsonConvert.DeserializeObject<ActionableEventsFilter>(filterDecoded);
                }

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                // Check which filter type
                string filterType = GetActionableEventsDataFilterType(eventsFilter);

                // Set the data filters
                List<DataFactory.Filter> filters = GetActionableEventsDataFilter(eventsFilter);

                // Get the data
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.O365.AuditLogEntry> logEntries = new List<POCO.O365.AuditLogEntry>();

                if (dataPaging != null && dataPaging.thisPageId != null && dataPaging.thisPageId != string.Empty)
                {
                    thisPageId = dataPaging.thisPageId;
                }

                // Get the data
                int rowsToRequest = Utils.GetMaxRows() - logEntries.Count;
                logEntries = DataFactory.O365.AuditLog.GetActionableEvents(datacfg, filterType, filters, thisPageId, rowsToRequest, out nextPageId);

                // Sort the data by descending date
                logEntries.Sort((x, y) => DateTime.Compare(y.CreationDateTime, x.CreationDateTime));

                // Check if a data page has been specified
                PagedData paged = new PagedData();
                paged.data = logEntries;
                paged.totalRecords = logEntries.Count;
                paged.nextPageId = nextPageId;

                entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);

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

        public class ActionableEventsFilter
        {
            public ActionableEventsFilter()
            {
                datefrom = string.Empty;
                dateto = string.Empty;
                operations = new List<string>();
                userids = new List<string>();
                workloads = new List<string>();
            }
            public string datefrom { get; set; }
            public string dateto { get; set; }
            public List<string> operations { get; set; }
            public List<string> userids { get; set; }
            public string filepath { get; set; }
            public List<string> workloads { get; set; }
        }

        private string GetActionableEventsDataFilterType(ActionableEventsFilter eventsFilter)
        {
            string filterType = "byfile";

            // Check which filter is used to determine the best PartitionKey or RowKey or field
            if (eventsFilter==null)
            {
                return "byfile";
            }

            // File path only check
            if ((eventsFilter.filepath != null && eventsFilter.filepath.Trim() != string.Empty)
                && (eventsFilter.datefrom == null || eventsFilter.datefrom.Trim() == string.Empty)
                && (eventsFilter.dateto == null || eventsFilter.dateto.Trim() == string.Empty)
                && (eventsFilter.userids == null || eventsFilter.userids.Count == 0))
            {
                return "byfile";
            }

            if ((eventsFilter.datefrom != null && eventsFilter.datefrom.Trim() != string.Empty)
                || (eventsFilter.dateto != null && eventsFilter.dateto.Trim() != string.Empty))
            {
                return "bydate";
            }

            if ((eventsFilter.datefrom != null && eventsFilter.datefrom.Trim() != string.Empty)
                || (eventsFilter.dateto != null && eventsFilter.dateto.Trim() != string.Empty))
            {
                return "byuser";
            }

            return filterType;
        }

        private List<DataFactory.Filter> GetActionableEventsDataFilter(ActionableEventsFilter eventsFilter)
        {
            List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

            if (eventsFilter==null)
            {
                return filters;
            }

            // Check which filter type
            string filterType = GetActionableEventsDataFilterType(eventsFilter);

            string fileFieldName = string.Empty;
            string dateFieldName = string.Empty;
            string userFieldName = string.Empty;

            switch(filterType)
            {
                case "byfile":
                    fileFieldName = "PartitionKey";
                    dateFieldName = "CreationTime";
                    userFieldName = "UserId";
                    break;
                case "bydate":
                    fileFieldName = "RowKey";
                    dateFieldName = "PartitionKey";
                    userFieldName = "UserId";
                    break;
                case "byuser":
                    fileFieldName = "RowKey";
                    dateFieldName = "CreationTime";
                    userFieldName = "PartitionKey";
                    break;
                default:
                    break;
            }

            // Check if a file path has been provided
            if (eventsFilter.filepath != null && eventsFilter.filepath.Trim() != string.Empty)
            {
                DataFactory.Filter fileFilter = new DataFactory.Filter(fileFieldName, eventsFilter.filepath.Trim(), "ge");
                filters.Add(fileFilter);
                fileFilter = new DataFactory.Filter(fileFieldName, Utils.CleanTableKey(Utils.GetLessThanFilter(eventsFilter.filepath.Trim())), "lt");
                filters.Add(fileFilter);
            }

            // Check if both date ranges have been provided
            if ((eventsFilter.datefrom != null && eventsFilter.datefrom.Trim() != string.Empty)
                && (eventsFilter.dateto != null && eventsFilter.dateto.Trim() != string.Empty))
            {
                // Check if the dates are the same
                if (eventsFilter.datefrom.Trim() == eventsFilter.dateto.Trim())
                {
                    // Set an equal filter for the date
                    DataFactory.Filter date = new DataFactory.Filter(dateFieldName, Utils.CleanTableKey(eventsFilter.datefrom.Trim()), "ge");
                    filters.Add(date);
                    DataFactory.Filter toDate = new DataFactory.Filter(dateFieldName, Utils.GetLessThanFilter(Utils.CleanTableKey(eventsFilter.dateto.Trim())), "lt");
                    filters.Add(toDate);
                }
                else
                {
                    // Set a range filter
                    DataFactory.Filter fromDate = new DataFactory.Filter(dateFieldName, Utils.CleanTableKey(eventsFilter.datefrom.Trim()), "ge");
                    filters.Add(fromDate);
                    DataFactory.Filter toDate = new DataFactory.Filter(dateFieldName, Utils.CleanTableKey(eventsFilter.dateto.Trim()), "le");
                    filters.Add(toDate);
                }
            }
            // Check if either date ranges have been provided
            else
            {
                // Check if From date is set
                if (eventsFilter.datefrom != null && eventsFilter.datefrom.Trim() != string.Empty)
                {
                    // Only set a ge filter
                    DataFactory.Filter fromDate = new DataFactory.Filter(dateFieldName, Utils.CleanTableKey(eventsFilter.datefrom.Trim()), "ge");
                    filters.Add(fromDate);
                }
                // Check if To date is set
                if (eventsFilter.dateto != null && eventsFilter.dateto.Trim() != string.Empty)
                {
                    // Only set a ge filter
                    DataFactory.Filter toDate = new DataFactory.Filter(dateFieldName, Utils.CleanTableKey(eventsFilter.dateto.Trim()), "le");
                    filters.Add(toDate);
                }
            }

            foreach (string userid in eventsFilter.userids)
            {
                if (userid.Trim()!=string.Empty)
                {
                    DataFactory.Filter userFilter = new DataFactory.Filter(userFieldName, Utils.CleanTableKey(userid.Trim()), "eq");
                    filters.Add(userFilter);
                }
            }

            foreach (string operation in eventsFilter.operations)
            {
                if (operation.Trim()!=string.Empty)
                {
                    DataFactory.Filter opFilter = new DataFactory.Filter("Operation", operation, "eq");
                    filters.Add(opFilter);
                }
            }

            foreach (string workload in eventsFilter.workloads)
            {
                if (workload.Trim() != string.Empty)
                {
                    DataFactory.Filter wlFilter = new DataFactory.Filter("Workload", workload, "eq");
                    filters.Add(wlFilter);
                }
            }

            return filters;
        }

        [HttpGet("actionable/user/filter", Name = "GetActionableEventsByUser")]
        public IActionResult GetActionableEventsByUser([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            int pageCount = 0;
            string thisPageId = string.Empty;
            string nextPageId = string.Empty;
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetActionableEvents");

                // Get filter requests
                ActionableEventsFilter userFilter = new ActionableEventsFilter();
                if (filter!=null && filter!=string.Empty)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);
                    userFilter = JsonConvert.DeserializeObject<ActionableEventsFilter>(filterDecoded);
                }

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                DataFactory.DataConfig datacfg = Utils.GetDataConfig();

                List<POCO.O365.AuditLogEntry> logEntries = new List<POCO.O365.AuditLogEntry>();

                // Create the userid filter
                List<DataFactory.Filter> filters = GetActionableEventsDataFilter(userFilter);

                // Check if datapaging tokens are set
                if (dataPaging != null && dataPaging.thisPageId != null && dataPaging.thisPageId != string.Empty)
                {
                    thisPageId = dataPaging.thisPageId;
                }

                // Get the data
                int rowsToRequest = Utils.GetMaxRows() - logEntries.Count;
                logEntries = DataFactory.O365.AuditLog.GetActionableEvents(datacfg, "byuser", filters, thisPageId, rowsToRequest, out nextPageId);

                // Sort the data by descending date
                logEntries.Sort((x, y) => DateTime.Compare(y.CreationDateTime, x.CreationDateTime));

                // Set the page information
                PagedData paged = new PagedData();
                paged.data = logEntries;
                paged.totalRecords = logEntries.Count;
                paged.nextPageId = nextPageId;

                entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);

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

        [HttpGet("actionable/date/filter", Name = "GetActionableEventsByDate")]
        public IActionResult GetActionableEventsByDate([FromQuery] string filter, [FromHeader] string CPDataPaging)
        {
            int pageCount = 0;
            string thisPageId = string.Empty;
            string nextPageId = string.Empty;
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetActionableEvents");

                // Get filter requests
                ActionableEventsFilter eventsFilter = new ActionableEventsFilter();
                if (filter != null && filter != string.Empty)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);
                    eventsFilter = JsonConvert.DeserializeObject<ActionableEventsFilter>(filterDecoded);
                }

                // Validate the date entries
                if (eventsFilter.datefrom.Trim()==string.Empty && eventsFilter.dateto.Trim()==string.Empty)
                {
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Get the filters
                List<DataFactory.Filter> filters = GetActionableEventsDataFilter(eventsFilter);

                // Get any Paging data requests
                POCO.DataPaging dataPaging = new POCO.DataPaging();
                if (CPDataPaging != null && CPDataPaging != string.Empty)
                {
                    _logger.LogDebug("Deserializing datapaging of length: " + CPDataPaging.Length);
                    string pagingDecoded = System.Net.WebUtility.HtmlDecode(CPDataPaging);
                    pagingDecoded = System.Net.WebUtility.UrlDecode(pagingDecoded);
                    dataPaging = JsonConvert.DeserializeObject<POCO.DataPaging>(pagingDecoded);
                }

                // Load the data
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.O365.AuditLogEntry> logEntries = new List<POCO.O365.AuditLogEntry>();

                // Check for data page request token
                if (dataPaging != null && dataPaging.thisPageId != null && dataPaging.thisPageId != string.Empty)
                {
                    thisPageId = dataPaging.thisPageId;
                }

                // Get the data
                int rowsToRequest = Utils.GetMaxRows() - logEntries.Count;
                logEntries = DataFactory.O365.AuditLog.GetActionableEvents(datacfg, "bydate", filters, thisPageId, rowsToRequest, out nextPageId);

                // Sort the data by descending date
                logEntries.Sort((x, y) => DateTime.Compare(y.CreationDateTime, x.CreationDateTime));

                // Check if a data page has been specified
                PagedData paged = new PagedData();
                paged.data = logEntries;
                paged.totalRecords = logEntries.Count;
                paged.nextPageId = nextPageId;

                entityAsJson = JsonConvert.SerializeObject(paged, Formatting.Indented);

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

    }



}

    class SensitiveFilter
    {
        public SensitiveFilter()
        {
            types = new List<string>();
            items = new List<SensitiveItemFilter>();
        }
        public List<string> types;
        public List<SensitiveItemFilter> items;
    }

    class SensitiveItemFilter
    {
        public string systemuri { get; set; }
        public string itemuri { get; set; }
    }

class FileExceptionFilter
{
    public FileExceptionFilter()
    {
    }
    public string systemuri;
    public string exceptiontype;
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
        public string itemuri { get; set; }
    }

    //internal class AuditLogEntry : TableEntity
    //{
    //    public AuditLogEntry() { }

    //    //		"CreationTime": "2018-06-03T05:07:28",
    //    public string CreationTime { get; set; }
    //    //        "Id": "0d315ba6-4fd8-471c-cff6-08d5c90fe764",
    //    [IgnoreProperty]
    //    public DateTime CreationDateTime {
    //        get
    //        {
    //            DateTime creationDateTime = Utils.AzureTableMinDateTime;
    //            if (this.CreationTime!=null && this.CreationTime!="")
    //            {
    //                // Check if the date will parse
    //                try
    //                {
    //                    bool dateValid = DateTime.TryParse(this.CreationTime, out creationDateTime);
    //                }
    //                catch(Exception exDateConversion)
    //                {
    //                    creationDateTime = Utils.AzureTableMinDateTime;
    //                }
    //            }
    //            return creationDateTime;
    //        }
    //    } 
    //    public string Id { get; set; }
    //    //        "Operation": "FileModified",
    //    public string Operation { get; set; }
    //    //        "OrganizationId": "e1e984d0-afc0-4573-adda-3d327fd23391",
    //    public string OrganizationId { get; set; }
    //    //        "RecordType": 6,
    //    public int RecordType { get; set; }
    //    //public string ResultStatus { get; set; }
    //    //        "UserKey": "i:0h.f|membership|10037ffe8358cd7e@live.com",
    //    public string UserKey { get; set; }
    //    //        "UserType": 0,
    //    public int UserType { get; set; }
    //    //        "Version": 1,
    //    public int Version { get; set; }
    //    //        "Workload": "SharePoint",
    //    public string Workload { get; set; }
    //    //        "ClientIP": "124.171.97.46",
    //    public string ClientIP { get; set; }
    //    //        "ObjectId": "https:\\/\\/stlpconsulting.sharepoint.com\\/TeamSite\\/2 STLP Clients1\\/D2.4 - Information Architecture.docx",
    //    public string ObjectId { get; set; }
    //    //        "UserId": "rachaelg@stlpconsulting.com",
    //    public string UserId { get; set; }
    //    //        "CorrelationId": "7d636d9e-d0ec-5000-cd1d-11b899989d9d",
    //    public string CorrelationId { get; set; }
    //    //        "EventSource": "SharePoint",
    //    public string EventSource { get; set; }
    //    //        "ItemType": "File",
    //    public string ItemType { get; set; }
    //    //        "ListId": "d94dd849-f473-45fd-bb49-5ff73757727d",
    //    public string ListId { get; set; }
    //    //        "ListItemUniqueId": "dc266cf1-ad04-457b-87d4-3011cee8d482",
    //    public string ListItemUniqueId { get; set; }
    //    //        "Site": "e368b916-c1f0-459b-a8a9-317240e9e504",
    //    public string Site { get; set; }
    //    //        "UserAgent": "Microsoft Office Word 2014",
    //    public string UserAgent { get; set; }
    //    //        "WebId": "fd66f1ac-9a7d-4b37-b72c-1e7ebbe2d623",
    //    public string WebId { get; set; }
    //    //        "SourceFileExtension": "docx",
    //    public string SourceFileExtension { get; set; }
    //    //        "SiteUrl": "https:\\/\\/stlpconsulting.sharepoint.com\\/",
    //    public string SiteUrl { get; set; }
    //    //        "SourceFileName": "D2.4 - Information Architecture.docx",
    //    public string SourceFileName { get; set; }
    //    //        "SourceRelativeUrl": "TeamSite\\/2 STLP Clients1"
    //    public string SourceRelativeUrl { get; set; }


    //    //public int AzureActiveDirectoryEventType { get; set; }
    //    //public AuditLogEntryExtendedProperties ExtendedProperties { get; set; }
    //    //public string Client { get; set; }
    //    //public int LoginStatus { get; set; }
    //    //public string UserDomain { get; set; }

    //}

    //public class AuditLogEntryExtendedProperties
    //{
    //    public string Name { get; set; }
    //    public string Value { get; set; }

    //}
