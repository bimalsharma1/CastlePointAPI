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
    [Route("log", Name = "Log")]
    [Authorize]
    public class LogController:Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<LogController> _logger;

        public LogController(IConfiguration config, ILogger<LogController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        private class LogEventProcessingFilter
        {
            public string YearMonth { get; set; }
            public string ItemUri { get; set; }
            public List<string> Event { get; set; }
            public List<string> MIMEType { get; set; }
            public int NumBytesStart { get; set; }
            public int NumBytesEnd { get; set; }
        }

        [HttpGet("exception/file/filter", Name = "GetByFileProcessExceptionFilter")]
        public IActionResult GetByFileProcessExceptionFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetByFileProcessExceptionFilter");

                // Deserialize the data filter
                FileExceptionFilter oFilter = new FileExceptionFilter();
                if (filter != null && filter.Length > 0)
                {
                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);

                    _logger.LogDebug("Deserializing filter of length: " + filterDecoded.Length);
                    oFilter = JsonConvert.DeserializeObject<FileExceptionFilter>(filterDecoded);
                }

                // Validate the filter
                if (string.IsNullOrEmpty(oFilter.systemuri)
                    || string.IsNullOrEmpty(oFilter.exceptiontype))
                {
                    _logger.LogWarning("GetByFileProcessExceptionFilter: systemuri or exceptiontype is blank");
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }
                //Guid systemGuid = Guid.NewGuid();
                //if (!Guid.TryParse(oFilter.systemid, out systemGuid))
                //{
                //    _logger.LogWarning("GetByFileProcessExceptionFilter: systemid is invalid - expected Guid");
                //    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                //}
                if (oFilter.exceptiontype.Length > 50)
                {
                    _logger.LogWarning("GetByFileProcessExceptionFilter: exception type is invalid - too long");
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Create system filter
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter f1 = new DataFactory.Filter("PartitionKey", oFilter.systemuri, "eq");
                filters.Add(f1);

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();

                List<POCO.System> systems = DataFactory.System.GetSystems(dataConfig, filters);
                if (systems.Count != 1)
                {
                    _logger.LogWarning("GetByFileProcessExceptionFilter: system count expected 1 instead of: " + systems.Count.ToString());
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Get the data
                List<POCO.FileProcessException> fileexceptions = DataFactory.Log.GetFileProcessException(dataConfig, systems[0], oFilter.exceptiontype);

                // Convert to JSON and return
                entityAsJson = JsonConvert.SerializeObject(fileexceptions);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "GetByFileProcessExceptionFilter: exception: " + ex.Message;
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


        [HttpGet("eventprocessing/filter", Name = "GetLogEventProcessingByFilter")]
        public IActionResult GetLogEventProcessingByFilter([FromHeader] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetLogEventProcessingByFilter");

                // Deserialize the ontology filter
                LogEventProcessingFilter oFilter = new LogEventProcessingFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<LogEventProcessingFilter>(filter);
                }


                // Validate the filter data
                if (oFilter == null)
                {
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

                // Check if an Event has been supplied
                if (oFilter.Event != null && oFilter.Event.Count==0)
                {
                    foreach(string e in oFilter.Event)
                    {
                        DataFactory.Filter eventfilter = new DataFactory.Filter("Event", e, "eq");
                        filters.Add(eventfilter);
                    }
                }

                // Call the data factory
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.LogEventProcessingTime> recordKeyPhrases = DataFactory.Log.GetProcessingTime(dataConfig, filters, oFilter.YearMonth);

                entityAsJson = JsonConvert.SerializeObject(recordKeyPhrases, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Log EventProcessing GET exception: " + ex.Message;
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
