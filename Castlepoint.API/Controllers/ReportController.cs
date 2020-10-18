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
    [Route("report", Name = "Report")]
    [Authorize]
    public class ReportController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<ReportController> _logger;

        public ReportController(IConfiguration config, ILogger<ReportController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        [HttpGet("exception/file/filter", Name = "GetByFileExceptionFilter")]
        public IActionResult GetByFileExceptionFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetByFileExceptionFilter");

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
                    _logger.LogWarning("GetByFileExceptionFilter: systemuri or exceptiontype is blank");
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }
                //Guid systemGuid = Guid.NewGuid();
                //if (!Guid.TryParse(oFilter.systemid, out systemGuid))
                //{
                //    _logger.LogWarning("GetByFileExceptionFilter: systemid is invalid - expected Guid");
                //    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                //}
                if (oFilter.exceptiontype.Length>50)
                {
                    _logger.LogWarning("GetByFileExceptionFilter: exception type is invalid - too long");
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Create system filter
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                DataFactory.Filter f1 = new DataFactory.Filter("PartitionKey", oFilter.systemuri, "eq");

                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();

                List<POCO.System> systems = DataFactory.System.GetSystems(dataConfig, filters);
                if (systems.Count!=1)
                {
                    _logger.LogWarning("GetByFileExceptionFilter: system count expected 1 instead of: " + systems.Count.ToString());
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                // Get the data
                List<POCO.FileProcessException> fileexceptions = DataFactory.Log.GetFileProcessException(dataConfig, systems[0], oFilter.exceptiontype);

                // Create a safe file name
                string safeFileName = Path.GetInvalidFileNameChars().Aggregate("fileexceptions-" + oFilter.exceptiontype + ".xlsx", (current, c) => current.Replace(c, '-'));

                return File(Utils.CreateExcelFile(fileexceptions.ToList<object>()), "application/octet-stream", safeFileName);

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

        }
    }


}
