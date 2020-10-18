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
    [Route("status")]
    [Authorize]
    public class StatusController : Controller
    {


        public IConfiguration Configuration { get; set; }
        ILogger<StatusController> _logger;

        public StatusController(IConfiguration config, ILogger<StatusController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        [HttpGet("lastaction", Name = "GetLastActionLog")]
        public IActionResult GetLastActionLog()
        {
            // Create the table if it doesn't exist. 
            //log.Info("Getting table reference");
            CloudTable table = Utils.GetCloudTable("stlploglastaction", _logger);

            //// Check for keys
            string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "lastaction");
            //string rkFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "fileslastupdated");
            //string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter);

            TableQuery<LogLastAction> query = new TableQuery<LogLastAction>().Where(pkFilter);
            List<LogLastAction> allLogs = new List<LogLastAction>();

            TableContinuationToken token = null;

            var runningQuery = new TableQuery<LogLastAction>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            do
            {
                runningQuery.TakeCount = query.TakeCount - allLogs.Count;

                Task<TableQuerySegment<LogLastAction>> tSeg = table.ExecuteQuerySegmentedAsync<LogLastAction>(runningQuery, token);
                tSeg.Wait();
                token = tSeg.Result.ContinuationToken;
                allLogs.AddRange(tSeg.Result);

            } while (token != null && (query.TakeCount == null || allLogs.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            // Check if a single entry has been returned
            string entityAsJson = JsonConvert.SerializeObject(allLogs);
            

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }
    }

    public class LogLastAction:TableEntity
    {
        public LogLastAction() { }
        public string EventName { get; set; }
        public string LastAction { get; set; }
    }
}