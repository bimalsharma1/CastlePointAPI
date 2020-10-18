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

namespace Castlepoint.REST
{
    [Produces("application/json")]
    [Route("sentencecontrolrecord", Name = "SentenceControlRecord")]
    [Authorize]
    public class SentenceControlRecordController : Controller
    {

        public IConfiguration Configuration { get; set; }
        ILogger<SentenceControlRecordController> _logger;

        public SentenceControlRecordController(IConfiguration config, ILogger<SentenceControlRecordController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        [HttpGet("", Name = "GetSentenceControlRecords")]
        public IActionResult Get()
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetSentenceControlRecords");

                // Create a query
                TableQuery<SentenceControlRecordEntry> query = new TableQuery<SentenceControlRecordEntry>();

                // Get the sentence control record data 
                List<SentenceControlRecordEntry> scrEntities = new List<SentenceControlRecordEntry>();

                // Get the cloud table and check if it exists
                CloudTable table = Utils.GetCloudTableNoCreate("stlpsentencecontrolrecord", _logger);

                Task<bool> t = table.ExistsAsync();
                t.Wait();
                if (t.Result)
                {
                    // Run the query on the table
                    TableContinuationToken token = null;

                    var runningQuery = new TableQuery<SentenceControlRecordEntry>()
                    {
                        FilterString = query.FilterString,
                        SelectColumns = query.SelectColumns
                    };

                    do
                    {
                        runningQuery.TakeCount = query.TakeCount - scrEntities.Count;

                        Task<TableQuerySegment<SentenceControlRecordEntry>> tSeg = table.ExecuteQuerySegmentedAsync<SentenceControlRecordEntry>(runningQuery, token);
                        tSeg.Wait();
                        token = tSeg.Result.ContinuationToken;
                        scrEntities.AddRange(tSeg.Result);

                    } while (token != null && (query.TakeCount == null || scrEntities.Count < query.TakeCount.Value) && scrEntities.Count < 100);    //!ct.IsCancellationRequested &&

                }

                // Sort the data by descending date
                scrEntities.Sort((x, y) => DateTimeOffset.Compare(y.Timestamp, x.Timestamp));

                entityAsJson = JsonConvert.SerializeObject(scrEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Sentence Control Record GET exception: " + ex.Message;
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


        internal class SentenceControlRecordEntry : TableEntity
        {
            public SentenceControlRecordEntry() { }

            public string Action { get; set; }
            public DateTime ActionDate { get; set; }
            public string AuthorisingActionOfficer { get; set; }
            public string Comments { get; set; }
            public string DestructionMethod { get; set; }
            public string RecordUri { get; set; }

        }
    }

}
