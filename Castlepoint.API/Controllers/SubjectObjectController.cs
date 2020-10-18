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
    [Route("subjectobject", Name = "SubjectObject")]
    [Authorize]
    public class SubjectObjectController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<SubjectObjectController> _logger;

        public SubjectObjectController(IConfiguration config, ILogger<SubjectObjectController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        class SubjectObjectEntity :TableEntity
        {
            public SubjectObjectEntity() { }
            public string Object { get; set; }
            public string Subject { get; set; }
            public string Relationship { get; set; }
        }

        class RecordAssociationFilter
        {
            public RecordAssociationFilter()
            {
                recordassociations = new List<string>();
            }
            public List<string> recordassociations;
        }

        [HttpGet("recordassociationfilter", Name = "GetSubjectObjectByRecordAssociationFilter")]
        public IActionResult GetByRecordAssociationFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<POCO.RecordAssociationSubjectObject> subjectObjectEntities = new List<POCO.RecordAssociationSubjectObject>();
            try
            {

                _logger.LogInformation("CPAPI: Get By Record Association Filter");

                // Deserialize the ontology filter
                Controllers.RecordAssociationFilter oFilter = new Controllers.RecordAssociationFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record Association filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<Controllers.RecordAssociationFilter>(filter);
                }

                // Check if a filter has been supplied
                if (oFilter.recordassociations.Count == 0)
                {
                    // Nothing to filter by - return invalid filter
                    return StatusCode((int)System.Net.HttpStatusCode.NoContent);
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
                    _logger.LogInformation("RecordKeyPhraseFilter query BLANK");
                }

                DataFactory.DataConfig dataCfg = Utils.GetDataConfig();

                List<POCO.RecordAssociationSubjectObject> subjectobject = DataFactory.Record.GetRecordAssociationSubjectObject(dataCfg, filters);

                //CloudTable table = Utils.GetCloudTableNoCreate("stlprecordassociationsubjectobject", _logger);

                //// Create a default query
                //TableQuery<SubjectObjectEntity> query = new TableQuery<SubjectObjectEntity>();
                //string combinedFilter = "";
                //if (oFilter.recordassociations.Count > 0)
                //{
                //    foreach (string rafilter in oFilter.recordassociations)
                //    {
                //        string cleanFilterPKey = Utils.CleanTableKey(rafilter);

                //        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);

                //        if (combinedFilter != "")
                //        {
                //            combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.Or, pkquery);
                //        }
                //        else
                //        {
                //            combinedFilter = pkquery;
                //        }
                //    }
                //}

                //// Create final combined query
                //query = new TableQuery<SubjectObjectEntity>().Where(combinedFilter);

                //List<SubjectObjectEntity> SubjectObjectEntityEntities = new List<SubjectObjectEntity>();
                //TableContinuationToken token = null;

                //var runningQuery = new TableQuery<SubjectObjectEntity>()
                //{
                //    FilterString = query.FilterString,
                //    SelectColumns = query.SelectColumns
                //};

                //do
                //{
                //    runningQuery.TakeCount = query.TakeCount - SubjectObjectEntityEntities.Count;

                //    Task<TableQuerySegment<SubjectObjectEntity>> tSeg = table.ExecuteQuerySegmentedAsync<SubjectObjectEntity>(runningQuery, token);
                //    tSeg.Wait();
                //    token = tSeg.Result.ContinuationToken;
                //    SubjectObjectEntityEntities.AddRange(tSeg.Result);

                //} while (token != null && (query.TakeCount == null || SubjectObjectEntityEntities.Count < query.TakeCount.Value) && SubjectObjectEntityEntities.Count < 1000);    //!ct.IsCancellationRequested &&

                // Return only distinct entrys
                List<string> SubjectObjectEntityKeys = new List<string>();
                foreach (POCO.RecordAssociationSubjectObject ne in subjectobject)
                {
                    if (!SubjectObjectEntityKeys.Contains(ne.RowKey))
                    {
                        // Add the entity to the output
                        // and the key
                        subjectObjectEntities.Add(ne);
                        SubjectObjectEntityKeys.Add(ne.RowKey);
                    }
                }
                subjectObjectEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(subjectObjectEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Subject Object GET BY RECORD FILTER exception: " + ex.Message;
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
