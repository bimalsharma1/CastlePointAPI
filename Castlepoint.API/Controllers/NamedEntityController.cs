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
    [Route("namedentity", Name = "NamedEntity")]
    [Authorize]
    public class NamedEntityController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<NamedEntityController> _logger;

        public NamedEntityController(IConfiguration config, ILogger<NamedEntityController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        class NamedEntityFilter
        {
            public NamedEntityFilter()
            {
                records = new List<string>();
                recordassociations = new List<string>();
                namedentities = new List<string>();
            }
            public List<string> records;
            public List<string> recordassociations;
            public List<string> namedentities;
        }

        class NamedEntity:TableEntity
        {
            public NamedEntity() { }

            public string OriginalText { get; set; }
            public string Type { get; set; }
        }

        [HttpGet("filter", Name = "GetNamedEntityReverseByNamedEntityFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<POCO.RecordAssociationNamedEntityReverse> namedEntities = new List<POCO.RecordAssociationNamedEntityReverse>();
            try
            {

                _logger.LogInformation("CPAPI: Get By Named Entity Filter");

                // Deserialize the ontology filter
                NamedEntityFilter oFilter = new NamedEntityFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Named Entity filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<NamedEntityFilter>(filter);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                if (oFilter.namedentities.Count > 0)
                {
                    foreach(string nefilter in oFilter.namedentities)
                    {
                        string cleanKey = Utils.CleanTableKey(nefilter);
                        DataFactory.Filter pkFilterGE = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(cleanKey), "ge");
                        DataFactory.Filter pkFilterLT = new DataFactory.Filter("PartitionKey", Utils.GetLessThanFilter(cleanKey), "lt");
                        filters.Add(pkFilterGE);
                        filters.Add(pkFilterLT);
                    }
                }

                List<POCO.RecordAssociationNamedEntityReverse> namedentities = new List<POCO.RecordAssociationNamedEntityReverse>();
                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                namedentities = DataFactory.NamedEntity.GetReverseNamedEntities(dataConfig, filters);


                //CloudTable table = Utils.GetCloudTableNoCreate("stlprecordassociationnamedentityreverse", _logger);

                //// Create a default query
                //TableQuery<NamedEntity> query = new TableQuery<NamedEntity>();
                //if (oFilter.namedentities.Count > 0)
                //{
                //    string combinedFilter = "";
                //    foreach (string rif in oFilter.namedentities)
                //    {
                //        string cleanFilterPKey = Utils.CleanTableKey(rif);
                //        string cleanFilterRKey = "";    // Utils.CleanTableKey(rif);

                //        string pkqueryStart = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, cleanFilterPKey);
                //        string pkqueryEnd = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterPKey));

                //        string pkqueryCombined = TableQuery.CombineFilters(pkqueryStart, TableOperators.And, pkqueryEnd);

                //        if (combinedFilter != "")
                //        {
                //            combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.And, pkqueryCombined);
                //        }
                //        else
                //        {
                //            combinedFilter = pkqueryCombined;
                //        }

                //        // Check if an item key has been provided
                //        if (cleanFilterRKey != "")
                //        {
                //            string rkqueryStart = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, cleanFilterRKey);
                //            string rkqueryEnd = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Utils.GetLessThanFilter(cleanFilterRKey));

                //            string rkqueryCombined = TableQuery.CombineFilters(rkqueryStart, TableOperators.And, rkqueryEnd);

                //            if (combinedFilter != "")
                //            {
                //                combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.Or, rkqueryCombined);
                //            }
                //            else
                //            {
                //                combinedFilter = rkqueryCombined;
                //            }
                //        }
                //    }
                //    // Create final combined query
                //    query = new TableQuery<NamedEntity>().Where(combinedFilter);
                //}
                //List<NamedEntity> namedEntityEntities = new List<NamedEntity>();
                //TableContinuationToken token = null;

                //var runningQuery = new TableQuery<NamedEntity>()
                //{
                //    FilterString = query.FilterString,
                //    SelectColumns = query.SelectColumns
                //};

                //do
                //{
                //    runningQuery.TakeCount = query.TakeCount - namedEntityEntities.Count;

                //    Task<TableQuerySegment<NamedEntity>> tSeg = table.ExecuteQuerySegmentedAsync<NamedEntity>(runningQuery, token);
                //    tSeg.Wait();
                //    token = tSeg.Result.ContinuationToken;
                //    namedEntityEntities.AddRange(tSeg.Result);

                //} while (token != null && (query.TakeCount == null || namedEntityEntities.Count < query.TakeCount.Value) && namedEntityEntities.Count < 1000);    //!ct.IsCancellationRequested &&


                //namedEntityEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // Return only distinct keyphrases
                List<string> namedEntityKeys = new List<string>();
                foreach (POCO.RecordAssociationNamedEntityReverse ne in namedentities)
                {
                    if (!namedEntityKeys.Contains(ne.PartitionKey))
                    {
                        // Add the entity to the output
                        // and the key
                        namedEntities.Add(ne);
                        namedEntityKeys.Add(ne.PartitionKey);
                    }
                }
                namedEntities.Sort((x, y) => String.Compare(x.PartitionKey, y.PartitionKey));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(namedEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Named Entity GET BY FILTER exception: " + ex.Message;
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

        class RecordAssociationFilter
        {
            public RecordAssociationFilter()
            {
                recordassociations = new List<string>();
            }
            public List<string> recordassociations;
        }

        [HttpGet("recordassociationfilter", Name = "GetNamedEntityByRecordAssociationFilter")]
        public IActionResult GetByRecordAssociationFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            List<POCO.RecordAssociationNamedEntity> namedEntities = new List<POCO.RecordAssociationNamedEntity>();
            try
            {

                _logger.LogInformation("CPAPI: Get By Record Association Filter");

                // Deserialize the ontology filter
                RecordAssociationFilter oFilter = new RecordAssociationFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing Record filter of length: " + filter.Length);

                    string filterDecoded = System.Net.WebUtility.HtmlDecode(filter);
                    filterDecoded = System.Net.WebUtility.UrlDecode(filterDecoded);

                    oFilter = JsonConvert.DeserializeObject<RecordAssociationFilter>(filterDecoded);
                }

                // Check if a filter has been supplied
                if (oFilter.recordassociations.Count==0)
                {
                    // Nothing to filter by - return invalid filter
                    return StatusCode((int)System.Net.HttpStatusCode.NoContent);
                }

                // TODO support multiple record assoc filters
                POCO.RecordAssociation recassoc = new POCO.RecordAssociation();
                recassoc.RowKey = oFilter.recordassociations[0];

                //CloudTable table = Utils.GetCloudTableNoCreate("stlprecordassociationnamedentity", _logger);

                //// Create a default query
                //TableQuery<NamedEntity> query = new TableQuery<NamedEntity>();
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
                //query = new TableQuery<NamedEntity>().Where(combinedFilter);

                //List<NamedEntity> namedEntityEntities = new List<NamedEntity>();
                //TableContinuationToken token = null;

                //var runningQuery = new TableQuery<NamedEntity>()
                //{
                //    FilterString = query.FilterString,
                //    SelectColumns = query.SelectColumns
                //};

                //do
                //{
                //    runningQuery.TakeCount = query.TakeCount - namedEntityEntities.Count;

                //    Task<TableQuerySegment<NamedEntity>> tSeg = table.ExecuteQuerySegmentedAsync<NamedEntity>(runningQuery, token);
                //    tSeg.Wait();
                //    token = tSeg.Result.ContinuationToken;
                //    namedEntityEntities.AddRange(tSeg.Result);

                //} while (token != null && (query.TakeCount == null || namedEntityEntities.Count < query.TakeCount.Value) && namedEntityEntities.Count < 1000);    //!ct.IsCancellationRequested &&

                //// Return only distinct entrys
                //List<string> namedEntityKeys = new List<string>();
                //foreach (NamedEntity ne in namedEntityEntities)
                //{
                //    if (!namedEntityKeys.Contains(ne.RowKey))
                //    {
                //        // Add the entity to the output
                //        // and the key
                //        namedEntities.Add(ne);
                //        namedEntityKeys.Add(ne.RowKey);
                //    }
                //}

                namedEntities = DataFactory.RecordAssociation.GetNamedEntities(Utils.GetDataConfig(), recassoc);

                namedEntities.Sort((x, y) => String.Compare(x.RowKey, y.RowKey));

                // Serialize
                entityAsJson = JsonConvert.SerializeObject(namedEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Named Entity GET BY RECORD FILTER exception: " + ex.Message;
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
