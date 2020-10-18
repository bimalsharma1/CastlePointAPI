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

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("ontology/explore", Name = "OntologyExplorer")]
    [Authorize]
    public class OntologyExplorerController : ControllerBase
    {
        public IConfiguration Configuration { get; set; }
        ILogger<OntologyExplorerController> _logger;
        private readonly IAppCache _cacheOntologyExplorer;

        public OntologyExplorerController(IConfiguration config, ILogger<OntologyExplorerController> logger)
        {
            Configuration = config;
            _logger = logger;
            _cacheOntologyExplorer = new CachingService();
        }

        private List<POCO.OntologyTermMatchResults> GetSummaryFromDatabase(string ontologyUri)
        {
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();

            POCO.OntologyAssigned oassigned = new POCO.OntologyAssigned();
            oassigned.RowKey = ontologyUri;

            List<POCO.OntologyTermMatchResults> matchsummaries = new List<POCO.OntologyTermMatchResults>();
            matchsummaries = DataFactory.Ontology.GetOntologyMatchSummary(Utils.GetDataConfig(), oassigned);

            return matchsummaries;

        }

        [HttpGet("summary/filter", Name = "ExploreSummaryByOntologyFilter")]
        public IActionResult GetSummaryByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the ontology filter
                OntologyExplorerFilter oFilter = new OntologyExplorerFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing ontology filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<OntologyExplorerFilter>(filter);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

                // Check for filters
                if (oFilter.ontology.Count > 0)
                {

                    Func<List<POCO.OntologyTermMatchResults>> dataGetter = () => GetSummaryFromDatabase(oFilter.ontology[0]);

                    var matchsummaries = _cacheOntologyExplorer.GetOrAdd(oFilter.ontology[0], dataGetter);

                    // Sort by ParentTerm then by Term
                    matchsummaries.Sort((x, y) => String.Compare(x.ParentTerm + "|" + x.Term, y.ParentTerm + "|" + y.Term));

                    entityAsJson = JsonConvert.SerializeObject(matchsummaries, Formatting.Indented);
                }

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Ontology GET exception: " + ex.Message;
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

        [HttpGet("filter", Name = "ExploreByOntologyFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the ontology filter
                OntologyExplorerFilter oFilter = new OntologyExplorerFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing ontology filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<OntologyExplorerFilter>(filter);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();

                // Check for filters
                if (oFilter.ontology.Count > 0)
                {
                    string combinedFilter = "";
                    foreach (string of in oFilter.ontology)
                    {
                        string cleanFilterPKey = Utils.CleanTableKey(of);
                        DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", of, "eq");
                        filters.Add(pkfilt);
                    }
                }

                List<POCO.OntologyMatchSummary> matchsummaries = new List<POCO.OntologyMatchSummary>();
                matchsummaries = DataFactory.Ontology.GetOntologyMatchSummary(Utils.GetDataConfig(), filters);

                entityAsJson = JsonConvert.SerializeObject(matchsummaries, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Ontology GET exception: " + ex.Message;
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

    class OntologyExplorerFilter
    {
        public OntologyExplorerFilter()
        {
            this.ontology = new List<string>();
        }
        public List<string> ontology { get; set; }
        public string output_type { get; set; }
    }
}
