using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

// Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Castlepoint.REST.Controllers
{

    [Produces("application/json")]
    [Route("ontology", Name = "Ontology")]
    [Authorize]
    public class OntologyController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<OntologyController> _logger;

        public OntologyController(IConfiguration config, ILogger<OntologyController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        // GET: api/Ontology
        [HttpGet]
        public IActionResult Get()
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Get all the ontologies
                List<POCO.Ontology> ontologies = DataFactory.Ontology.GetOntology(Utils.GetDataConfig(), new List<DataFactory.Filter>());
                ontologies.Sort((x, y) => String.Compare(x.Label, y.Label));

                entityAsJson = JsonConvert.SerializeObject(ontologies, Formatting.Indented);

            }
            catch(Exception ex)
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

        public class FilterMatchTerm
        {
            public string ontologyuri { get; set; }
            public string matchterm { get; set; }
        }
        public class FilterMatchTerms
        {
            public List<FilterMatchTerm> filterterms { get; set; }
        }

        [HttpPost("v2/match", Name = "GetByMatchTerm")]
        public IActionResult GetByMatchTerm([FromBody] FilterMatchTerms filter)
        {
            string entityAsJson;
            try
            {

                _logger.LogInformation("CPAPI: Get");

                List<POCO.OntologyTermMatch> ontologyMatches = new List<POCO.OntologyTermMatch>();

                DataFactory.DataConfig datacfg = Utils.GetDataConfig();

                foreach (FilterMatchTerm filterterm in filter.filterterms)
                {
                    List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                    string cleanFilterPKey = Utils.CleanTableKey(filterterm.ontologyuri);
                    DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                    filters.Add(pkfilt);
                    DataFactory.Filter termfilt = new DataFactory.Filter("TermRowKey", filterterm.matchterm, "eq");
                    filters.Add(termfilt);

                    // Filter on this term and add to our final list
                    List<POCO.OntologyTermMatch> matches = DataFactory.Ontology.GetOntologyTermMatches(datacfg, filters);
                    ontologyMatches.AddRange(matches);
                }


                entityAsJson = JsonConvert.SerializeObject(ontologyMatches, Formatting.Indented);

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
                return new  UnprocessableEntityObjectResult(exceptionMsg);
            }

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }

        [HttpGet("filter", Name = "GetByOntologyFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the ontology filter
                OntologyFilter oFilter = new OntologyFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing ontology filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<OntologyFilter>(filter);
                }

                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                foreach (string of in oFilter.ontology)
                {
                    string cleanFilterPKey = Utils.CleanTableKey(of);
                    DataFactory.Filter pkfilt = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                    filters.Add(pkfilt);
                }

                //CloudTable table = Utils.GetCloudTable("stlpontology", _logger);

                //// Create a default query
                //TableQuery<OntologyEntity> query = new TableQuery<OntologyEntity>();
                //if (oFilter.ontology.Count>0)
                //{
                //    string combinedFilter = "";
                //    foreach(string of in oFilter.ontology)
                //    {
                //        string cleanFilterPKey = Utils.CleanTableKey(of);
                //        string pkquery = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cleanFilterPKey);

                //        if (combinedFilter!="")
                //        {
                //            combinedFilter = TableQuery.CombineFilters(combinedFilter, TableOperators.Or, pkquery);
                //        }
                //        else
                //        {
                //            combinedFilter = pkquery;
                //        }
                //    }
                //    // Create final combined query
                //    query = new TableQuery<OntologyEntity>().Where(combinedFilter);
                //}
                //List<OntologyEntity> ontologyEntities = new List<OntologyEntity>();
                //TableContinuationToken token = null;

                //var runningQuery = new TableQuery<OntologyEntity>()
                //{
                //    FilterString = query.FilterString,
                //    SelectColumns = query.SelectColumns
                //};

                //do
                //{
                //    runningQuery.TakeCount = query.TakeCount - ontologyEntities.Count;

                //    Task<TableQuerySegment<OntologyEntity>> tSeg = table.ExecuteQuerySegmentedAsync<OntologyEntity>(runningQuery, token);
                //    tSeg.Wait();
                //    token = tSeg.Result.ContinuationToken;
                //    ontologyEntities.AddRange(tSeg.Result);

                //} while (token != null && (query.TakeCount == null || ontologyEntities.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.Ontology> ontologyEntities = DataFactory.Ontology.GetOntology(datacfg, filters);

                ontologyEntities.Sort((x, y) => String.Compare(x.Label, y.Label));

                entityAsJson = JsonConvert.SerializeObject(ontologyEntities, Formatting.Indented);

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

        // GET: api/Ontology/5
        //[HttpGet("{id}", Name = "Get")]
        //public string Get(int id)
        //{
        //if (ontology != null && ontology != "")
        //{
        //    string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, ontology);
        //    if (ontologyType != null && ontologyType != "")
        //    {
        //        string typeFilter = TableQuery.GenerateFilterCondition("Type", QueryComparisons.Equal, ontologyType);
        //        string combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, typeFilter);
        //        // Combined filter on partition key
        //        query = new TableQuery<OntologyEntity>().Where(combinedFilter);
        //    }
        //    else
        //    {
        //        // Single filter on partition key
        //        query = new TableQuery<OntologyEntity>().Where(pkFilter);
        //    }
        //}

        //    return "";
        //}

        // POST: api/Ontology
        [HttpPost]
        public void Post([FromBody]string value)
        {
        }
        
        // PUT: api/Ontology/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }
        
        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        /// <summary>
        /// Assigns one or more Ontologys to a System
        /// </summary>
        /// <param name="system"></param>
        /// <param name="ontologys"></param>
        /// <returns></returns>
        [HttpPost("system/assign", Name = "AssignOntologyToSystem")]
        public IActionResult AssignToSystem([FromHeader] string system, List<string> ontologys)
        {
            _logger.LogInformation("CPAPI: AssignOntologyToSystem");

            List<POCO.OntologyAssigned> newOnts = new List<POCO.OntologyAssigned>();

            // Validate the params
            if (string.IsNullOrEmpty(system))
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            if (ontologys == null || ontologys.Count == 0)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Assign each record authority to the system
            foreach (string ra in ontologys)
            {
                // Create a new OntologyAssigned entry
                POCO.OntologyAssigned ontAssigned = new POCO.OntologyAssigned();
                ontAssigned.PartitionKey= Utils.CleanTableKey(system);
                ontAssigned.RowKey= Utils.CleanTableKey(ra);

                POCO.OntologyAssigned addedOntology = AssignToSystem(ontAssigned);

                newOnts.Add(addedOntology);
            }

            // Check if any adds succeeded
            if (newOnts.Count > 0)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(newOnts);
                return result;
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        private POCO.OntologyAssigned AssignToSystem(POCO.OntologyAssigned ontAssigned)
        {
            bool isAddedOk = false;

            // Execute the insert operation. 
            //log.Verbose("Executing table operation");
            try
            {
                // Call the Add datafactory method
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                DataFactory.Ontology.AssignToSystem(datacfg, ontAssigned);
            }

            catch (Exception aex)
            {
                _logger.LogError("ERR exception: " + aex.Message);
            }

            isAddedOk = true;

            return ontAssigned;
        }

        /// <summary>
        /// Add a new system
        /// </summary>
        /// <param name="ontology"></param>
        /// <returns></returns>
        [HttpPost("add", Name = "OntologyAdd")]
        public IActionResult OntologyAdd([FromHeader] string ontology)
        {
            _logger.LogInformation("CPAPI: OntologyAdd");

            // Deserialize the system
            POCO.Ontology newOntology = new POCO.Ontology();
            if (ontology != null && ontology.Length > 0)
            {
                _logger.LogDebug("Deserializing Ontology of length: " + ontology.Length);
                newOntology = JsonConvert.DeserializeObject<POCO.Ontology>(ontology);
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Validate the system
            if (newOntology.PartitionKey == null || newOntology.PartitionKey == string.Empty)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            if (newOntology.Label == null || newOntology.Label == string.Empty)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Check if the System Id has been set
            if (newOntology.UniqueId == null)
            {
                // Set to a new Guid
                newOntology.UniqueId = Guid.NewGuid().ToString();
            }

            // Clean the table and row keys
            if (!newOntology.PartitionKey.EndsWith("/")) { newOntology.PartitionKey += "/"; }
            newOntology.PartitionKey = Utils.CleanTableKey(newOntology.PartitionKey);
            newOntology.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));

            POCO.Ontology addedOntology = AddOntology(newOntology);

            if (addedOntology != null)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(addedOntology);
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
        /// <param name="ontology"></param>
        /// <returns></returns>
        [HttpPost("v2/add", Name = "OntologyAddV2")]
        public IActionResult OntologyAddV2([FromBody] POCO.Ontology ontology)
        {
            _logger.LogInformation("CPAPI: OntologyAdd");

            // Deserialize the system
            if (ontology == null)
            {
                return BadRequest("Ontology data is invalid or empty");
            }

            // Validate the system
            if (string.IsNullOrEmpty(ontology.PartitionKey))
            {
                return BadRequest("PartitionKey is invalid");
            }

            if (string.IsNullOrEmpty(ontology.Label))
            {
                return BadRequest("Ontology is invalid");
            }

            // Check if the System Id has been set
            if (ontology.UniqueId == null)
            {
                // Set to a new Guid
                ontology.UniqueId = Guid.NewGuid().ToString();
            }

            // Clean the table and row keys
            if (!ontology.PartitionKey.EndsWith("/")) { ontology.PartitionKey += "/"; }
            ontology.PartitionKey = Utils.CleanTableKey(ontology.PartitionKey);
            ontology.RowKey = Utils.CleanTableKey(DateTime.UtcNow.ToString(Utils.ISODateFormat));

            POCO.Ontology addedOntology = AddOntology(ontology);

            if (addedOntology != null)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(addedOntology);
                return result;
            }
            else
            {
                return new UnprocessableEntityObjectResult("Internal server error, cannot process this request.");
            }

        }

        private POCO.Ontology AddOntology(POCO.Ontology ontologyEntity)
        {
            bool isAddedOk = false;

            // Execute the insert operation. 
            //log.Verbose("Executing table operation");
            try
            {
                // Check if an SystemId has been set
                if (ontologyEntity.UniqueId == null)
                {
                    ontologyEntity.UniqueId = Guid.NewGuid().ToString();
                }

                // Call the Add datafactory method
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                DataFactory.Ontology.Add(datacfg, ontologyEntity);
            }

            catch (Exception aex)
            {
                _logger.LogError("ERR exception: " + aex.Message);
            }

            isAddedOk = true;

            return ontologyEntity;
        }

    }

    class OntologyEntity : TableEntity
    {
        public OntologyEntity() { }

        [IgnoreProperty]
        public string OntologyUri { get { return this.PartitionKey; } }
        [IgnoreProperty]
        public string Version { get { return this.RowKey; } }
        public Guid UniqueId { get; set; }
        public string Label { get; set; }
        public string Description { get; set; }
        public string Type { get; set; }
    }

    class OntologyFilter
    {
        public OntologyFilter()
        {
            this.ontology = new List<string>();
        }
        public List<string> ontology { get; set; }
    }

}
