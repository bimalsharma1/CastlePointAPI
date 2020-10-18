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
    [Route("recordauthority", Name = "RecordAuthority")]
    [Authorize]
    public class RecordAuthorityController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<RecordAuthorityController> _logger;

        public RecordAuthorityController(IConfiguration config, ILogger<RecordAuthorityController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        class RecordAuthorityFilter
        {
            public RecordAuthorityFilter()
            {
                systems = new List<SystemFilter>();
                records = new List<RecordFilter>();
            }
            public List<SystemFilter> systems;
            public List<RecordFilter> records;
        }

        class SystemFilter
        {
            public string systemuri;
        }
        class RecordFilter
        {
            public string recorduri;
        }

        class RASchemaFilterEntity : TableEntity
        {
            public RASchemaFilterEntity() { }
            public RASchemaFilterEntity(string RASchemaUri, string Function, string Class)
            {
                this.RASchemaUri = RASchemaUri;
                this.Class = Class;
                this.Function = Function;
            }
            public string RASchemaUri { get; set; }
            public string Class { get; set; }
            public string Function { get; set; }
        }


        [HttpGet("assigned/system", Name = "GetSystemAssignedRecordAuthority")]
        public IActionResult GetBySystemFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetAssigned");

                // Deserialize the ontology filter
                RecordAuthorityFilter oFilter = new RecordAuthorityFilter();
                if (filter != null && filter.Length > 0)
                {
                    _logger.LogDebug("Deserializing filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<RecordAuthorityFilter>(filter);
                }

                if (oFilter.systems==null || oFilter.systems.Count==0)
                {
                    _logger.LogWarning("Missing System filters for GetBySystemFilter");
                    return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
                }

                List<POCO.RecordAuthorityFilter> recauth = new List<POCO.RecordAuthorityFilter>();


                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                    if (oFilter.systems.Count > 0)
                {
                    foreach (SystemFilter sf in oFilter.systems)
                    {
                        POCO.System sys = new POCO.System();
                        sys.PartitionKey = Utils.CleanTableKey(sf.systemuri);

                        List<POCO.RecordAuthorityFilter> sysrecauth = DataFactory.RecordAuthority.GetAssignedForSystem(datacfg, sys);
                        recauth.AddRange(sysrecauth);
                    }
                }

                recauth.Sort((x, y) => String.Compare(x.Function, y.Function));

                entityAsJson = JsonConvert.SerializeObject(recauth, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Authority GET By Filter exception: " + ex.Message;
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

        [HttpGet("filter", Name = "GetRecordAuthorityByOntologyFilter")]
        public IActionResult GetByFilter([FromQuery] string filter)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: Get");

                // Deserialize the ontology filter
                OntologyFilter oFilter = new OntologyFilter();
                if (filter!=null && filter.Length>0)
                {
                    _logger.LogDebug("Deserializing ontology filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<OntologyFilter>(filter);
                }

                List<POCO.RecordAuthorityFilter> filters = new List<POCO.RecordAuthorityFilter>();
                foreach (string of in oFilter.ontology)
                {
                    string cleanFilterPKey = Utils.CleanTableKey(of);
                    POCO.RecordAuthorityFilter filt = new POCO.RecordAuthorityFilter(cleanFilterPKey, "", "");
                    //DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", cleanFilterPKey, "eq");
                    filters.Add(filt);
                }

                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                List<POCO.RecordAuthorityFunctionActivityEntry> recordAuthorityEntities = DataFactory.RecordAuthority.GetEntries(datacfg, filters);

                recordAuthorityEntities.Sort((x, y) => String.Compare(x.Function, y.Function));

                entityAsJson = JsonConvert.SerializeObject(recordAuthorityEntities, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = "Record Authority GET By Filter exception: " + ex.Message;
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

        /// <summary>
        /// Assigns a Records Authority to a System
        /// </summary>
        /// <param name="system"></param>
        /// <param name="recordauth"></param>
        /// <returns></returns>
        [HttpPost("system/assign", Name = "AssignRecordAuthorityToSystem")]
        public IActionResult AssignToSystem([FromHeader] string system, List<string> recordauths)
        {
            _logger.LogInformation("CPAPI: AssignRecordAuthorityToSystem");

            List<POCO.RecordAuthorityFilter> newRAFilts = new List<POCO.RecordAuthorityFilter>();

            // Validate the params
            if (string.IsNullOrEmpty(system))
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            if (recordauths==null || recordauths.Count==0)
            {
                return StatusCode((int)System.Net.HttpStatusCode.BadRequest);
            }

            // Assign each record authority to the system
            foreach(string ra in recordauths)
            {
                // Create a new RecordAuthorityFilter
                POCO.RecordAuthorityFilter rafilt = new POCO.RecordAuthorityFilter();
                rafilt.PartitionKey = Utils.CleanTableKey(system);
                rafilt.RowKey = Utils.CleanTableKey(ra);
                rafilt.RASchemaUri = ra;

                POCO.RecordAuthorityFilter addedRAFilt = AssignToSystem(rafilt);

                newRAFilts.Add(addedRAFilt);
            }

            // Check if any adds succeeded
            if (newRAFilts.Count > 0)
            {
                // Return the added system entity
                ObjectResult result = new ObjectResult(newRAFilts);
                return result;
            }
            else
            {
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }

        }

        private POCO.RecordAuthorityFilter AssignToSystem(POCO.RecordAuthorityFilter recordAuthFilt)
        {
            bool isAddedOk = false;

            // Execute the insert operation. 
            //log.Verbose("Executing table operation");
            try
            {


                // Call the Add datafactory method
                DataFactory.DataConfig datacfg = Utils.GetDataConfig();
                DataFactory.RecordAuthority.AssignToSystem(datacfg, recordAuthFilt);
            }

            catch (Exception aex)
            {
                _logger.LogError("ERR exception: " + aex.Message);
            }

            isAddedOk = true;

            return recordAuthFilt;
        }

    }

    class RecordAuthorityEntity : TableEntity
    {
        public RecordAuthorityEntity() { }

        [IgnoreProperty]
        public string OntologyUri { get { return this.PartitionKey; } }
        public string Authority { get; set; }
        public string Activity { get; set; }
        public int ContentType { get; set; }
        public string DescriptionofRecords { get; set; }
        public string DisposalAction { get; set; }
        public int EntryNo { get; set; }
        public string Function { get; set; }
        public string FunctionDescription { get; set; }
        public int Retention { get; set; }
        public string Trigger { get; set; }
    }
    
}
