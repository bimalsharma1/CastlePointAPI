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
    [Route("account", Name = "Account")]
    [Authorize]
    public class AccountController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<AccountController> _logger;

        public AccountController(IConfiguration config, ILogger<AccountController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        [HttpGet("managed", Name = "GetAllManagedAccounts")]
        public IActionResult GetAllManagedAccounts()
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetManagedAccount");


                DataFactory.DataConfig dataConfig = Utils.GetDataConfig();
                List<POCO.Account.Managed> managedAccounts = DataFactory.AccountManagement.Get(dataConfig);

                entityAsJson = JsonConvert.SerializeObject(managedAccounts, Formatting.Indented);

            }
            catch (Exception ex)
            {
                string exceptionMsg = " GET exception: " + ex.Message;
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
