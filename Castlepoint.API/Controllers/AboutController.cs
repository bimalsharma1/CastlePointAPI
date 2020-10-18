using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Localization;

using Microsoft.AspNetCore.Authorization;

using Newtonsoft.Json;

namespace Castlepoint.API.Controllers
{
    [Produces("application/json")]
    [Route("about")]
    public class AboutController : Controller
    {
        ILogger<AboutController> _logger;
        private readonly IStringLocalizer<AboutController> _localizer;

        //set by dependency injection
        public AboutController(ILogger<AboutController> logger, IStringLocalizer<AboutController> localizer)
        {
            _logger = logger;
            _localizer = localizer;
        }

        class AboutResponse
        {
            public string about { get; set; }
        }

        // GET: api/About
        [HttpGet("noauth", Name = "GetNoAuth")]
        public IActionResult GetNoAuth()
        {
            _logger.LogInformation("CPAPI: Get (no auth)");
            AboutResponse aboutResponse = new AboutResponse();
            
            aboutResponse.about = _localizer["Castlepoint is an API for compliant Records Management (no authentication for this service)"];
            string entityAsJson = JsonConvert.SerializeObject(aboutResponse, Formatting.Indented);

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }

        // GET: api/About
        [HttpGet]
        [Authorize]
        public IActionResult Get()
        {
            _logger.LogInformation("CPAPI: Get (with auth)");
            AboutResponse aboutResponse = new AboutResponse();
            aboutResponse.about = _localizer["Castlepoint is an API for compliant Records Management (authenticated response)"];
            string entityAsJson = JsonConvert.SerializeObject(aboutResponse, Formatting.Indented);

            ObjectResult result = new ObjectResult(entityAsJson);
            return result;
        }
    }
}
