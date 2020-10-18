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

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("batch", Name = "Batch")]
    [Authorize]
    public class BatchController : ControllerBase
    {
        public IConfiguration Configuration { get; set; }
        ILogger<BatchController> _logger;

        public BatchController(IConfiguration config, ILogger<BatchController> logger)
        {
            Configuration = config;
            _logger = logger;
        }
    }
}
