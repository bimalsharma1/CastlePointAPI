using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using System.Net.Http.Headers;

using System.IO;

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("hash")]
    [Authorize]    
    public class HashController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<HashController> _logger;

        public HashController(IConfiguration config, ILogger<HashController> logger)
        {
            Configuration = config;
            _logger = logger;
        }


        // POST: hash
        [HttpPost]
        public IActionResult Post(ICollection<IFormFile> files)
        {
            _logger.LogInformation("CPAPI: Post");

            try
            {

                // Get the siphash key from secret/environment variable
                string siphashKey = Utils.GetSecretOrEnvVar(ConfigurationProperties.HashKey, this.Configuration, this._logger).Trim();
                // validate tika base address
                if (siphashKey == "")
                {
                    _logger.LogWarning("Hash key not valid - cannot generate hash");
                    return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
                }
                else
                {
                    _logger.LogDebug("Hash key loaded");
                }
                byte[] siphashKeyBytes = System.Text.Encoding.ASCII.GetBytes(siphashKey);


                // Check that only one file has been uploaded
                if (files.Count != 1)
                {
                    return BadRequest();
                }
                if (files.FirstOrDefault().Length == 0)
                {
                    return BadRequest();
                }

                // Get the bytes from the file
                MemoryStream stream = new MemoryStream();
                files.FirstOrDefault().CopyTo(stream);
                byte[] fileBytes = stream.ToArray();

                // Calculate the hash
                System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SipHash hasher = new SipHash(siphashKeyBytes);
                long hashResult = hasher.Compute(fileBytes);
                stopwatch.Stop();
                _logger.LogDebug("Hash time (ms): " + stopwatch.ElapsedMilliseconds.ToString());

                // Convert to JSON
                string hasResultAsJson = JsonConvert.SerializeObject(hashResult, Formatting.None);

                // Return final result
                ObjectResult result = new ObjectResult(hasResultAsJson);
                return result;


            }
            catch (Exception hashEx)
            {
                _logger.LogError("Hash Controller: " + hashEx.Message);
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);

            }
        }


    }
}
