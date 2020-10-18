using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;

using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

using Newtonsoft.Json;

namespace Castlepoint.Text
{
    public class UploadFile
    {
        public string filename { get; set; }
        public string mimetype { get; set; }
        public IFormFile file { get; set; }
    }

    [Route("text/extract")]
    public class TextExtractController : Controller
    {


        public IConfiguration Configuration { get; set; }
        ILogger<TextExtractController> _logger;

        public TextExtractController(IConfiguration config, ILogger<TextExtractController> logger)
        {
            Configuration = config;
            _logger = logger;
        }

        // GET: api/<controller>
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "Please POST to the service" };
        }

        // POST api/<controller>
        [HttpPost]
        public async Task<IActionResult> Post([FromForm]UploadFile upload)
        {
            try
            {
                // Validate the upload
                if (upload.file == null
                    || upload.filename == null
                    || upload.mimetype == null)
                {
                    return new BadRequestResult();
                }

                long fileLength = upload.file.Length;
                if (fileLength > 50000000)
                {
                    return new BadRequestResult();
                }

                POCO.DocumentText text = new POCO.DocumentText();

                Stopwatch st = new Stopwatch();

                // Check the mimetype
                switch (upload.mimetype.ToLower().Trim())
                {
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.template":
                        // DOCX
                        st.Start();
                        text = Word.ExtractText(upload.file, _logger);
                        st.Stop();
                        _logger.LogInformation("TEXT [" + st.ElapsedMilliseconds + "ms] DOCX: " + upload.filename);
                        break;

                    case "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                    case "application/vnd.openxmlformats-officedocument.presentationml.template":
                    case "application/vnd.openxmlformats-officedocument.presentationml.slideshow":
                        // PPTX
                        st.Start();
                        text = PowerPoint.ExtractText(upload.file);
                        st.Stop();
                        _logger.LogInformation("TEXT [" + st.ElapsedMilliseconds + "ms] PPTX: " + upload.filename);
                        break;
                    case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                    case "application/vnd.openxmlformats-officedocument.spreadsheetml.template":
                        // XLSX
                        st.Start();
                        text = Excel.ExtractText(upload.file, _logger);
                        st.Stop();
                        _logger.LogInformation("TEXT [" + st.ElapsedMilliseconds + "ms] XLSX: " + upload.filename);
                        break;
                    default:
                        return new BadRequestResult();

                }

                // Serialize the extract result
                string jsonText = JsonConvert.SerializeObject(text);

                ObjectResult result = new ObjectResult(jsonText);
                return result;
            }
            catch(Exception exPost)
            {
                _logger.LogError("ExtractTextController: " + exPost.Message);
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
            }


        }

        



    }
}
