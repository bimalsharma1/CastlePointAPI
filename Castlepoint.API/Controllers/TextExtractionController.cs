using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

using System.Net.Http;
using System.Text.Encodings;
using System.Text;
using System.IO;
using System.Configuration;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using System.Net.Http.Headers;

namespace Castlepoint.REST.Controllers
{
    [Produces("application/json")]
    [Route("textextraction")]
    [Authorize]
    public class TextExtractionController : Controller
    {

        public IConfiguration Configuration { get; set; }
        ILogger<TextExtractionController> _logger;

        public TextExtractionController(IConfiguration config, ILogger<TextExtractionController> logger)
        {
            Configuration = config;
            _logger = logger;
        }



        //set by dependency injection
        //public TextExtractionController()
        //{
        //    _logger = logger;
        //}

        //internal string GetSecretOrEnvVar(string key)
        //{
        //    try
        //    {

        //    const string DOCKER_SECRET_PATH = "/run/secrets/";
        //    if (Directory.Exists(DOCKER_SECRET_PATH))
        //    {
        //        _logger.LogDebug("Getting docker secret: " + key);
        //        IFileProvider provider = new PhysicalFileProvider(DOCKER_SECRET_PATH);
        //        IFileInfo fileInfo = provider.GetFileInfo(key);
        //        if (fileInfo.Exists)
        //        {
        //            using (var stream = fileInfo.CreateReadStream())
        //            using (var streamReader = new StreamReader(stream))
        //            {
        //                return streamReader.ReadToEnd();
        //            }
        //        }
        //    }
        //        else
        //        {
        //            _logger.LogDebug("Getting configuration value: " + key);
        //            var configValue = Configuration.GetValue<string>(key);
        //            if (configValue==null)
        //            {
        //                return string.Empty;
        //            }
        //            else
        //            {
        //                return configValue.ToString();
        //            }
        //        }
        //    }
        //    catch(Exception ex)
        //    {
        //        _logger.LogError("Error getting environment variable: " + key + " (" + ex.Message + ")");
        //        return string.Empty;
        //    }

        //    return string.Empty;

        //}

        // POST: textextraction
        [HttpPost]
        public IActionResult Post(ICollection<IFormFile> files)
        {
            _logger.LogInformation("CPAPI: Post");
            try
            {

                // Check that only one file has been uploaded
                if (files.Count != 1)
                {
                    return BadRequest();
                }
                if (files.FirstOrDefault().Length == 0)
                {
                    return BadRequest();
                }

                //var fileBytes = formFiles.FirstOrDefault().OpenReadStream;
                MemoryStream stream = new MemoryStream();
                files.FirstOrDefault().CopyTo(stream);
                byte[] fileBytes = stream.ToArray();

                // Extract text
                string extractedText = "";
                string tikaBaseAddress = "";
                try
                {
                    // Get the Tika base address
                    tikaBaseAddress = Utils.GetSecretOrEnvVar(ConfigurationProperties.TikaBaseAddress, this.Configuration, this._logger).Trim();
                    // validate tika base address
                    if (tikaBaseAddress == "")
                    {
                        _logger.LogWarning("Tika Base Address not valid - cannot extract keyphrases");
                        return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
                    }
                    else
                    {
                        _logger.LogDebug("Tika base address: " + tikaBaseAddress);
                    }

                    //log.Verbose("attempting text extraction...");
                    //TikaOnDotNet.TextExtraction.TextExtractor extractor = new TikaOnDotNet.TextExtraction.TextExtractor();
                    //TikaOnDotNet.TextExtraction.TextExtractionResult extractionResult = extractor.Extract(fileBytes);
                    //extractedText = extractionResult.Text.Replace("\"", "");

                    // Call the TIKA service
                    using (var tikaClient = new HttpClient())
                    {
                        //tikaClient.BaseAddress = new Uri("http://localhost:9998");
                        //tikaClient.BaseAddress = new Uri("http://172.17.0.3:9998");
                        tikaClient.BaseAddress = new Uri(tikaBaseAddress);
                        tikaClient.DefaultRequestHeaders.Accept.Clear();
                        //tikaClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                        var tikaContent = new ByteArrayContent(fileBytes);
                        Task<HttpResponseMessage> tikaResponse = tikaClient.PutAsync("tika", tikaContent);
                        tikaResponse.Wait();
                        var tikaResult = tikaResponse.Result;

                        if (tikaResult.IsSuccessStatusCode)
                        {
                            // Store text extracted
                            Task<string> tData = tikaResult.Content.ReadAsStringAsync();
                            tData.Wait();
                            extractedText = tData.Result;
                            _logger.LogDebug("Extract text length: " + extractedText.Length.ToString());
                        }
                        else
                        {
                            // Problem getting data from tika server
                        }
                    }

                    //log.Info("Content type: " + extractionResult.ContentType);
                    //log.Info("Text: " + extractionResult.Text);
                    //log.Verbose("text extracted");
                }
                catch (Exception ex)
                {
                    string exceptionMsg = "Tika exception (" + tikaBaseAddress + "): " + ex.Message;
                    //log.Info("Exception occurred extracting text from uploaded file \r\nError: " + ex.Message);
                    if (ex.InnerException != null)
                    {
                        exceptionMsg = exceptionMsg + "[" + ex.InnerException.Message + "]";
                    }

                    _logger.LogError(exceptionMsg);
                    return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
                }

                // Make sure we have some text to analyse
                string keyPhrasesJson = "";
                try
                {
                    if (extractedText != "")
                    {
                        string keyphraseBaseAddress = Utils.GetSecretOrEnvVar(ConfigurationProperties.KeyphraseBaseAddress, this.Configuration, this._logger).Trim();
                        // validate keyphrase base address
                        if (keyphraseBaseAddress == "")
                        {
                            _logger.LogWarning("Keyphrase Base Address not valid - cannot extract keyphrases");
                            return StatusCode((int)System.Net.HttpStatusCode.InternalServerError, "Keyphrase Base Address not valid - cannot extract keyphrases");
                        }
                        string keyphraseAccessKey = Utils.GetSecretOrEnvVar(ConfigurationProperties.KeyphraseAccesKey, this.Configuration, this._logger).Trim();
                        if (keyphraseAccessKey == "")
                        {
                            _logger.LogWarning("Keyphrase Access Key not valid - cannot extract keyphrases");
                            return StatusCode((int)System.Net.HttpStatusCode.InternalServerError, "Keyphrase Access Key not valid - cannot extract keyphrases");
                        }
                        //log.Verbose("attempting text split");
                        List<string> splitText = SplitText(extractedText, 5000);
                        //log.Verbose("Split text pieces: " + splitText.Count.ToString());

                        // Submit to text analytics
                        var client = new HttpClient();

                        // Request headers
                        client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", keyphraseAccessKey);

                        HttpResponseMessage response;

                        // Compose request.
                        string body = "";
                        int countString = 0;
                        KeyPhraseInput kpInput = new KeyPhraseInput();
                        //log.Info("Adding split text strings to request body: " + splitText.Count.ToString());
                        foreach (string s in splitText)
                        {
                            countString++;
                            //string textPhrase = CleanTextForJson(extractedText);
                            KeyPhraseDocumentInput kpDocInput = new KeyPhraseDocumentInput("en", countString.ToString(), s);
                            kpInput.documents.Add(kpDocInput);
                            //body = body + "{ \"language\": \"" + "en" + "\", \"id\":\"" + countString.ToString() + "\",  \"text\": \"" + textPhrase + "\"   }";
                        }
                        //body = "{  \"documents\": [" + body + "] }";
                        //log.Verbose("Serializing kpInput to JSON");
                        body = JsonConvert.SerializeObject(kpInput, Formatting.Indented);
                        //log.Verbose("keyphrase json request: " + body);

                        // Request body
                        byte[] byteData = Encoding.UTF8.GetBytes(body);

                        using (var content = new ByteArrayContent(byteData))
                        {
                            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                            Task<HttpResponseMessage> tResponse = client.PostAsync(keyphraseBaseAddress, content);
                            tResponse.Wait();
                            response = tResponse.Result;

                            // Check the response
                            if (response.StatusCode != System.Net.HttpStatusCode.OK)
                            {
                                //log.Info("Analytics.KeyPhrase: " + response.StatusCode.ToString() + "(" + response.ReasonPhrase + ")");
                                //TODO: log error
                                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);
                            }
                        }

                        // Get the JSON response key phrases from the processed document
                        //log.Info("saving key phrases");
                        Task<string> rString = response.Content.ReadAsStringAsync();
                        rString.Wait();
                        keyPhrasesJson = rString.Result;
                    }
                }
                catch (Exception keyPhraseEx)
                {
                    _logger.LogError("Keyphrase extraction: " + keyPhraseEx.Message);
                    return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);

                }

                ObjectResult result = new ObjectResult(keyPhrasesJson);
                return result;


            }
            catch (Exception postEx)
            {
                _logger.LogError("Extraction Controller: " + postEx.Message);
                return StatusCode((int)System.Net.HttpStatusCode.InternalServerError);

            }

        }




        private static List<string> SplitText(string textToSplit, int chunkSize)
        {
            List<string> textSplitByChunk = new List<string>();
            textToSplit = textToSplit.Trim();

            // Make sure there is something to do
            if (textToSplit.Length == 0)
            {
                //log.Verbose("SplitText: no text to split, returning");
                return textSplitByChunk;
            }

            //log.Verbose("Split text size: " + textToSplit.Length.ToString());

            int charsToProcess = textToSplit.Length;
            int nextChunkSize = 0;
            string remainingChunk = textToSplit;
            string currentChunk = "";
            int maxIterations = 100;

            do
            {
                // Check if chars left gt chunk size
                if (charsToProcess > chunkSize)
                {
                    //log.Verbose("Setting chunk size to maximum (" + chunkSize.ToString() + ")");
                    nextChunkSize = chunkSize;
                }
                else
                {
                    //log.Verbose("Setting chunk size to remaining characters");
                    nextChunkSize = charsToProcess;
                }

                // Get the next set of data
                currentChunk = remainingChunk.Substring(0, nextChunkSize);

                int index = -1;
                int maxIndex = -1;
                if (currentChunk.Length < chunkSize)
                {
                    // Done with splitting
                    //log.Verbose("No more splitting required, adding remaining characters");
                    maxIndex = currentChunk.Length;
                }
                else
                {
                    // Split text further
                    //log.Verbose("More splitting required, looking for a split point");

                    // Find the right-most character in the order . then ; then , then : then (space)
                    index = currentChunk.LastIndexOf(".");
                    if (index > maxIndex) { maxIndex = index; }

                    if (index == -1)
                    {
                        index = currentChunk.LastIndexOf(";");
                        if (index > maxIndex) { maxIndex = index; }
                    }
                    if (index == -1)
                    {
                        index = currentChunk.LastIndexOf(",");
                        if (index > maxIndex) { maxIndex = index; }
                    }

                    // Default condition
                    if (maxIndex == -1)
                    {
                        maxIndex = currentChunk.Length;
                    }

                    // Validate maxIndex, make sure it is still a large part of the chunk (in case the work splitting characters aren't found
                    Decimal halfWay = Math.Floor((Decimal)(currentChunk.Length / 2));
                    if (maxIndex < halfWay)
                    {
                        maxIndex = (int)halfWay;
                    }

                }

                //log.Verbose("character index: " + maxIndex.ToString());        

                // Add the chunk to the chunk collection
                string chunkToAdd = currentChunk.Substring(0, maxIndex);
                //log.Verbose("chunk to add: " + chunkToAdd);
                textSplitByChunk.Add(chunkToAdd);

                //log.Verbose("chunk to add: " + chunkToAdd);
                // Remove the chunk we are adding from the remaining chunk of string
                remainingChunk = remainingChunk.Substring(chunkToAdd.Length, remainingChunk.Length - chunkToAdd.Length);
                charsToProcess = remainingChunk.Length;

                maxIterations--;
            }
            while (charsToProcess > 0 && maxIterations >= 0);

            return textSplitByChunk;
        }
    }




    class KeyPhraseInput
    {
        public KeyPhraseInput()
        {
            this.documents = new List<KeyPhraseDocumentInput>();
        }
        public List<KeyPhraseDocumentInput> documents;
    }

     class KeyPhraseDocumentInput
    {
        public KeyPhraseDocumentInput() { }
        public KeyPhraseDocumentInput(string language, string id, string text)
        {
            this.language = language;
            this.id = id;
            this.text = text;
        }

        public string language = "";
        public string id = "";
        public string text = "";
    }

     class keyPhrases
    {
        public keyPhrases()
        {

        }
        public List<KeyPhraseDocument> documents = new List<KeyPhraseDocument>();
        public List<KeyPhraseError> errors = new List<KeyPhraseError>();
    }

     class KeyPhraseDocument
    {
        public string id = "";
        public List<string> keyPhrases = new List<string>();

        public KeyPhraseDocument()
        {

        }
    }

     class KeyPhraseError
    {
        public KeyPhraseError()
        {

        }
    }

}
