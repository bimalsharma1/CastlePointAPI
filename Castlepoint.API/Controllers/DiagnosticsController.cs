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
    [Route("diagnostics", Name = "Diagnostics")]
    [Authorize]
    public class DiagnosticsController : Controller
    {
        public IConfiguration Configuration { get; set; }
        ILogger<DiagnosticsController> _logger;

        public DiagnosticsController(IConfiguration config, ILogger<DiagnosticsController> logger)
        {
            Configuration = config;
            _logger = logger;
        }
        public static string[] EventTypes = { "GetFileBytes", "DetectMIMEType", "GetFilePermissions", "GetFileSecurityClassification", "GetFileHash", "GetFileMetadata", "GetFileText", "GetKeyPhrases", "GetNamedEntitys", "GetKeyPhrasesFilename", "GetNamedEntitysFileName", "GetSubjectObject" };

        // GET: api/Ontology
        [HttpGet("fileprocessing/events", Name = "ListDiagnosticEvents")]
        public IActionResult Get()
        {
            string entityAsJson = "";
            try
            {
                List<string> diagEvents = new List<string>(EventTypes) ;

                diagEvents.Sort((x, y) => String.Compare(x, y));

                entityAsJson = JsonConvert.SerializeObject(diagEvents, Formatting.Indented);

            }
            catch(Exception ex)
            {
                string exceptionMsg = "List Diagnostic Types GET exception: " + ex.Message;
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

        class DiagnoticEventFilter
        {
            public string[] eventtypes{ get; set; }
        }

        class ChartLineDataset
        {
            public ChartLineDataset()
            {
                this.label = string.Empty;
                this.backgroundColor = "#f87979";
                this.data = new List<double>();
            }
            public string label { get; set; }
            public string backgroundColor { get; set; }
            public List<double> data { get; set; }
        }
        class ChartLineData
        {
            public ChartLineData()
            {
                this.labels = new List<string>();
                this.datasets = new List<ChartLineDataset>();
            }
            public List<string> labels { get; set; }
            public List<ChartLineDataset> datasets { get; set; }
        }

    [HttpGet("fileprocessing/filter", Name = "GetByDiagnosticsEventFilter")]
        public IActionResult GetByFilter([FromQuery] string filter, string format)
        {
            string entityAsJson = "";
            try
            {

                _logger.LogInformation("CPAPI: GetByDiagnosticEventFilter");

                // Deserialize the ontology filter
                DiagnoticEventFilter oFilter = new DiagnoticEventFilter();
                if (filter!=null && filter.Length>0)
                {
                    _logger.LogDebug("Deserializing ontology filter of length: " + filter.Length);
                    oFilter = JsonConvert.DeserializeObject<DiagnoticEventFilter>(filter);
                }

                // Check if a format has been requested
                string dataFormat = string.Empty;
                if (format!=null && format!="")
                {
                    dataFormat = format.ToLower();
                }

                // Create the filters
                List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                foreach(string s in oFilter.eventtypes)
                {
                    DataFactory.Filter etFilter = new DataFactory.Filter("Event", s, "eq");
                }

                // Call the data factory
                DataFactory.DataConfig providerConfig = Utils.GetDataConfig();
                List<POCO.CPDiagnostic> diags = DataFactory.Diagnostics.GetDiagnosticEntries(providerConfig, filters);

                // Check the data format request
                switch(dataFormat)
                {
                    case "chartline":
                        {
                            // Pre-sort the data by event type and date of event (RowKey)
                            //diags.Sort((x, y) => string.Compare(x.RowKey, y.RowKey));
                            diags = diags.OrderBy(x => x.Event).ThenBy(x => x.RowKey).ToList();

                            ChartLineData chartData = new ChartLineData();

                            // Pre-populate all the dates for the chart
                            foreach (POCO.CPDiagnostic diagitem in diags)
                            {
                                string itemDate = diagitem.RowKey.Substring(0, 10);

                                // Check if the label exists
                                if (!chartData.labels.Contains(itemDate))
                                {
                                    chartData.labels.Add(itemDate);
                                }
                            }

                            // Sort the label data in the chart data object
                            chartData.labels.Sort((x, y) => string.Compare(x, y));

                            // Line chart colors
                            string[] chartColors = new string[] {"#000099", "#006666", "#0099cc","#660099", "#666633", "#669999"
                                , "#66cc66", "#990099", "#996666", "#99cc66", "#cc00ff", "#cc9933", "#ccff99", "#ccff99", "#ff99ff"
                                , "#ffcc33", "#ff0000", "#ffcccc", "#cc99ff", "#99cc00"};

                            // Transform the event data
                            ChartLineDataset currentDataset = null;
                            string currentEvent = string.Empty;
                            string currentDate = string.Empty;
                            int countDataset = 0;
                            int countDataInDay = 0;
                            double sumAverages = 0;
                            Dictionary<string, double> eventAverageForDay = new Dictionary<string, double>();

                            foreach (POCO.CPDiagnostic diagitem in diags)
                            {
                                string itemDate = diagitem.RowKey.Substring(0, 10);

                                // Check if the event has been set
                                if (currentEvent==string.Empty)
                                {
                                    // First event in list - init vars
                                    currentDataset = new ChartLineDataset();
                                    currentDataset.backgroundColor = chartColors[countDataset % 20];
                                    currentDataset.label = diagitem.Event;
                                    currentEvent = diagitem.Event;
                                    currentDate = itemDate;
                                    if (diagitem.TotalMilliseconds!=0)
                                    {
                                        sumAverages = diagitem.NumBytes / diagitem.TotalMilliseconds;
                                        countDataInDay = 1;
                                    }
                                    else
                                    {
                                        sumAverages = 0;
                                        countDataInDay = 0;
                                    }
                                    countDataset++;
                                }
                                else
                                {
                                    // Check if the event has changed from the current event
                                    if (currentEvent != diagitem.Event)
                                    {
                                        //// New event - finalise previous event and add data
                                        if (countDataInDay != 0)
                                        {
                                            eventAverageForDay.Add(currentDate, sumAverages / countDataInDay);
                                        }

                                        // Populate the dataset by matching the labels (X-axis) to our averages
                                        foreach (string s in chartData.labels)
                                        {
                                            if (eventAverageForDay.ContainsKey(s))
                                            {
                                                currentDataset.data.Add(eventAverageForDay[s]);
                                            }
                                            else
                                            {
                                                currentDataset.data.Add(0);
                                            }
                                        }

                                        // Add the dataset to our chart data
                                        chartData.datasets.Add(currentDataset);

                                        // Now set the new event and date
                                        // and reset the eventAverage data
                                        eventAverageForDay = new Dictionary<string, double>();
                                        currentDataset = new ChartLineDataset();
                                        currentDataset.backgroundColor = chartColors[countDataset % 20];
                                        currentDataset.label = diagitem.Event;
                                        currentEvent = diagitem.Event;
                                        currentDate = itemDate;
                                        if (diagitem.TotalMilliseconds != 0)
                                        {
                                            sumAverages = diagitem.NumBytes / diagitem.TotalMilliseconds;
                                            countDataInDay = 1;
                                        }
                                        else
                                        {
                                            sumAverages = 0;
                                            countDataInDay = 0;
                                        }
                                        countDataset++;
                                    }
                                    else
                                    {
                                        // Check if the date has changed
                                        if (currentDate != itemDate)
                                        {
                                            // New date - finalise previous date data
                                            if (countDataInDay!=0)
                                            { 
                                                eventAverageForDay.Add(currentDate, sumAverages / countDataInDay);
                                            }

                                            // Set the new date
                                            currentDate = itemDate;

                                            // Reset date and count vars
                                            if (diagitem.TotalMilliseconds != 0)
                                            {
                                                sumAverages = diagitem.NumBytes / diagitem.TotalMilliseconds;
                                                countDataInDay = 1;
                                            }
                                            else
                                            {
                                                sumAverages = 0;
                                                countDataInDay = 0;
                                            }
                                        }
                                        else
                                        {
                                            // Same date - add to previous data
                                            if (diagitem.TotalMilliseconds != 0)
                                            {
                                                sumAverages += diagitem.NumBytes / diagitem.TotalMilliseconds;
                                                countDataInDay++;
                                            }
                                        }
                                    }


                                }


                            }

                            // end of loop - check if there is a dataset to add
                            if (currentDataset!=null)
                            {
                                // TODO final calculations
                                // HACK
                                //currentDataset.data.Add(1 * countDataset);
                                //currentDataset.data.Add(2 * countDataset);
                                //currentDataset.data.Add(3 * countDataset);
                                //currentDataset.data.Add(4 * countDataset);

                                // Add the dataset to our chart data
                                chartData.datasets.Add(currentDataset);
                            }

                            // Serialize the chart data
                            entityAsJson = JsonConvert.SerializeObject(chartData, Formatting.Indented);

                            break;
                        }
                    default:
                        {
                            entityAsJson = JsonConvert.SerializeObject(diags, Formatting.Indented);
                            break;
                        }
                }


            }
            catch (Exception ex)
            {
                string exceptionMsg = "Diagnostics GET exception: " + ex.Message;
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
