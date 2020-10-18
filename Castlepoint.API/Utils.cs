using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

using System.IO;

using System.Data;

using DocumentFormat.OpenXml.Spreadsheet;
using A = DocumentFormat.OpenXml.Drawing;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml;

namespace Castlepoint.REST
{
    public static class Utils
    {
        internal const string TableSuffixDateFormatYMD = "yyyyMMdd";
        internal const string TableSuffixDateFormatYM = "yyyyMM";
        internal const string ISODateFormatNoTime = "yyyy-MM-dd";
        internal const string ISODateFormat = "yyyy-MM-ddTHH:mm:ssZ";
        internal static readonly DateTime AzureTableMinDateTime = DateTime.Parse("1601-01-01T00:00:00+00:00").ToUniversalTime();

        internal static class SecretNames
        {
            public static string ServiceConfigFile { get { return "cpapi_service_config"; } }
        }

        internal static ServiceConfig GetServiceConfig()
        {
            // Load service configuration file
            Console.WriteLine("Loading service configuration file: " + Utils.SecretNames.ServiceConfigFile);
            string serviceConfigJson = Utils.GetSecretOrEnvVar(Utils.SecretNames.ServiceConfigFile);
            if (serviceConfigJson == null || serviceConfigJson.Length == 0)
            {
                throw new ApplicationException("Invalid service configuration file: " + Utils.SecretNames.ServiceConfigFile);
            }

            Utils.ServiceConfig svccfg = JsonConvert.DeserializeObject<Utils.ServiceConfig>(serviceConfigJson);
            return svccfg;
        }

        private static int max_rows = -1;
        internal static int GetMaxRows()
        {
            if (max_rows == -1)
            {
                // Load the service configuration file
                Utils.ServiceConfig svccfg = Utils.GetServiceConfig();
                max_rows = 1000;
                if (svccfg.max_rows > 0)
                {
                    max_rows = svccfg.max_rows;
                }
            }

            return max_rows;
        }

        internal class ServiceConfig
        {
            public string data_storage_service_type { get; set; }
            public string data_storage_connection_string { get; set; }
            public string database_name { get; set; }

            public string url_allowed_cors { get; set; }
            public string url_identity_server { get; set; }
            public int max_rows { get; set; }

            public List<StartupCommand> startup_commands { get; set; }
        }

        internal class StartupCommand
        {
            public string command { get; set; }
            public string arguments { get; set; }
        }

        internal static DataFactory.DataConfig GetDataConfig()
        {
            // Load the service config file
            string serviceConfigJson = Utils.GetSecretOrEnvVar(Utils.SecretNames.ServiceConfigFile);
            if (serviceConfigJson == null || serviceConfigJson.Length == 0)
            {
                throw new ApplicationException("Invalid service configuration file: " + Utils.SecretNames.ServiceConfigFile);
            }
            Utils.ServiceConfig svccfg = JsonConvert.DeserializeObject<Utils.ServiceConfig>(serviceConfigJson);

            // Get the data configuration
            string dataStorageType = svccfg.data_storage_service_type.Trim();
            string connString = svccfg.data_storage_connection_string.Trim();
            string databaseName = svccfg.database_name.Trim();

            // Return the config
            DataFactory.DataConfig dc = new DataFactory.DataConfig();
            dc.ProviderType = dataStorageType;
            dc.ConnectionString = connString;
            dc.DatabaseName = databaseName;

            return dc;
        }

        internal static CloudTable GetCloudTable(string cloudTableName, ILogger _logger)
        {
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();

            string storageAccountConnectionString = datacfg.ConnectionString;
            // validate storage account address
            if (storageAccountConnectionString == "")
            {
                string errormsg = "Data storage connection string not set, cannot access data. Check service config file: " + Utils.SecretNames.ServiceConfigFile;
                _logger.LogError(errormsg);
                throw new InvalidOperationException(errormsg);
            }
            else
            {
                _logger.LogDebug("Data storage connection string loaded");
            }

            // Process the records
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

            // Create the table client. 
            //_logger.Info("Creating cloud table client");
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // Create the table if it doesn't exist. 
            //_logger.Info("Getting table reference");
            CloudTable table = tableClient.GetTableReference(cloudTableName);
            Task tCreate = table.CreateIfNotExistsAsync();
            tCreate.Wait();

            return table;
        }

        internal static CloudTable GetCloudTableNoCreate(string cloudTableName, ILogger _logger)
        {
            DataFactory.DataConfig datacfg = Utils.GetDataConfig();

            string storageAccountConnectionString = datacfg.ConnectionString;
            // validate storage account address
            if (storageAccountConnectionString == "")
            {
                _logger.LogWarning("Storage account connection string not set");
                throw new InvalidOperationException("Storage account connection string not set");
            }
            else
            {
                _logger.LogDebug("Storage account connection string loaded");
            }

            // Configure the account connection
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountConnectionString);

            // Create the table client. 
            //_logger.Info("Creating cloud table client");
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // Create the table if it doesn't exist. 
            //_logger.Info("Getting table reference");
            CloudTable table = tableClient.GetTableReference(cloudTableName);

            return table;
        }

        internal static string CleanTableKey(string keyToClean)
        {

            /*
            The following characters are not allowed in PartitionKey and RowKey fields:
                The forward slash (/) character
                The backslash (\) character
                The number sign (#) character 
                The question mark (?) character
            */

            // Validate the key input
            if (keyToClean==null || keyToClean=="")
            {
                return "";
            }

            string cleanKey = "";

            // unescape html encoding (just in case)
            cleanKey = System.Web.HttpUtility.HtmlDecode(keyToClean);

            string patternForwardSlash = @"\\";
            Regex regForwardSlash = new Regex(patternForwardSlash);
            string patternBackSlash = "/";
            Regex regBackSlash = new Regex(patternBackSlash);
            string patternHash = "#";
            Regex regHash = new Regex(patternHash);
            string patternQuestionMark = @"\?";
            Regex regQuestionMark = new Regex(patternQuestionMark);

            cleanKey = regForwardSlash.Replace(cleanKey.ToLower(), "|");
            cleanKey = regBackSlash.Replace(cleanKey, "|");
            cleanKey = regHash.Replace(cleanKey, "|");
            cleanKey = regQuestionMark.Replace(cleanKey, "|");

            return cleanKey;
        }

        internal static List<string> CreateNGrams(string input, int nGramLength)
        {
            List<string> ngram = new List<string>();

            // Remove all characters that aren't letters or numbers
            const string _regNotLettersNumbers = "[^a-zA-Z0-9]";
            Regex rgx = new Regex(_regNotLettersNumbers);
            string inputLettersNumbersOnly = rgx.Replace(input, "").ToLowerInvariant();

            // Loop through text making ngrams of specified length
            for (int i = 0; i < inputLettersNumbersOnly.Length - (nGramLength - 1); i++)
            {
                string ngramSingle = inputLettersNumbersOnly.Substring(i, nGramLength);
                ngram.Add(ngramSingle);
            }

            return ngram;
        }

        internal static decimal CompareNGrams(List<string> ngramA, List<string> ngramB, decimal minMatchRatio, ILogger _logger)
        {
            // Check if count of ngrams is within 0.8 of each other
            // Otherwise don't bother trying to match them
            decimal ngramRatio = 0;
            if (ngramA.Count > ngramB.Count)
            {
                ngramRatio = (decimal)ngramB.Count / (decimal)ngramA.Count;
            }
            else
            {
                ngramRatio = (decimal)ngramA.Count / (decimal)ngramB.Count;
            }
            if (ngramRatio < minMatchRatio)
            {
                // Skip the comparison with this record authority key phrase
                return ngramRatio;
            }

            // Compare ngrams
            var SameList = ngramA.Where(x => ngramB.Any(x1 => x1 == x))
                .Union(ngramB.Where(x => ngramA.Any(x1 => x1 == x)));
            int sameListCount = SameList.Count();
            decimal matchRatio = 0;
            if (sameListCount > ngramB.Count)
            {
                matchRatio = (decimal)ngramB.Count / (decimal)sameListCount;
            }
            else
            {
                matchRatio = (decimal)sameListCount / (decimal)ngramB.Count;
            }

            _logger.LogDebug("NGram match ratio: " + ngramA.Count.ToString() + ":" + ngramB.Count.ToString() + " => " + matchRatio.ToString());

            return matchRatio;

        }

            internal static string GetLessThanFilter(string filter)
        {
            string returnString = "";

            // Validate the provided string
            if (filter == null || filter.Length == 0) { return returnString; }

            string lastLetter = filter.Substring(filter.Length - 1, 1);

            char letter = char.Parse(lastLetter);
            char nextChar;

            if (letter == '/' || letter == '\\' || letter == ':')
            {
                returnString = filter + 'Z';
            }
            else
            {
                if (letter == 'z')
                    nextChar = 'a';
                else if (letter == 'Z')
                    nextChar = 'A';
                else
                    nextChar = (char)(((int)letter) + 1);

                returnString = filter.Substring(0, filter.Length - 1) + nextChar.ToString();
            }
            return returnString;
        }

        internal static string GetGreaterThanFilter(string filter)
        {
            string returnString = "";

            // Validate the provided string
            if (filter == null || filter.Length == 0) { return returnString; }

            string lastLetter = filter.Substring(filter.Length - 1, 1);

            char letter = char.Parse(lastLetter);
            char previousChar;

            if (letter == '/' || letter == '\\' || letter == ':')
            {
                returnString = filter + 'A';
            }
            else
            {
                if (letter == 'a')
                    previousChar = 'z';
                else if (letter == 'A')
                    previousChar = 'Z';
                else
                    previousChar = (char)(((int)letter) + 1);

                returnString = filter.Substring(0, filter.Length - 1) + previousChar.ToString();
            }
            return returnString;
        }

        internal static string GetSecretOrEnvVar(string key)
        {
            try
            {
                string secretsPath = Environment.GetEnvironmentVariable("CASTLEPOINT_SECRETS_PATH");
                if (secretsPath == null || secretsPath.Trim() == "")
                {
                    secretsPath = "Castlepoint\\cpdev1";
                }


                const string DOCKER_SECRET_PATH = "/run/secrets/";
                if (Directory.Exists(DOCKER_SECRET_PATH))
                {
                    IFileProvider provider = new PhysicalFileProvider(DOCKER_SECRET_PATH);
                    IFileInfo fileInfo = provider.GetFileInfo(key);
                    if (fileInfo.Exists)
                    {
                        using (var stream = fileInfo.CreateReadStream())
                        using (var streamReader = new StreamReader(stream))
                        {
                            return streamReader.ReadToEnd();
                        }
                    }
                }
                else
                {
                    string folderPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), secretsPath);
                    IFileProvider provider = new PhysicalFileProvider(folderPath);
                    IFileInfo fileInfo = provider.GetFileInfo(key);
                    if (fileInfo.Exists)
                    {
                        using (var stream = fileInfo.CreateReadStream())
                        using (var streamReader = new StreamReader(stream))
                        {
                            return streamReader.ReadToEnd();
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                return string.Empty;
            }

            return string.Empty;
        }

        internal static byte[] GetSecretOrEnvVarAsByte(string key)
        {

            var bytes = default(byte[]);

            string configPath = GetConfigPath();

            try
            {

                IFileProvider provider = new PhysicalFileProvider(configPath);
                IFileInfo fileInfo = provider.GetFileInfo(key);
                if (fileInfo.Exists)
                {
                    using (var stream = fileInfo.CreateReadStream())
                    {

                        using (var memstream = new MemoryStream())
                        {
                            var buffer = new byte[512];
                            var bytesRead = default(int);
                            while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                                memstream.Write(buffer, 0, bytesRead);
                            bytes = memstream.ToArray();
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException("Environment variable not found: " + key);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Exception loading: " + key + " (" + ex.Message + ")");
            }

            return bytes;

        }

        internal static string GetConfigPath()
        {
            string configPath = string.Empty;

            string secretsPath = Environment.GetEnvironmentVariable("CASTLEPOINT_SECRETS_PATH");
            if (secretsPath == null || secretsPath.Trim() == "")
            {
                secretsPath = "Castlepoint\\cpdev1";
            }

            const string DOCKER_SECRET_PATH = "/run/secrets/";
            if (Directory.Exists(DOCKER_SECRET_PATH))
            {
                configPath = DOCKER_SECRET_PATH;
            }
            else
            {
                configPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), secretsPath);
                if (!Directory.Exists(configPath))
                {
                    throw new ApplicationException("GetConfigPath path does not exist: " + configPath);
                }
            }

            return configPath;
        }

        internal static string GetSecretOrEnvVar(string key, IConfiguration configuration, ILogger logger)
        {
            try
            {
                string configPath = GetConfigPath();

                logger.LogDebug("Getting docker secret: " + key);
                IFileProvider provider = new PhysicalFileProvider(configPath);
                IFileInfo fileInfo = provider.GetFileInfo(key);
                if (fileInfo.Exists)
                {
                    using (var stream = fileInfo.CreateReadStream())
                    using (var streamReader = new StreamReader(stream))
                    {
                        return streamReader.ReadToEnd();
                    }
                }

            }
            catch (Exception ex)
            {
                logger.LogError("Error getting environment variable: " + key + " (" + ex.Message + ")");
                return string.Empty;
            }

            return string.Empty;

        }

        internal static Stream CreateExcelFile(List<object> data)
        {

            // Lets converts our object data to Datatable for a simplified logic.
            // Datatable is most easy way to deal with complex datatypes for easy reading and formatting. 
            DataTable table = (DataTable)JsonConvert.DeserializeObject(JsonConvert.SerializeObject(data), (typeof(DataTable)));

            MemoryStream stream = new MemoryStream();

            using (SpreadsheetDocument document = SpreadsheetDocument.Create(stream, SpreadsheetDocumentType.Workbook))
            {
                WorkbookPart workbookPart = document.AddWorkbookPart();
                workbookPart.Workbook = new Workbook();

                WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
                var sheetData = new SheetData();
                worksheetPart.Worksheet = new Worksheet(sheetData);

                Sheets sheets = workbookPart.Workbook.AppendChild(new Sheets());
                Sheet sheet = new Sheet() { Id = workbookPart.GetIdOfPart(worksheetPart), SheetId = 1, Name = "Sheet1" };

                sheets.Append(sheet);

                Row headerRow = new Row();

                List<String> columns = new List<string>();
                foreach (System.Data.DataColumn column in table.Columns)
                {
                    columns.Add(column.ColumnName);

                    Cell cell = new Cell();
                    cell.DataType = CellValues.String;
                    cell.CellValue = new CellValue(column.ColumnName);
                    headerRow.AppendChild(cell);
                }

                sheetData.AppendChild(headerRow);

                foreach (DataRow dsrow in table.Rows)
                {
                    Row newRow = new Row();
                    foreach (String col in columns)
                    {
                        Cell cell = new Cell();
                        cell.DataType = CellValues.String;
                        cell.CellValue = new CellValue(dsrow[col].ToString());
                        newRow.AppendChild(cell);
                    }

                    sheetData.AppendChild(newRow);
                }

                workbookPart.Workbook.Save();
            }

            return stream;
        }


    }
}
