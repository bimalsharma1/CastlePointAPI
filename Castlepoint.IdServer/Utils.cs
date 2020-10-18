using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using System.IO;

namespace Castlepoint.IdServer
{
    public static class Utils
    {
        /// <summary>
        /// Docker Secret filenames for the Docker image to use
        /// </summary>
        internal static class SecretNames
        {
            public static string ConfigFileIDServer { get { return "cpid_service_config"; } }
            public static string AdminUsers { get { return "cpid_admin_users"; } }
        }

        internal static CastlepointIdentityServerConfig GetCPIDConfig()
        {
            // Get the Castlepoint configuration for this client
            string cpConfigSecret = Utils.GetSecretOrEnvVarAsString(SecretNames.ConfigFileIDServer);
            if (cpConfigSecret.Length == 0)
            {
                Console.WriteLine("ERROR: service configuration file is empty or does not exist: " + SecretNames.ConfigFileIDServer);
                return null;
            }

            CastlepointIdentityServerConfig cpConfig = JsonConvert.DeserializeObject<CastlepointIdentityServerConfig>(cpConfigSecret);

            return cpConfig;
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

        internal static string GetSecretOrEnvVarAsString(string key)
        {
            try
            {
                string configPath = GetConfigPath();

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
                Console.WriteLine("Error getting environment variable: " + key + " (" + ex.Message + ")");
                return string.Empty;
            }

            return string.Empty;

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

    }
}
