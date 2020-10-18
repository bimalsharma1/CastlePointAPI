using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.FileProviders;
using DocumentFormat.OpenXml.Math;
using System.Drawing;
using Newtonsoft.Json;
using Microsoft.VisualBasic.CompilerServices;

namespace Castlepoint.Utilities
{
    public class Configuration
    {
        public static string GetConnectionData(string connectionData)
        {
            // Try and deserialize to check if there is a connection to get from a provider
            POCO.Connections.BaseConnection baseconn = JsonConvert.DeserializeObject<POCO.Connections.BaseConnection>(connectionData);
            if (string.IsNullOrEmpty(baseconn.ManagedAccountId))
            {
                // No managed account set - return blank string
                return string.Empty;
            }
            else
            {
                // TODO load from identity provider instead of Docker secret
                string connectionFromStore = Utilities.Configuration.GetSecretOrEnvVarAsString(baseconn.ManagedAccountId);
                if (string.IsNullOrEmpty(connectionFromStore))
                {
                    // No matching connection for this managed account
                    return string.Empty;
                }
                else
                {
                    return connectionFromStore;
                }
            }
        }

        public static string GetConfigPath()
        {
            string configPath = string.Empty;

            // Linux secrets path
            const string DOCKER_SECRET_PATH_LINUX = "/run/secrets/";
            if (Directory.Exists(DOCKER_SECRET_PATH_LINUX))
            {
                configPath = DOCKER_SECRET_PATH_LINUX;
                return configPath;
            }

            // Windows secrets path
            const string DOCKER_SECRET_PATH_WINDOWS = "C:\\run\\secrets\\";
            if (Directory.Exists(DOCKER_SECRET_PATH_WINDOWS))
            {
                configPath = DOCKER_SECRET_PATH_WINDOWS;
                return configPath;
            }

            // Use local Application Data path
            string localSecretsPath = Environment.GetEnvironmentVariable("CASTLEPOINT_SECRETS_PATH");
            if (string.IsNullOrEmpty(localSecretsPath))
            {
                localSecretsPath = "Castlepoint\\cpdev1";
                configPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), localSecretsPath);
            }
            else
            {
                // Check if path is fully qualified
                if (System.IO.Path.IsPathFullyQualified(localSecretsPath))
                {
                    configPath = localSecretsPath;
                }
                else
                {
                    configPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), localSecretsPath);
                }
            }

            // Validate the directory path
            if (!Directory.Exists(configPath))
            {
                throw new ApplicationException("GetConfigPath path does not exist: " + configPath);
            }

            return configPath;
        }


        public static string GetSecretOrEnvVarAsString(string key)
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

                return string.Empty;
            }

            return string.Empty;

        }

        public static string GetSecretOrEnvVarAsString(string key, ILogger logger)
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
                logger.LogError("Error getting environment variable: " + key + " (" + ex.Message + ")");
                return string.Empty;
            }

            return string.Empty;

        }

        public static byte[] GetSecretOrEnvVarAsByte(string key, ILogger logger)
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
                logger.LogError("Error getting environment variable: " + key + " (" + ex.Message + ")");
                return bytes;
            }

            return bytes;

        }
    }
}
