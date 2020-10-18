using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Castlepoint.REST
{
    static class Program
    {

        static void Main(string[] args)
        {
            CreateWebHostBuilder(args).Build().Run();
        }

        static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            //TODO this should be dynamically loaded from Secrets
            var dict = new Dictionary<string, string>
            {
                {"data_storage_service_type", "internal.mongodb"},
                {"connection_string", "mongodb://localhost:27017"}
            };

            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(dict)
                .Build();

            // Show the configuration path
            Console.WriteLine("Configuration file path: " + Utils.GetConfigPath());

            // Get the configured environment variable
            string environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            // Check for configured HTTPS ports
            string httpsOverridePort = Environment.GetEnvironmentVariable("OVERRIDE_HTTPS_PORT");
            if (string.IsNullOrEmpty(httpsOverridePort))
            {
                httpsOverridePort = "443";
            }
            else
            {
                Console.WriteLine("HTTPS Override environment variable found: " + httpsOverridePort);
            }
            int httpsPort = int.Parse(httpsOverridePort);

            // Configure ssl file name and password
            string sslFileName = "castlepoint.pfx";
            string sslPasswordName = "castlepoint.pfx.pwd";
            if (environment=="Development")
            {
                Console.WriteLine("Development environment detected - setting developer SSL certificate");
                sslFileName = "castlepoint-localhost.pfx";
                sslPasswordName = "castlepoint-localhost.pfx.pwd";
            }

            // load web certificate
            Console.WriteLine("Loading SSL certificate files...");
            byte[] signingCert = Utils.GetSecretOrEnvVarAsByte(sslFileName);
            if (signingCert == null || signingCert.Length == 0)
            {
                throw new ApplicationException("Invalid SSL certificate. Check the configuration of: " + sslFileName);
            }
            Console.WriteLine("Cert file loaded length=" + signingCert.Length.ToString());

            string certPassword = Utils.GetSecretOrEnvVar(sslPasswordName);
            if (string.IsNullOrEmpty(certPassword))
            {
                throw new ApplicationException("Invalid SSL password. Check the configuration of: " + sslPasswordName);
            }
            else
            {
                certPassword = certPassword.Trim();
            }
            Console.WriteLine("Cert pwd loaded, converting to SecureString...");
            System.Security.SecureString secpwd = new System.Security.SecureString();
            foreach (char c in certPassword.ToCharArray())
            {
                secpwd.AppendChar(c);
            }


            System.Security.Cryptography.X509Certificates.X509Certificate2 cert;
            try
            {
                Console.WriteLine("Loading SSL certificate with key...");
                cert = new System.Security.Cryptography.X509Certificates.X509Certificate2(signingCert, secpwd, System.Security.Cryptography.X509Certificates.X509KeyStorageFlags.MachineKeySet);

                Console.WriteLine("SSL certificate loaded");

                Console.WriteLine("Configuring web host...");

                return WebHost.CreateDefaultBuilder(args)
                .UseConfiguration(config)
                .ConfigureLogging((context, builder) =>
                {
                    builder.AddConsole();
                    builder.AddDebug();
                })
                .UseStartup<Startup>()
                .UseKestrel(options =>
                {
                    // Switch based on environment
                    Console.WriteLine("==> Environment detected:" + environment);
                    switch (environment)
                    {
                        case "Production":
                            //options.ListenUnixSocket("/tmp/kestrel-castlepoint-api.sock");
                            //options.ListenUnixSocket("/tmp/kestrel-castlepoint-api.sock", listenOptions =>
                            //{
                            //    listenOptions.UseHttps(cert);
                            //});
                            Console.WriteLine("Starting Web listen service on port " + httpsOverridePort + "...");
                            options.Listen(System.Net.IPAddress.Any, httpsPort, listenOptions =>
                            {
                                listenOptions.UseHttps(cert);
                            });
                            break;
                        case "Staging":
                            //options.ListenUnixSocket("/tmp/kestrel-castlepoint-api.sock");
                            //options.ListenUnixSocket("/tmp/kestrel-castlepoint-api.sock", listenOptions =>
                            //{
                            //    listenOptions.UseHttps(cert);
                            //});
                            Console.WriteLine("Starting Web listen service on port " + httpsOverridePort + "...");
                            options.Listen(System.Net.IPAddress.Any, httpsPort, listenOptions =>
                            {
                                listenOptions.UseHttps(cert);
                            });
                            break;
                        case "Development":
                            Console.WriteLine("Starting Web listen service on port 5001...");

                            options.Listen(System.Net.IPAddress.Any, 5001, listenOptions =>
                            {
                                listenOptions.UseHttps(cert);
                            });

                            break;
                    }
                });

            }
            catch (Exception ex)
            {
                string exceptionMsg = ex.Message;
                Console.WriteLine("Error loading SSL certificate: " + exceptionMsg);
                throw;
            }




            //    Console.WriteLine("Loading SSL certificate...");
            //var cert = new System.Security.Cryptography.X509Certificates.X509Certificate2(signingCert, certPassword);
            //Console.WriteLine("SSL certificate loaded");


        }
    }

}
