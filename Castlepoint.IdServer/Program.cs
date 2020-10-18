using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Castlepoint.IdServer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateWebHostBuilder(args).Build().Run();
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            // Show the configuration path
            Console.WriteLine("Configuration file path: " + Utils.GetConfigPath());

            // Get the configured environment variable
            string environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            // Check for configured HTTPS ports
            string httpsOverridePort = Environment.GetEnvironmentVariable("OVERRIDE_HTTPS_PORT");
            if (httpsOverridePort == null || httpsOverridePort == string.Empty)
            {
                httpsOverridePort = "443";
            }
            else
            {
                Console.WriteLine("HTTPS Override environment variable found: " + httpsOverridePort);
            }
            int httpsPort = int.Parse(httpsOverridePort);

            // Configure ssl file name ans passowrd
            string sslFileName = "castlepoint.pfx";
            string sslPasswordName = "castlepoint.pfx.pwd";
            if (environment == "Development")
            {
                Console.WriteLine("Development environment detected - setting developer SSL certificate");
                sslFileName = "castlepoint-localhost.pfx";
                sslPasswordName = "castlepoint-localhost.pfx.pwd";
            }

            // load web certificate
            Console.WriteLine("Loading SSL certificate files...");
            byte[] signingCert = Utils.GetSecretOrEnvVarAsByte(sslFileName);
            if (signingCert==null || signingCert.Length==0)
            {
                throw new ApplicationException("Invalid SSL certificate. Check the configuration of: " + sslFileName);
            }
            string certPassword = Utils.GetSecretOrEnvVarAsString(sslPasswordName);
            if (certPassword == null || certPassword.Length == 0)
            {
                throw new ApplicationException("Invalid SSL password. Check the configuration of: " + sslPasswordName);
            }
            else
            {
                certPassword = certPassword.Trim();
            }
            Console.WriteLine("Loading SSL certificate...");
            var cert = new System.Security.Cryptography.X509Certificates.X509Certificate2(signingCert, certPassword);
            Console.WriteLine("SSL certificate loaded");

            return WebHost.CreateDefaultBuilder(args)
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
                            Console.WriteLine("Starting Web listen service on port 5000...");
                            options.Listen(System.Net.IPAddress.Any, 5000, listenOptions =>
                            {
                                listenOptions.UseHttps(cert);
                            });

                            break;
                    }
                });
        }
    }
}
