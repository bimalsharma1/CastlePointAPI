using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Castlepoint.Text
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateWebHostBuilder(args).Build().Run();
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            // Get the configured environment variable
            string environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            return WebHost.CreateDefaultBuilder(args)
                    .UseStartup<Startup>()
                .UseKestrel(options =>
                {
                    // No limit on Request Body size
                    options.Limits.MaxRequestBodySize = null;

                    // Switch based on environment
                    Console.WriteLine("==> Environment detected:" + environment);

                    // Default port number for the Text service
                    int portNumber = 8398;

                    // Check which environment is set
                    switch (environment.ToLower())
                    {
                        case "production":

                            Console.WriteLine("Starting Web listen service on port " + portNumber.ToString());
                            options.Listen(System.Net.IPAddress.Any, portNumber);
                            break;

                        case "staging":

                            Console.WriteLine("Starting Web listen service on port " + portNumber.ToString());
                            options.Listen(System.Net.IPAddress.Any, portNumber);
                            break;

                        case "development":
                            portNumber = 6002;
                            Console.WriteLine("Starting Web listen service on port " + portNumber.ToString());
                            options.Listen(System.Net.IPAddress.Any, portNumber);

                            break;
                    }
                });
        }
    }
}
