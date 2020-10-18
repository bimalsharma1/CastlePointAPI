using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Localization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Localization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
//using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Newtonsoft.Json;

using System.Diagnostics;

using Microsoft.OpenApi.Models;

namespace Castlepoint.REST
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // Load the service configuration file
            Utils.ServiceConfig svccfg = Utils.GetServiceConfig();

            // Load configuration values
            Console.WriteLine("Loading Allowed CORS URLs...");
            if (svccfg.url_allowed_cors==null || svccfg.url_allowed_cors.Length == 0)
            {
                throw new ApplicationException("Invalid configuration entry: allowed_cors");
            }
            Console.WriteLine("CORS Url(s): " + svccfg.url_allowed_cors);
            string[] corsUrls = svccfg.url_allowed_cors.Split(",", StringSplitOptions.RemoveEmptyEntries);

            Console.WriteLine("Loading Identity Server Url...");
            if (svccfg.url_identity_server == null || svccfg.url_identity_server.Length == 0)
            {
                throw new ApplicationException("Invalid configuration entry: url_identity_server");
            }
            svccfg.url_identity_server = "http://localhost:5000"; // Vikas
            Console.WriteLine("Identity Server Url: " + svccfg.url_identity_server);

            // Check for any startup commands
            if (svccfg.startup_commands != null && svccfg.startup_commands.Count > 0)
            {
                // Process each command
                foreach (Utils.StartupCommand command in svccfg.startup_commands)
                {
                    Console.WriteLine("Starting process: " + command.command);
                    Process startProcess = Process.Start(command.command, command.arguments);
                    Console.WriteLine("Waiting 10 seconds...");
                    startProcess.WaitForExit(10000);
                    if (!startProcess.HasExited)
                    {
                        Console.WriteLine("WARNING: Startup process not completed within 10 seconds, continuing startup functions");
                    }
                    else
                    {
                        Console.WriteLine("Startup process finished (NOTE: does not indicate success!)");
                    }
                }
            }

            Console.WriteLine("Adding logging...");

            // Enable logging of identity model events that may contain PII (required for .well-known debugging issues)
            Microsoft.IdentityModel.Logging.IdentityModelEventSource.ShowPII = true;

            services.AddLogging();

            // Add CORS
            Console.WriteLine("Adding CORS...");
            services.AddCors(options =>
            {
                //options.AddPolicy("AllowAllCors",
                //    builder => builder.AllowAnyOrigin()
                //                    .AllowAnyMethod()
                //                    .AllowAnyHeader()
                //                    .AllowCredentials()
                //                    );

                // "https://atsmeptyltd.sharepoint.com","https://stlpcastlepoint.sharepoint.com","http://localhost:6060", https://web.stlp.castlepoint.systems
                options.AddPolicy("AllowRestrictedCors",
                    builder => builder.WithOrigins(svccfg.url_allowed_cors)
                                    .AllowAnyMethod()
                                    .AllowAnyHeader()
                                    .AllowCredentials()
                                    );
            });

            // Add Authentication
            Console.WriteLine("Adding Authentication...");
            services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                // base-address of your identityserver
                options.Authority = svccfg.url_identity_server;

                // if you are using API resources, you can specify the name here
                //options.Audience = "castlepoint";

                // IdentityServer emits a typ header by default, recommended extra check
                options.TokenValidationParameters.ValidTypes = new[] { "at+jwt" };

                // Remove audience validation
                options.TokenValidationParameters.ValidateAudience = false;
            });

            // GM 20200727
            // Removed legacy ID4 v3 code
            //services.AddAuthentication(options =>
            //{
            //    options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
            //    options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;

            //})
                //.AddIdentityServerAuthentication(options =>
                //{
                //    options.Authority = svccfg.url_identity_server;
                //    options.RequireHttpsMetadata = false;
                //    options.ApiName = "castlepoint";
                //});

            // Add localization
            services.AddLocalization(options => options.ResourcesPath = "Resources");

            // Configure MVC with Authorization
            Console.WriteLine("Adding authorization...");
            services.AddMvcCore(options => options.EnableEndpointRouting = false)
                    .AddAuthorization()
                    .AddNewtonsoftJson()
                    .AddMvcLocalization()
                    .AddApiExplorer();  // Add the Swagger API explorer

            // Add Swagger documentation generation
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "Castlepoint API", Version = "v1" });
            });
            services.AddSwaggerGenNewtonsoftSupport(); // explicit opt-in - needs to be placed after AddSwaggerGen()
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory logFactory)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            using (logFactory = LoggerFactory.Create(builder => builder.AddConsole()))
            {
                // use loggerFactory
            }

            //call ConfigureLogger in a centralized place in the code
            //ApplicationLogging.ConfigureLogger(loggerFactory);
            //set it as the primary LoggerFactory to use everywhere
            //ApplicationLogging.LoggerFactory = loggerFactory;

            //app.UseErrorLogging();

            // Apply CORS config
            app.UseCors("AllowRestrictedCors");

            // Apply authentication config
            app.UseAuthentication();

            // Add localization
            var supportedCultures = new[]
            {
                new CultureInfo("en-US"),
                new CultureInfo("fr"),
                new CultureInfo("es"),
            };

            app.UseRequestLocalization(new RequestLocalizationOptions
            {
                DefaultRequestCulture = new RequestCulture("en-US"),
                // Formatting numbers, dates, etc.
                SupportedCultures = supportedCultures,
                // UI strings that we have localized.
                SupportedUICultures = supportedCultures
            });

            app.UseMvc();

            // Add Swagger documentation generation
            app.UseSwagger();

            // Add Swagger documentation UI portal
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "Castlepoint API V1");
            });
        }
    }

}
