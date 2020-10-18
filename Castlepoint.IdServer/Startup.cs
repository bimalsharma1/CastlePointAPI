using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
//using Microsoft.Owin.Security.OpenIdConnect;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using IdentityServer4.Extensions;
using IdentityServer4;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Mvc.Authorization;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authentication.AzureAD.UI;
using Microsoft.AspNetCore.Authentication;
using Microsoft.IdentityModel.Tokens;
using System.Text;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.AspNetCore.Identity;

namespace Castlepoint.IdServer
{
    public class Startup
    {

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            // Load configuration
            CastlepointIdentityServerConfig cpIDConfig = Utils.GetCPIDConfig();
            if (cpIDConfig==null)
            {
                throw new ApplicationException("ERROR: service configuration file is null, cannot start service");
            }
            string corsUrl = cpIDConfig.AllowedCORSUrls.Trim();
            Console.WriteLine("CORS: " + corsUrl);

            // Configure CORS
            services.AddCors(options =>
            {
                // this defines a CORS policy called "default"
                options.AddPolicy("default", policy =>
                {
                    policy.WithOrigins(corsUrl)
                        .AllowAnyHeader()
                        .AllowAnyMethod();
                });
            });

            // load signing credential certificate
            Console.WriteLine("Loading signing certificate...");
            byte[] signingCert = Utils.GetSecretOrEnvVarAsByte(cpIDConfig.SigningCertificateName);
            var cert = new System.Security.Cryptography.X509Certificates.X509Certificate2(signingCert, cpIDConfig.SigningCertificatePassword);


            // configure identity server with in-memory stores, keys, clients and scopes
            services.AddIdentityServer(options =>
            {

                Console.WriteLine("Checking for ISSUER_URI...");
                string issuerUri = string.Empty;
                if (Environment.GetEnvironmentVariable("ISSUER_URI") != null && Environment.GetEnvironmentVariable("ISSUER_URI") != "")
                {
                    issuerUri = Environment.GetEnvironmentVariable("ISSUER_URI").Trim();
                    Console.WriteLine("ISSUER_URI=" + issuerUri);
                    options.IssuerUri = issuerUri;
                    Console.WriteLine("options.IssuerUri=" + issuerUri);
                }

            })
                .AddSigningCredential(cert)
                //.AddInMemoryApiResources(Config.GetApiResources())
                .AddInMemoryApiScopes(Config.ApiScopes)
                .AddInMemoryIdentityResources(Config.GetIdentityResources())
                .AddInMemoryClients(Config.GetClients(cpIDConfig))
                .AddTestUsers(Config.GetUsers());
            services.Configure<CookiePolicyOptions>(options =>
            {
                // This lambda determines whether user consent for non-essential cookies is needed for a given request.
                options.CheckConsentNeeded = context => true;
                options.MinimumSameSitePolicy = SameSiteMode.None;
            });

            services.AddMvc(options =>
            {
                var policy = new AuthorizationPolicyBuilder()
                    .RequireAuthenticatedUser()
                    .Build();
                options.Filters.Add(new AuthorizeFilter(policy));
                options.EnableEndpointRouting = false;
            });

            services.AddAuthentication(sharedOptions =>
            {
                //sharedOptions.DefaultAuthenticateScheme = CookieAuthenticationDefaults.AuthenticationScheme;
                //sharedOptions.DefaultChallengeScheme = CookieAuthenticationDefaults.AuthenticationScheme;
                //sharedOptions.DefaultSignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;

            })

           // .AddAzureAD(options => Configuration.Bind("AzureAd", options))
           .AddCookie()
           //.AddOpenIdConnect("adfs", "ADFS authentication", options =>
           //{
           //    //options.MetadataAddress = "https://localhost:5000/.well-known/openid-configuration";
           //    //options.SignInScheme = IdentityServerConstants.ExternalCookieAuthenticationScheme; ;
           //    //options.Authority = "https://localhost:5001";
           //    //options.RequireHttpsMetadata = false;

           //    //options.ResponseType = OpenIdConnectResponseType.Code;
           //    //options.UsePkce = true;
           //    //options.Scope.Clear();
           //    //options.Scope.Add("openid");
           //    //options.Scope.Add("profile");
           //    //options.Scope.Add("email");
           //    //options.SaveTokens = true;
           //    //options.MetadataAddress = "https://localhost:5000/adfs/.well-known/openid-configuration";
           //    //options.ClientId = "aa788ef2-99ba-4dd9-97e5-de98ea797ef4";

           //    options.SignInScheme = IdentityServerConstants.ExternalCookieAuthenticationScheme;
           //    options.SignOutScheme = IdentityServerConstants.SignoutScheme;

           //    options.Authority = $"https://login.microsoftonline.com/976fba20-6af6-4743-8a64-13886793c542/oauth2/";
           //    options.ClientId = "5382774d-442b-424c-bbb2-0025bf050728";
           //    options.ResponseType = OpenIdConnectResponseType.Code;

           //    options.CallbackPath = "/signin-adfs";
           //    options.SignedOutCallbackPath = "/signout-callback-adfs";
           //    options.RemoteSignOutPath = "/signout-adfs";
           //    options.TokenValidationParameters = new TokenValidationParameters
           //    {
           //        NameClaimType = "name",
           //        RoleClaimType = "role"
           //    };
           //})

           .AddOpenIdConnect("azuread", "Login with Azure AD", options =>
           {
               options.Authority = $"https://login.microsoftonline.com/common";
               options.TokenValidationParameters =
                        new TokenValidationParameters { ValidateIssuer = false };
               options.ClientId = "aa788ef2-99ba-4dd9-97e5-de98ea797ef4";
               options.CallbackPath = "/signin-oidc";
               options.SignInScheme = IdentityServerConstants.ExternalCookieAuthenticationScheme;
           });
            //.AddOpenIdConnect("AzureAD", "AzureAD", options =>
            //{
            //   // Configuration.GetSection("AzureAD").Bind(options); ;
            //    options.ResponseType = OpenIdConnectResponseType.CodeIdToken;
            //    options.RemoteAuthenticationTimeout = TimeSpan.FromSeconds(120);
            //    options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            //    options.RequireHttpsMetadata = false;
            //    options.SaveTokens = true;
            //});

            services.Configure<OpenIdConnectOptions>("azuread", options =>
            {
                options.Scope.Add("openid");
                options.Scope.Add("profile");
                options.Events = new OpenIdConnectEvents()
                {
                    OnRedirectToIdentityProviderForSignOut = context =>
                    {
                        context.HandleResponse();
                        context.Response.Redirect("/Account/Logout");
                        return Task.FromResult(0);
                    }
                };
            });

            services.Configure<OpenIdConnectOptions>("adfs", options =>
            {
                options.Scope.Add("openid");
                options.Scope.Add("profile");
                options.Events = new OpenIdConnectEvents()
                {
                    OnRedirectToIdentityProviderForSignOut = context =>
                    {
                        context.HandleResponse();
                        context.Response.Redirect("/Account/Logout");
                        return Task.FromResult(0);
                    }
                };
            });


            //services.Configure<OpenIdConnectOptions>(AzureADDefaults.OpenIdScheme, options =>
            //{
            //    options.Authority = options.Authority + "/v2.0/";         // Microsoft identity platform

            //    options.TokenValidationParameters.ValidateIssuer = false; // accept several tenants (here simplified)
            //});



        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {

            app.UseCors("default");

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.Use((context, next) =>
            {
                if (Environment.GetEnvironmentVariable("SSL_OFFLOAD") == "true")
                    context.Request.Scheme = "https";

                return next();
            });

            //string publicFacingUrl = string.Empty;
            //if (Environment.GetEnvironmentVariable("PUBLIC_FACING_URL") != null && Environment.GetEnvironmentVariable("PUBLIC_FACING_URL") != "")
            //{
            //    publicFacingUrl = Environment.GetEnvironmentVariable("PUBLIC_FACING_URL");
            //    app.UseMiddleware<PublicFacingUrlMiddleware>(publicFacingUrl);
            //    app.UseMiddleware<IdentityServerMiddleware>();
            //}


            app.UseIdentityServer();

            app.UseStaticFiles();
            app.UseMvcWithDefaultRoute();

        }

    }

    /// <summary>
    /// Configures the HttpContext by assigning IdentityServerOrigin.
    /// </summary>
    //public class PublicFacingUrlMiddleware
    //{
    //    private readonly RequestDelegate _next;
    //    private readonly string _publicFacingUri;

    //    public PublicFacingUrlMiddleware(RequestDelegate next, string publicFacingUri)
    //    {
    //        _publicFacingUri = publicFacingUri;
    //        _next = next;
    //    }

    //    public async Task Invoke(HttpContext context)
    //    {
    //        var request = context.Request;

    //        context.SetIdentityServerOrigin(_publicFacingUri);
    //        context.SetIdentityServerBasePath(request.PathBase.Value.TrimEnd('/'));

    //        await _next(context);
    //    }
    //}

}
