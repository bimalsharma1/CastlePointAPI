using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using IdentityServer4;
using IdentityServer4.Models;
using IdentityServer4.Test;
using IdentityModel;
using Newtonsoft.Json;

namespace Castlepoint.IdServer
{
    public class Config
    {
        public static IEnumerable<ApiScope> ApiScopes =>
    new List<ApiScope>
    {
            new ApiScope("castlepoint", "Castlepoint API")
    };

        // GM 20200727 DEPRECATED IdentityServer4
        //public static IEnumerable<ApiResource> GetApiResources()
        //{
        //    return new List<ApiResource>
        //    {
        //        new ApiResource("castlepoint", "Castlepoint API")
        //         {
        //            UserClaims =  { JwtClaimTypes.Name, JwtClaimTypes.Email, JwtClaimTypes.Id, JwtClaimTypes.JwtId, JwtClaimTypes.Subject }
        //        }
        //    };
        //}

        internal static IEnumerable<Client> GetClients(CastlepointIdentityServerConfig cpIdentityConfig)
        {
            return new List<Client>
    {
        new Client
        {
            ClientId = "js",
            ClientName = "Castlepoint Javascript",
            AllowedGrantTypes = GrantTypes.Implicit,
            AllowAccessTokensViaBrowser = true,
            RedirectUris =           { cpIdentityConfig.RedirectUrl },
            PostLogoutRedirectUris = { cpIdentityConfig.PostLogoutUrl },
            AllowedCorsOrigins =     { cpIdentityConfig.AllowedCORSUrls },

            // scopes that client has access to
            AllowedScopes =
            {
                IdentityServerConstants.StandardScopes.OpenId,
                IdentityServerConstants.StandardScopes.Profile,
                "castlepoint"
            }
        },
        // OpenID Connect implicit flow client (MVC)
        new Client
        {
            ClientId = "mvc",
            ClientName = "MVC Client",
            AllowedGrantTypes = GrantTypes.Implicit,

            // where to redirect to after login
            RedirectUris = { "http://localhost:5002/signin-oidc" },

            // where to redirect to after logout
            PostLogoutRedirectUris = { "http://localhost:5002/signout-callback-oidc" },

            AllowedScopes = new List<string>
            {
                IdentityServerConstants.StandardScopes.OpenId,
                IdentityServerConstants.StandardScopes.Profile,
                "castlepoint"
            }
        }

    };
        }

        public static IEnumerable<IdentityResource> GetIdentityResources()
        {
            return new List<IdentityResource>
    {
        new IdentityResources.OpenId(),
        new IdentityResources.Profile()
    };
        }

        public class User
        {
            public User() { }
            public string SubjectId { get; set; }
            public string Username { get; set; }
            public string Password { get; set; }
            public List<UserClaim> Claims { get; set; }
        }
        public class UserClaim
        {
            public UserClaim() { } 
            public string Type { get; set; }
            public string Value { get; set; }
        }

        public static List<TestUser> GetUsers()
        {

            List<TestUser> users = new List<TestUser>();

            // Load admin user accounts
            Console.WriteLine("Loading admin user configuration...");
            string adminUsersJson = Utils.GetSecretOrEnvVarAsString(Utils.SecretNames.AdminUsers);

            if (adminUsersJson==null || adminUsersJson=="")
            {
                Console.WriteLine("WARNING: admin user configuration not found, no admin users created: " + Utils.SecretNames.AdminUsers);
                return users;
            }
            List<User> adminUsers = JsonConvert.DeserializeObject<List<User>>(adminUsersJson);

            foreach (User u in adminUsers)
            {
                TestUser tu = new TestUser();
                tu.SubjectId = u.SubjectId;
                tu.Username = u.Username;
                tu.Password = u.Password;
                foreach (UserClaim uc in u.Claims)
                {
                    Claim c = new Claim(uc.Type, uc.Value);
                    tu.Claims.Add(c);
                }

                // Add the user to the list of admin users
                users.Add(tu);
            }

            Console.WriteLine("Admin users found: " + users.Count.ToString());


            return users;

        }
    }
}
