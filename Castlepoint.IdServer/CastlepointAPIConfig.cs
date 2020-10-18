using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Castlepoint.IdServer
{
    internal class CastlepointIdentityServerConfig
    {
        public string IdentityServerUrl { get; set; }
        public string AllowedCORSUrls { get; set; }
        public string RedirectUrl { get; set; }
        public string PostLogoutUrl { get; set; }
        public string SigningCertificateName { get; set; }
        public string SigningCertificatePassword { get; set; }

    }
}
