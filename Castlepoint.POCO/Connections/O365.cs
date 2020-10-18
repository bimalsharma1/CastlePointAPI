using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Connections
{
    public class AzureCertificateConnectionConfig:BaseConnection
    {
        public string TenantId { get; set; }
        public string UrlTenant { get; set; }
        public string UrlSiteCollection { get; set; }
        public string ClientId { get; set; }
        public string CertificatePath { get; set; }
        public string CertificateFileName { get; set; }
        public string CertificatePassword { get; set; }

    }
}
