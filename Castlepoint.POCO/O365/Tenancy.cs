using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.O365
{
    public class AzureADConnectionConfig
    {
        public string Label { get; set; }
        public string TenantId { get; set; }
        public string UrlTenant { get; set; }
        public string ClientId { get; set; }
        public string CertificatePath { get; set; }
        public string CertificateFileName { get; set; }
        public string CertificatePassword { get; set; }
    }

    public class ConnectionConfig
    {
        /// <summary>
        /// Guid of the connection file
        /// </summary>
        public string ConnectionFileId { get; set; }
        public string ConnectionType { get; set; }
        /// <summary>
        /// URL for the resource to connect to
        /// </summary>
        public string ResourceUrl { get; set; }
    }
}
