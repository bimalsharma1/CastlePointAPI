using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Connections
{
    public class SharePointADConfiguration:BaseConnection
    {
        public string UrlSiteCollection { get; set; }
        public string Domain { get; set; }
        public string UserId { get; set; }
        public string Password { get; set; }
    }
}
