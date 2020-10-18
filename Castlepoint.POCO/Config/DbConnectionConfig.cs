using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Config
{
    public class DbConnectionConfig
    {
        public string data_storage_service_type { get; set; }
        public string azure_storage_account_connection_string { get; set; }
        public string connection_string { get; set; }
        public string database_name { get; set; }
    }
}
