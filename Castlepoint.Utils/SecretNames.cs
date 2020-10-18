using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.Utilities
{
    public static class SecretNames
    {
        public static string ClientConfigFile { get { return "batch_client_config"; } }
        public static string ClientConfigFilePrefix { get { return "batch_client_config_"; } }
        public static string BatchServiceConfigFile { get { return "batch_service_config"; } }
        public static string O365NumDaysLogFiles { get { return "o365_log_file_days"; } }

        // Throttles
        public static string ThrottleMaxTika { get { return "throttle_max_tika_threads"; } }
        public static string ThrottleMaxMachinebox { get { return "throttle_max_machinebox_threads"; } }
        public static string ThrottleMaxProcessFiles { get { return "throttle_max_processfiles_threads"; } }

        public static string O365CastlepointApp { get { return "batch_o365_app_registration"; } }

        public static string ProxyServer { get { return "proxy_server_address"; } }
        public static string ProxyServerUserName { get { return "proxy_server_username"; } }
        public static string ProxyServerPasssword { get { return "proxy_server_password"; } }
    }
}
