using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace Castlepoint.REST
{
    internal static class ConfigurationProperties
    {
        internal static string HashKey { get { return "hash_key"; } }
        internal static int HashKeyLength { get { return 16; } }
        internal static string AzureStorageAccountConnectionString { get { return "azure_storage_account_connection_string"; } }
        internal static string TikaBaseAddress { get { return "tika_base_address"; } }
        internal static string KeyphraseBaseAddress { get { return "keyphrase_base_address"; } }
        internal static string KeyphraseAccesKey { get { return "keyphrase_access_key"; } }
    }
}
