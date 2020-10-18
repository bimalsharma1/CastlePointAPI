using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Connections
{
    public class BaseConnection
    {
        /// <summary>
        /// Reference used by the managed account provider to access the account configuration
        /// </summary>
        public string ManagedAccountId { get; set; }
    }
}
