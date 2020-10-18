using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Account
{
    public class Managed
    {
        /// <summary>
        /// Unique id - Guid
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// ISO date time format when created
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// Visual label of account
        /// </summary>
        public string Label { get; set; }
        /// <summary>
        /// Internal name of account. Used to look up account secrets or account service management systems.
        /// </summary>
        public string InternalName { get; set; }
        /// <summary>
        /// Type of account
        /// </summary>
        public string AccountType { get; set; }
        /// <summary>
        /// Type of account service
        /// Used for hosted account details / secret management systems
        /// </summary>
        public string AccountService { get; set; }
    }

    public class AccountType
    {
        public const string AzureActiveDirectory = "azure.ad";
        public const string ActiveDirectory = "ntlm.ad";
    }
}
