using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.Extensions.Logging;

namespace Castlepoint.POCO.Files
{
    interface IFile
    {
        byte[] GetFileBytes(Config.DbConnectionConfig cpConfig, POCO.System system, ILogger logger);
        string GetFilePermissions(Config.DbConnectionConfig cpConfig, POCO.System system, ILogger logger);
        SecClass GetFileSecurityClassification(Config.DbConnectionConfig cpConfig, POCO.System system, ILogger _logger);

        string Name { get; set; }
        long SizeInBytes { get; set; }
        string GetFileExtension();
        bool SaveFileProcessStatus(Config.DbConnectionConfig cpConfig, Guid batchGuid, Dictionary<string, string> fileProcessStatus, ILogger logger);
        bool SaveMIMEType(Config.DbConnectionConfig cpConfig, ILogger _logger);
        string GetBatchStatus(Config.DbConnectionConfig cpConfig, ILogger logger);
    }
}
