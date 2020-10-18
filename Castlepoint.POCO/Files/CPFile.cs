using System;
using System.Collections.Generic;
using System.Text;
using Castlepoint.POCO.Config;
using Microsoft.Extensions.Logging;

using Microsoft.Extensions.Options;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Castlepoint.POCO.Files
{

    public class CPFile:IFile
    {

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public DateTime CreationTime { get; set; }
        public string CreatedBy { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public string LastModifiedBy { get; set; }
        public string Name { get; set; }
        public string SourceFileName { get; set; }
        public string SourceRelativeUrl { get; set; }
        public string ServerRelativeUrl { get; set; }
        public virtual string SiteUrl { get; set; }
        public virtual string FolderUri { get; set; }
        public virtual string ItemUri { get; set; }
        public string Type { get; set; }
        public string Title { get; set; }
        public string BatchStatus { get; set; }
        public Guid BatchGuid { get; set; }
        public string JsonFileProcessResult { get; set; }
        public Guid OrganisationId { get; set; }
        public string MIMEType { get; set; }
        public long SizeInBytes { get; set; }

        public virtual string GetBatchStatus(DbConnectionConfig cpConfig, ILogger logger)
        {
            throw new NotImplementedException();
        }
        public virtual long GetFileLength(DbConnectionConfig cpConfig, System system, ILogger logger)
        {
            return -1;
        }

        public virtual byte[] GetFileBytes(DbConnectionConfig cpConfig, System system, ILogger logger)
        {
            throw new NotImplementedException();
        }


        public virtual string GetFileExtension()
        {
            throw new NotImplementedException();
        }

        public virtual string GetFilePermissions(DbConnectionConfig cpConfig, System system, ILogger logger)
        {
            throw new NotImplementedException();
        }


        public virtual SecClass GetFileSecurityClassification(DbConnectionConfig cpConfig, System system, ILogger _logger)
        {
            throw new NotImplementedException();
        }


        public virtual bool SaveFileProcessStatus(DbConnectionConfig cpConfig, Guid batchGuid, Dictionary<string, string> fileProcessStatus, ILogger logger)
        {
            throw new NotImplementedException();
        }



        public virtual bool SaveMIMEType(DbConnectionConfig cpConfig, ILogger _logger)
        {
            throw new NotImplementedException();
        }

    }

    public class SecClass : TableEntity
    {
        public SecClass()
        {
            this.SecurityClassification = "";
            this.DLM = "";
        }
        public string SecurityClassification { get; set; }
        public string DLM { get; set; }
    }
}
