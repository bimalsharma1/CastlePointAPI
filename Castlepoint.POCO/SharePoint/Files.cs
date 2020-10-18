using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.SharePoint
{
    public class SPFileUpdate
    {
        public SPFileUpdate() { }
        public SPFileUpdate(string pkey, string rkey)
        {
            this.PartitionKey = pkey;
            this.RowKey = rkey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string CPFileStatus { get; set; }
        public string Name { get; set; }

    }
    public class SPFile:FileResult
    {
        public SPFile() { }
        public SPFile(string pkey, string rkey)
        {
            this.PartitionKey = pkey;
            this.RowKey = rkey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string CPFileStatus { get; set; }

        // GM 20190814
        // Copied from POCO.SPFile to resolve issues with GetFileInfo having extra properties...
        public int ItemCount { get; set; }
        public string SourceFileName { get; set; }
        public string ItemUri { get; set; }
        public string SiteUrl { get; set; }
        public string SourceRelativeUrl { get; set; }
        public DateTime CreationTime { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public string UniqueId { get; set; }
        public long Version { get; set; }
        public string CPFolderStatus { get; set; }
        public long SizeInBytes { get; set; }
        public Guid OrganisationId { get; set; }
        public string MIMEType { get; set; }    
    }

    public class FileResultJson
    {
        public FileNamespace d { get; set; }
    }
    public class FileNamespace
    {
        public List<FileResult> results { get; set; }

    }
    public class FileResult:POCO.FileBatch
    {
        //public string Author { get; set; }
        public int Length { get; set; }
        //public string ModifiedBy { get; set; }
        public string Name { get; set; }
        public string ServerRelativeUrl { get; set; }
        public string Title { get; set; }
        public string TimeCreated { get; set; }
        public string TimeLastModified { get; set; }
        public string UIVersion { get; set; }
}
}
