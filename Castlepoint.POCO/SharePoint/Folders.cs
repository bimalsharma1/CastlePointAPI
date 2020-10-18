using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.SharePoint
{
    public class SPFolderUpdate
    {
        public SPFolderUpdate() { }
        public SPFolderUpdate(string pkey, string rkey)
        {
            this.PartitionKey = pkey;
            this.RowKey = rkey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string CPFolderStatus { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }

    }
    public class SPFolder : FolderResult
    {
        public SPFolder() { }
        public SPFolder(string pkey, string rkey)
        {
            this.PartitionKey = pkey;
            this.RowKey = rkey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string CPFolderStatus { get; set; }
    }

    public class FolderResultJson
    {
        public FolderNamespace d { get; set; }
    }
    public class FolderNamespace
    {
        public List<FolderResult> results { get; set; }

    }
    public class FolderResult
    {
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string Path { get; set; }
        public string ServerRelativeUrl { get; set; }
        public string WelcomePage { get; set; }
    }
}
