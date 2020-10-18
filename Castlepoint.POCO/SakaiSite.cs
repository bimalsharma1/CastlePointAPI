using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class SakaiSite
    {
        public string SITE_ID { get; set; }
        public string TITLE { get; set; }
        public string TYPE { get; set; }
        public string SHORT_DESC { get; set; }
        public string DESCRIPTION { get; set; }
        public string ICON_URL { get; set; }
        public string INFO_URL { get; set; }
        public string SKIN { get; set; }
        public int PUBLISHED { get; set; }
        public string JOINABLE { get; set; }
        public string PUBVIEW { get; set; }
        public string JOIN_ROLE { get; set; }
        public string CREATEDBY { get; set; }
        public string MODIFIEDBY { get; set; }
        public DateTime CREATEDON { get; set; }
        public DateTime MODIFIEDON { get; set; }
        public string IS_SPECIAL { get; set; }
        public string IS_USER { get; set; }
        public string CUSTOM_PAGE_ORDERED { get; set; }
    }

    public class SakaiContentResource
    {
        public string RESOURCE_ID { get; set; }
        public string RESOURCE_UUID { get; set; }
        public string IN_COLLECTION { get; set; }
        public string FILE_PATH { get; set; }
        public int FILE_SIZE { get; set; }
        public string CONTEXT { get; set; }
        public string RESOURCE_TYPE_ID { get; set; }
        public string FileName { get; set; }
        public DateTime TimeCreated { get; set; }
        public string CreatedBy { get; set; }
        public DateTime TimeLastModified { get; set; }
        public string LastModifiedBy { get; set; }
    }

    public class SakaiWikiPage
    {
        public string id { get; set; }
        public string rwikiid { get; set; }
        public string content { get; set; }
        public string version { get; set; }
        public string name { get; set; }
        public string realm { get; set; }
        public string referenced { get; set; }
    }

    public class SakaiSiteEntity : AbstractDataEntity
    {
        public SakaiSiteEntity() { }
        public SakaiSiteEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.UniqueId = "";
            this.TimeCreated = Utils.AzureTableMinDateTime;
            this.TimeLastModified = Utils.AzureTableMinDateTime;
            this.ItemCount = 0;
            this.Name = "";
            this.Version = 0;
            this.CPStatus = "";
        }
    }

    public class SakaiFileBatchStatus
    {
        public SakaiFileBatchStatus() { }
        public SakaiFileBatchStatus(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public Guid BatchGuid { get; set; }
        public string BatchStatus { get; set; }
        public string JsonFileProcessResult { get; set; }
    }

    public class SakaiDocumentEntity : AbstractDataEntity
    {
        public SakaiDocumentEntity() { }
        public SakaiDocumentEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.UniqueId = "";
            this.TimeCreated = Utils.AzureTableMinDateTime;
            this.TimeLastModified = Utils.AzureTableMinDateTime;
            this.ItemCount = 0;
            this.Name = "";
            this.Version = 0;
            this.CPStatus = "";
        }
        public string Title { get; set; }
    }
}
