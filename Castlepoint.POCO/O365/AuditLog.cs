using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.O365
{
    public class AuditLogEntryImport
    {
        public AuditLogEntryImport() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        //		"CreationTime": "2018-06-03T05:07:28",
        public string CreationTime { get; set; }

        public DateTime CreationDateTime
        {
            get
            {
                DateTime creationDateTime = Utils.AzureTableMinDateTime;
                if (this.CreationTime != null && this.CreationTime != "")
                {
                    // Check if the date will parse
                    try
                    {
                        bool dateValid = DateTime.TryParse(this.CreationTime, out creationDateTime);
                    }
                    catch (Exception exDateConversion)
                    {
                        creationDateTime = Utils.AzureTableMinDateTime;
                    }
                }
                return creationDateTime;
            }
        }

        public string Id { get; set; }
        //        "Operation": "FileModified",
        public string Operation { get; set; }
        //        "OrganizationId": "e1e984d0-afc0-4573-adda-3d327fd23391",
        public string OrganizationId { get; set; }
        //        "RecordType": 6,
        public int RecordType { get; set; }
        //public string ResultStatus { get; set; }
        //        "UserKey": "i:0h.f|membership|10037ffe8358cd7e@live.com",
        public string UserKey { get; set; }
        //        "UserType": 0,
        public int UserType { get; set; }
        //        "Version": 1,
        public int Version { get; set; }
        //        "Workload": "SharePoint",
        public string Workload { get; set; }
        //        "ClientIP": "124.171.97.46",
        public string ClientIP { get; set; }
        //        "ObjectId": "https:\\/\\/stlpconsulting.sharepoint.com\\/TeamSite\\/2 STLP Clients1\\/D2.4 - Information Architecture.docx",
        public string ObjectId { get; set; }
        //        "UserId": "rachaelg@stlpconsulting.com",
        public string UserId { get; set; }
        //        "CorrelationId": "7d636d9e-d0ec-5000-cd1d-11b899989d9d",
        public string CorrelationId { get; set; }
        //        "EventSource": "SharePoint",
        public string EventSource { get; set; }
        //        "ItemType": "File",
        public string ItemType { get; set; }
        //        "ListId": "d94dd849-f473-45fd-bb49-5ff73757727d",
        public string ListId { get; set; }
        //        "ListItemUniqueId": "dc266cf1-ad04-457b-87d4-3011cee8d482",
        public string ListItemUniqueId { get; set; }
        //        "Site": "e368b916-c1f0-459b-a8a9-317240e9e504",
        public string Site { get; set; }
        //        "UserAgent": "Microsoft Office Word 2014",
        public string UserAgent { get; set; }
        //        "WebId": "fd66f1ac-9a7d-4b37-b72c-1e7ebbe2d623",
        public string WebId { get; set; }
        //        "SourceFileExtension": "docx",
        public string SourceFileExtension { get; set; }
        //        "SiteUrl": "https:\\/\\/stlpconsulting.sharepoint.com\\/",
        public string SiteUrl { get; set; }
        //        "SourceFileName": "D2.4 - Information Architecture.docx",
        public string SourceFileName { get; set; }
        //        "SourceRelativeUrl": "TeamSite\\/2 STLP Clients1"
        public string SourceRelativeUrl { get; set; }


        //public int AzureActiveDirectoryEventType { get; set; }
        //public AuditLogEntryExtendedProperties ExtendedProperties { get; set; }
        //public string Client { get; set; }
        //public int LoginStatus { get; set; }
        //public string UserDomain { get; set; }

    }

    public class AuditLogEntryFactorsUpdate
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Factors { get; set; }
    }

    public class AuditLogEntryv1:AuditLogEntry
    {
        public string Id { get; set; }
    }

    public class AuditLogEntry
    {
        public AuditLogEntry() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        //		"CreationTime": "2018-06-03T05:07:28",
        public string CreationTime { get; set; }

        public DateTime CreationDateTime
        {
            get
            {
                DateTime creationDateTime = Utils.AzureTableMinDateTime;
                if (this.CreationTime != null && this.CreationTime != "")
                {
                    // Check if the date will parse
                    try
                    {
                        bool dateValid = DateTime.TryParse(this.CreationTime, out creationDateTime);
                    }
                    catch (Exception exDateConversion)
                    {
                        creationDateTime = Utils.AzureTableMinDateTime;
                    }
                }
                return creationDateTime;
            }
        }

        public string IdEventLog { get; set; }
        //        "Operation": "FileModified",
        public string Operation { get; set; }
        //        "OrganizationId": "e1e984d0-afc0-4573-adda-3d327fd23391",
        public string OrganizationId { get; set; }
        //        "RecordType": 6,
        public int RecordType { get; set; }
        //public string ResultStatus { get; set; }
        //        "UserKey": "i:0h.f|membership|10037ffe8358cd7e@live.com",
        public string UserKey { get; set; }
        //        "UserType": 0,
        public int UserType { get; set; }
        //        "Version": 1,
        public int Version { get; set; }
        //        "Workload": "SharePoint",
        public string Workload { get; set; }
        //        "ClientIP": "124.171.97.46",
        public string ClientIP { get; set; }
        //        "ObjectId": "https:\\/\\/stlpconsulting.sharepoint.com\\/TeamSite\\/2 STLP Clients1\\/D2.4 - Information Architecture.docx",
        public string ObjectId { get; set; }
        //        "UserId": "rachaelg@stlpconsulting.com",
        public string UserId { get; set; }
        //        "CorrelationId": "7d636d9e-d0ec-5000-cd1d-11b899989d9d",
        public string CorrelationId { get; set; }
        //        "EventSource": "SharePoint",
        public string EventSource { get; set; }
        //        "ItemType": "File",
        public string ItemType { get; set; }
        //        "ListId": "d94dd849-f473-45fd-bb49-5ff73757727d",
        public string ListId { get; set; }
        //        "ListItemUniqueId": "dc266cf1-ad04-457b-87d4-3011cee8d482",
        public string ListItemUniqueId { get; set; }
        //        "Site": "e368b916-c1f0-459b-a8a9-317240e9e504",
        public string Site { get; set; }
        //        "UserAgent": "Microsoft Office Word 2014",
        public string UserAgent { get; set; }
        //        "WebId": "fd66f1ac-9a7d-4b37-b72c-1e7ebbe2d623",
        public string WebId { get; set; }
        //        "SourceFileExtension": "docx",
        public string SourceFileExtension { get; set; }
        //        "SiteUrl": "https:\\/\\/stlpconsulting.sharepoint.com\\/",
        public string SiteUrl { get; set; }
        //        "SourceFileName": "D2.4 - Information Architecture.docx",
        public string SourceFileName { get; set; }
        //        "SourceRelativeUrl": "TeamSite\\/2 STLP Clients1"
        public string SourceRelativeUrl { get; set; }

        public string Factors { get; set; }

        //public int AzureActiveDirectoryEventType { get; set; }
        //public AuditLogEntryExtendedProperties ExtendedProperties { get; set; }
        //public string Client { get; set; }
        //public int LoginStatus { get; set; }
        //public string UserDomain { get; set; }

    }

    public class AuditLogEntryFactors
    {
        public string RecordAuthority { get; set; }
        public string RecordKey { get; set; }
        public string SystemKey { get; set; }

        public string Financial { get; set; }
        public string Privacy { get; set; }
        public string Security { get; set; }
    }

    public class AuditLogEntryExtendedProperties
    {
        public string Name { get; set; }
        public string Value { get; set; }

    }

    public class AuditContentEntity
    {
        public AuditContentEntity() { }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string contentUri { get; set; }
        public string contentId { get; set; }
        public string contentType { get; set; }
        public string contentCreated { get; set; }
        public string contentExpiration { get; set; }
        public string ProcessStatus { get; set; }
    }

    public class AuditContentEntityUpdateStatus
    {
        public AuditContentEntityUpdateStatus() { }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ProcessStatus { get; set; }

    }

}

