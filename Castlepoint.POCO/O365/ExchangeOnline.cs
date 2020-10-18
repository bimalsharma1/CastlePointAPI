using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Castlepoint.POCO.O365
{
    public class EOMailboxFolders
    {
        [JsonProperty("@odata.nextLink")]
        public string odatanextlink { get; set; }
        public List<EOMailboxFolder> value { get; set; }

    }
    public class EOMailboxFolder
    {
        public int childFolderCount { get; set; }
        public string displayName { get; set; }
        public string id { get; set; }
        public string parentFolderId { get; set; }
        public int totalItemCount { get; set; }
        public int unreadItemCount { get; set; }
        public List<EOMailboxFolder> childFolders { get; set; }
        // "@odata.type": "microsoft.graph.mailFolder" } ],
        //"messageRules": [ { "@odata.type": "microsoft.graph.messageRule" } ],
        //"messages": [ { "@odata.type": "microsoft.graph.message" } ],
        //"multiValueExtendedProperties": [ { "@odata.type": "microsoft.graph.multiValueLegacyExtendedProperty" }],
        //"singleValueExtendedProperties": [ { "@odata.type": "microsoft.graph.singleValueLegacyExtendedProperty" }]
    }

    public class EOMessages
    {
        [JsonProperty("@odata.nextLink")]
        public string odatanextlink { get; set; }
        public List<EOMessage> value { get; set; }
    }

    public class EOMessage
    {
        [JsonProperty("@odata.etag")]
        public string odataetag { get; set; }
        public string id { get; set; }
        public List<EORecipient> bccRecipients { get; set; }
        public EOMessageBody body { get; set; }
        public string bodyPreview { get; set; }
        public List<string> categories { get; set; }
        public List<EORecipient> ccRecipients { get; set; }
        public string changeKey { get; set; }
        public string conversationId { get; set; }
        public string conversationIndex { get; set; }
        public string createdDateTime { get; set; }
        //"flag": {"@odata.type": "microsoft.graph.followupFlag"},
        public EOSender from { get; set; }
        public string hasAttachments { get; set; }
        public string importance { get; set; }
        public string inferenceClassification { get; set; }
        //"internetMessageHeaders": [{"@odata.type": "microsoft.graph.internetMessageHeader"}],
        public string internetMessageId { get; set; }
        public string isDeliveryReceiptRequested { get; set; }
        public string isDraft { get; set; }
        public string isRead { get; set; }
        public string isReadReceiptRequested { get; set; }
        public string lastModifiedDateTime { get; set; }
        public string parentFolderId { get; set; }
        public string receivedDateTime { get; set; }
        public List<EORecipient> replyTo { get; set; }
        public EOSender sender { get; set; }
        public string sentDateTime { get; set; }
        public string subject { get; set; }
        public List<EORecipient> toRecipients { get; set; }
        public string uniqueBody { get; set; }
        public string webLink { get; set; }

        //"attachments": [{"@odata.type": "microsoft.graph.attachment"}],
        //"extensions": [{"@odata.type": "microsoft.graph.extension"}],
        //"multiValueExtendedProperties": [{"@odata.type": "microsoft.graph.multiValueLegacyExtendedProperty"}],
        //"singleValueExtendedProperties": [{"@odata.type": "microsoft.graph.singleValueLegacyExtendedProperty"}]

    }

    public class EOMessageBody
    {
        public string contentType { get; set; }
        public string content { get; set; }
    }
    public class EOSender
    {
        public EOEmailAddress emailAddress { get; set; }
    }

public class EORecipient
{
    public EOEmailAddress emailAddress { get; set; }
}

public class EOEmailAddress
    {
        public string address { get; set; }
        public string name { get; set; }
    }

    public class EOMessageEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime CreationTime { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public string SourceFileName { get; set; }
        public int SizeInBytes { get; set; }
        public string BatchStatus { get; set; }
        public Guid BatchGuid { get; set; }
        public Guid OrganisationId { get; set; }
        public string SourceId { get; set; }
    }

    public class EOFolderEntity
    {
        public EOFolderEntity() { }
        public EOFolderEntity(string pKey, string rKey)
        {
            this.PartitionKey = pKey;
            this.RowKey = rKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public string UniqueId { get; set; }
        public string CPFolderStatus { get; set; }
    }

    public class EOFolderUpdate
    {
        public EOFolderUpdate() { }
        public EOFolderUpdate(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.TimeCreated = Utils.AzureTableMinDateTime;
            this.TimeLastModified = Utils.AzureTableMinDateTime;
            this.ItemCount = 0;
            this.Name = "";
            this.CPFolderStatus = "";
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public string CPFolderStatus { get; set; }
    }
}
