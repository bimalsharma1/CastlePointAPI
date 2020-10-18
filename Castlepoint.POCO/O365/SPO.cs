using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Castlepoint.POCO.O365
{

    public class SPOGraphByUser
    {
        public SPOGraphByUserInfo user { get; set; }
    }
    public class SPOGraphByUserInfo
    {
        public string email { get; set; }
        public string id { get; set; }
        public string displayName { get; set; }
    }

    public class SPODriveItemResults
    {
        [JsonProperty("@odata.nextLink")]
        public string odatanextlink { get; set; }
        public List<SPODriveItemResult> value { get; set; }
    }

    public class SPOGraphItemResults
    {
        [JsonProperty("@odata.context")]
        public string odatacontext { get; set; }
        [JsonProperty("@odata.nextLink")]
        public string odatanextlink { get; set; }
        public List<SPOGraphItemResult> value { get; set; }
    }

    public class SPOGraphItemResult
    {
        [JsonProperty("@odata.etag")]
        public string odataetag { get; set; }
        public string createdDateTime { get; set; }
        public string eTag { get; set; }
        public string id { get; set; }
        public string lastModifiedDateTime { get; set; }
        public long size { get; set; }
        public string webUrl { get; set; }
        public SPOGraphByUser createdBy { get; set; }
        public SPOGraphByUser lastModifiedBy { get; set; }
        public SPOGraphItemParentReference parentReference { get; set; }
        public SPOGraphItemContentType contentType { get; set; }
    }

    public class SPOGraphItemParentReference
    {
        public string id { get; set; }
        public string siteId { get; set; }
    }

    public class SPOGraphItemContentType
    {
        public string id { get; set; }
    }

    public class SPOFoldersInfo
    {
        public SPOFoldersInfo() { }
        public List<SPOFolderInfo> value { get; set; }
    }

    public class SPOFolderInfo
    {
        public SPOFolderInfo() { }
        public Boolean Exists { get; set; }
        public Boolean IsWOPIEnabled { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string ProgID { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public Guid UniqueId { get; set; }
        public string WelcomePage { get; set; }

    }

    public class SPOFilesInfos
    {
        public List<SPOFileInfo> value { get; set; }
    }
    public class SPOFileInfo
    {

        public string CheckInComment { get; set; }
        public int CheckOutType { get; set; }
        public string ContentTag { get; set; }
        public int CustomizedPageStatus { get; set; }
        //public string ETag { get; set; }
        public bool Exists { get; set; }
        public bool IrmEnabled { get; set; }
        public Int64 Length { get; set; }
        public int Level { get; set; }
        public string LinkingUri { get; set; }
        public string LinkingUrl { get; set; }
        public int MajorVersion { get; set; }
        public int MinorVersion { get; set; }
        public string Name { get; set; }
        public string ServerRelativeUrl { get; set; }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public string Title { get; set; }
        public int UIVersion { get; set; }
        public string UIVersionLabel { get; set; }
        public Guid UniqueId { get; set; }

    }

    public class SPOFileInfoEntity
    {
        public SPOFileInfoEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;

            this.BatchGuid = Guid.Empty;
            this.BatchStatus = string.Empty;

            // Set date-times to minimum date supported
            this.CreationTime = Utils.AzureTableMinDateTime;
            this.LastModifiedTime = Utils.AzureTableMinDateTime;

            this.OrganisationId = Guid.Empty;
        }

        public SPOFileInfoEntity() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public Guid BatchGuid { get; set; }
        public string BatchStatus { get; set; }
        public DateTime CreationTime { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public Guid OrganisationId { get; set; }
        public string SiteUrl { get; set; }
        public string SourceFileName { get; set; }
        public string SourceRelativeUrl { get; set; }

        public string JsonFileProcessResult { get; set; }
        public int ItemCount { get; set; }
        public string Name { get; set; }
        public string ItemUri { get; set; }
        public string ServerRelativeUrl { get; set; }
        public string UniqueId { get; set; }
        public int Version { get; set; }
        public string CPFolderStatus { get; set; }
        public int SizeInBytes { get; set; }
        public string MIMEType { get; set; }
    }

    public class SPOWebsResult
    {
        public List<SPOWebInfoResult> value { get; set; }
    }

    public class SPOWebInfoResult
    {
        public DateTime Created { get; set; }
        public string Id { get; set; }
        public string Description { get; set; }
        public DateTime LastItemModifiedDate { get; set; }
        public DateTime LastItemUserModifiedDate { get; set; }
        public string ServerRelativeUrl { get; set; }
        public string Title { get; set; }
        public string WebTemplate { get; set; }
        public string WebTemplateId { get; set; }
    }

    public class SPOSitesGraphResult
    {
        public List<SPOSiteGraphInfoResult> value { get; set; }
    }

    public class SPOSiteGraphInfoResult
    {
        [JsonProperty("@odata.context")]
        public string odatacontext { get; set; }
        public string createdDateTime { get; set; }
        public string description { get; set; }
        public string id { get; set; }
        public string lastModifiedDateTime { get; set; }
        public string name { get; set; }
        public string webUrl { get; set; }
        public string displayName { get; set; }
    }

    public class SPODriveGraphInfoResult
    {

    }

    public class SPOWebInfoEntity
    {
        public SPOWebInfoEntity() { }
        public SPOWebInfoEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime LastItemModifiedDate { get; set; }
        public DateTime LastItemUserModifiedDate { get; set; }


    }

    public class SPOGraphListsResult
    {
        public List<SPOGraphListResult> value { get; set; }
    }

    public class SPOGraphListResult
    {
        public string createdDateTime { get; set; }
        public string description { get; set; }
        public string eTag { get; set; }
        public string id { get; set; }
        public string lastModifiedDateTime { get; set; }
        public string name { get; set; }
        public string webUrl { get; set; }
        public string displayName { get; set; }
        public SPOGraphByUser createdBy { get; set; }
        public SPOGraphByUser lastModifiedBy { get; set; }
        public ParentReference parentReference { get; set; }
        public WebList list { get; set; }

        public class ParentReference
        {
            public string siteId { get; set; }
        }

        public class WebList
        {
            public bool contentTypesEnabled { get; set; }
            public bool hidden { get; set; }
            public string template { get; set; }
        }
    }

    public class SPOListsResult
    {
        public List<SPOListResult> value { get; set; }
    }
    public class SPOListResult
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string BaseTemplate { get; set; }
        public bool IsCatalog { get; set; }
        public bool IsApplicationList { get; set; }
    }

    public class SPODriveResult
    {
        [JsonProperty("@odata.context")]
        public string DataContext { get; set; }
        public string createdDateTime { get; set; }
        public string description { get; set; }
        public string id { get; set; }
        public string lastModifiedDateTime { get; set; }
        public string name { get; set; }
        public string webUrl { get; set; }
        public string driveType { get; set; }
        public SPOGraphByUser createdBy { get; set; }
        public SPOGraphByUser owner { get; set; }
        public SPODriveQuota quota { get; set; }
    }

    public class DriveItemNew
    {
        [JsonProperty("@microsoft.graph.conflictBehavior")]
        public string MicrosoftGraphConflictBehavior { get; set; }
        public string name { get; set; }
        public DriveItemNewFolder folder { get; set; }
    }

    public class DriveItemNewFolder
    {

    }

    public class DriveItemUploadRequest
    {
        [JsonProperty("@microsoft.graph.conflictBehavior")]
        public string MicrosoftGraphConflictBehavior { get; set; }
        public string description { get; set; }
        public DriveItemFileSystemInfo fileSystemInfo { get; set; }
        public string name { get; set; }
    }

    public class DriveItemFileSystemInfo
    {
        [JsonProperty("@odata.type")]
        public string ODataType { get { return "microsoft.graph.fileSystemInfo"; } }
        public DateTimeOffset createdDateTime { get; set; }
        public DateTimeOffset lastAccessedDateTime { get; set; }
        public DateTimeOffset lastModifiedDateTime { get; set; }

    }

    public class DriveItemUploadResponse
    {
        public string uploadUrl { get; set; }
        public string expirationDateTime { get; set; }
    }

    public class ListItemPatch
    {
        public ListItemPatch()
        {
            this.formValues = new List<FormValue>();
        }
        public List<FormValue> formValues { get; set; }
        public bool bNewDocumentUpdate { get; set; }
    }

    public class FormValue
    {
        public FormValue()
        {
            //metadata = new ListItemMetadata();
        }
        //[JsonProperty("__metadata")]
        //public ListItemMetadata metadata { get; set; }
        public string FieldName { get; set; }
        public string FieldValue { get; set; }
    }

    public class ListItemMetadata
    {
        public string type { get; set; }
    }

    public class ListItemUpdate
    {
        //[JsonProperty("__metadata")]
        //public ListItemMetadata metadata { get; set; }
        public string Title { get; set; }
    }

    public class DriveItemPatch
    {
        public DriveItemPatch()
        {
            fileSystemInfo = new DriveItemFileSystemInfo();
        }
        //public string version { get; set; }
        //public string Title { get; set; }
        //public DateTime Created { get; set; }
        //public DateTime Modified { get; set; }
        //public string Author { get; set; }
        //public string Editor { get; set; }
        public DriveItemFileSystemInfo fileSystemInfo { get; set; }
    }

    public class SPODriveItemResult
    {
        [JsonProperty("@odata.context")]
        public string DataContext { get; set; }
        [JsonProperty("@microsoft.graph.downloadUrl")]
        public string MicrosoftGraphDownloadUrl { get; set; }
        public string createdDateTime { get; set; }
        public string eTag { get; set; }
        public string id { get; set; }
        public string lastModifiedDateTime { get; set; }
        public string name { get; set; }
        public string webUrl { get; set; }
        public string cTag { get; set; }
        public long size { get; set; }

        // TODO
        //"createdBy": {
        //    "application": {
        //        "id": "cc15fd57-2c6c-4117-a88c-83b1d56b4bbe",
        //        "displayName": "Microsoft Teams Services"
        //    },
        //    "user": {
        //        "email": "gavinm@stlpconsulting.com",
        //        "id": "5fc1a6fc-2742-4fb3-a853-d1b66eee150b",
        //        "displayName": "Gavin McKay"
        //    }
        //},
        //"lastModifiedBy": {
        //    "application": {
        //        "id": "1fec8e78-bce4-4aaf-ab1b-5451cc387264",
        //        "displayName": "Microsoft Teams"
        //    },
        //    "user": {
        //        "email": "gavinm@stlpconsulting.com",
        //        "id": "5fc1a6fc-2742-4fb3-a853-d1b66eee150b",
        //        "displayName": "Gavin McKay"
        //    }
        //},
        //"parentReference": {
        //    "driveId": "b!W5O8mg-iH0qOfVfgAlUxiRisSVpsSi9BtEIraBOqngDS8Bcy9Xq6QryCc93O_lYg",
        //    "driveType": "documentLibrary",
        //    "id": "01TQC7SUGSCTZWSOE3TJGL2IPNHMIK5GFJ",
        //    "path": "/drives/b!W5O8mg-iH0qOfVfgAlUxiRisSVpsSi9BtEIraBOqngDS8Bcy9Xq6QryCc93O_lYg/root:/General/Committees"
        //},
        public SPODriveItemFileInfo file { get; set; }
        public SPODriveItemFolderInfo folder { get; set; }
        public SPODriveItemFileSystemInfo fileSystemInfo { get; set; }
        public SPOListItemResult listItem { get; set; }
        public SPOSharePointIds sharepointIds { get; set; }
    }

    public class SPOListItemResult
    {
        public string id { get; set; }
        public string name { get; set; }
        SPOIdentitySet createdBy { get; set; }
        public DateTimeOffset createdDateTime { get; set; }
        public string description { get; set; }
        public string eTag { get; set; }
        public SPOIdentitySet lastModifiedBy { get; set; }
        public DateTimeOffset dateTimeOffset { get; set; }
        //parentReference
        public SPOSharePointIds sharepointIds { get; set; }
        public string webUrl { get; set; }
    }

    public class SPOIdentitySet
    {
        public SPOIdentity application { get; set; }
        public SPOIdentity device { get; set; }
        public SPOIdentity user { get; set; }
    }

    public class SPOIdentity
    {
        public string displayName { get; set; }
        public string id { get; set; }
    }

    public class SPOSharePointIds
    {
        public string listId { get; set; }
        public string listItemId { get; set; }
        public string listItemUniqueId { get; set; }
        public string siteId { get; set; }
        public Uri siteUrl { get; set; }
        public string webId { get; set; }
    }

    public class SPODriveItemFileInfo
    {
        public string mimeType { get; set; }
        public SPODriveItemFileHashInfo hashes { get; set; }
    }
    public class SPODriveItemFileHashInfo
    {
        public string quickXorHash { get; set; }
    }
    public class SPODriveItemFolderInfo
    {
        public int childCount { get; set; }
        public SPODriveItemFolderView view { get; set; }

    }
    public class SPODriveItemFolderView
    {
        public string sortyBy { get; set; }
        public string sortOrder { get; set; }
        public string viewType { get; set; }
    }

    public class SPODriveItemFileSystemInfo
    {
        public string createdDateTime { get; set; }
        public string lastModifiedDateTime { get; set; }
    }

    public class SPODriveQuota
    {
        public long deleted { get; set; }
        public long remaining { get; set; }
        public string state { get; set; }
        public long total { get; set; }
        public long used { get; set; }
    }

    public class SPFolderUpdate
    {
        public SPFolderUpdate() { }
        public SPFolderUpdate(string partitionKey, string rowKey)
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

    public class SPFolder
    {
        public SPFolder() { }
        public SPFolder(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.UniqueId = Guid.Empty;
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
        public string ServerRelativeUrl { get { return this.RowKey; } }
        public DateTime TimeCreated { get; set; }
        public DateTime TimeLastModified { get; set; }
        public Guid UniqueId { get; set; }
        public string CPFolderStatus { get; set; }
    }

    public class SPList
    {
        public SPList() { }

        public SPList(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
    }
}
