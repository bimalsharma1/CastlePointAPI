using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.SharePoint
{
    public class SPList
    {

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public bool AllowContentTypesB { get; set; }
        public int BaseTemplate { get; set; }
        public int BaseType { get; set; }
        public bool ContentTypesEnabled { get; set; }
        public string Created { get; set; }
        public string Description { get; set; }
        public int DraftVersionVisibility { get; set; }
        public bool EnableAttachments { get; set; }
        public bool EnableFolderCreation { get; set; }
        public bool EnableMinorVersions { get; set; }
        public bool EnableModeration { get; set; }
        public bool EnableVersioning { get; set; }
        public string EntityTypeName { get; set; }
        public bool ForceCheckout { get; set; }
        public bool HasExternalDataSource { get; set; }
        public bool Hidden { get; set; }
        public string IdList { get; set; }
        public string ImageUrl { get; set; }
        public bool IrmEnabled { get; set; }
        public bool IrmExpire { get; set; }
        public bool IrmReject { get; set; }
        public bool IsApplicationList { get; set; }
        public bool IsCatalog { get; set; }
        public bool IsPrivate { get; set; }
        public int ItemCount { get; set; }
        public string LastItemDeletedDate { get; set; }
        public string LastItemModifiedDate { get; set; }
        public string ListItemEntityTypeFullName { get; set; }
        public int MajorVersionLimit { get; set; }
        public int MajorWithMinorVersionsLimit { get; set; }
        public bool MultipleDataList { get; set; }
        public bool NoCrawl { get; set; }
        public string ParentWebUrl { get; set; }
        public bool ServerTemplateCanCreateFolders { get; set; }
        public string TemplateFeatureId { get; set; }
        public string Title { get; set; }
    }

    public class ListResultJson
    {
        public ListNamespace d { get; set; }
    }

    public class ListNamespace
    {
        public List<ListResult> results { get; set; }
    }

    public class ListResult
    {
        public bool AllowContentTypesB { get; set; }
        public int BaseTemplate { get; set; }
        public int BaseType { get; set; }
        public bool ContentTypesEnabled { get; set; }
        public string Created { get; set; }
        public string Description { get; set; }
        public int DraftVersionVisibility { get; set; }
        public bool EnableAttachments { get; set; }
        public bool EnableFolderCreation { get; set; }
        public bool EnableMinorVersions { get; set; }
        public bool EnableModeration { get; set; }
        public bool EnableVersioning { get; set; }
        public string EntityTypeName { get; set; }
        public bool ForceCheckout { get; set; }
        public bool HasExternalDataSource { get; set; }
        public bool Hidden { get; set; }
        public string Id { get; set; }
        public string ImageUrl { get; set; }
        public bool IrmEnabled { get; set; }
        public bool IrmExpire { get; set; }
        public bool IrmReject { get; set; }
        public bool IsApplicationList { get; set; }
        public bool IsCatalog { get; set; }
        public bool IsPrivate { get; set; }
        public int ItemCount { get; set; }
        public string LastItemDeletedDate { get; set; }
        public string LastItemModifiedDate { get; set; }
        public string ListItemEntityTypeFullName { get; set; }
        public int MajorVersionLimit { get; set; }
        public int MajorWithMinorVersionsLimit { get; set; }
        public bool MultipleDataList { get; set; }
        public bool NoCrawl { get; set; }
        public string ParentWebUrl { get; set; }
        public bool ServerTemplateCanCreateFolders { get; set; }
        public string TemplateFeatureId { get; set; }
        public string Title { get; set; }
    }
}
