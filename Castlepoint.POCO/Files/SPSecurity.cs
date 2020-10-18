using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Castlepoint.POCO.Files
{
    internal static class SPSecurity
    {
        // EXAMPLE JSON FROM SPO ONLINE
        //string json = "{\"ListItemAllFields\":" +
        //        "{\"RoleAssignments\":" +
        //            "[{\"Member\":" +
        //                "{\"Id\":1,\"IsHiddenInUI\":false,\"LoginName\":\"i:0#.f|membership|gavinm@stlpconsulting.com\",\"Title\":\"Gavin McKay\",\"PrincipalType\":1,\"Email\":\"gavinm@stlpconsulting.com\",\"IsEmailAuthenticationGuestUser\":false,\"IsShareByEmailGuestUser\":false,\"IsSiteAdmin\":true," +
        //                "\"UserId\":" +
        //                "   {\"NameId\":\"10033fff8358ff89\",\"NameIdIssuer\":\"urn:federation:microsoftonline\"}}," +
        //              "\"RoleDefinitionBindings\":" +
        //                "   [{\"BasePermissions\":{\"High\":\"48\",\"Low\":\"134287360\"}," +
        //                "   \"Description\":\"Can view specific lists, document libraries, list items, folders, or documents when given permissions.\"" +
        //                "   ,\"Hidden\":true,\"Id\":1073741825,\"Name\":\"Limited Access\",\"Order\":160,\"RoleTypeKind\":1}]" +
        //                ",\"PrincipalId\":1}," +
        //           "{\"Member\":{\"Id\":3,\"IsHiddenInUI\":false,\"LoginName\":\"c:0(.s|true\",\"Title\":\"Everyone\",\"PrincipalType\":4,\"Email\":\"\",\"IsEmailAuthenticationGuestUser\":false,\"IsShareByEmailGuestUser\":false,\"IsSiteAdmin\":false,\"UserId\":null},\"RoleDefinitionBindings\":[{\"BasePermissions\":{\"High\":\"48\",\"Low\":\"134287360\"},\"Description\":\"Can view specific lists, document libraries, list items, folders, or documents when given permissions.\",\"Hidden\":true,\"Id\":1073741825,\"Name\":\"Limited Access\",\"Order\":160,\"RoleTypeKind\":1}],\"PrincipalId\":3},{\"Member\":{\"Id\":4,\"IsHiddenInUI\":false,\"LoginName\":\"c:0-.f|rolemanager|spo-grid-all-users/c6a4bfd8-d354-47c2-a666-86080f66aee3\",\"Title\":\"Everyone except external users\",\"PrincipalType\":4,\"Email\":\"\",\"IsEmailAuthenticationGuestUser\":false,\"IsShareByEmailGuestUser\":false,\"IsSiteAdmin\":false,\"UserId\":null},\"RoleDefinitionBindings\":[{\"BasePermissions\":{\"High\":\"48\",\"Low\":\"134287360\"},\"Description\":\"Can view specific lists, document libraries, list items, folders, or documents when given permissions.\",\"Hidden\":true,\"Id\":1073741825,\"Name\":\"Limited Access\",\"Order\":160,\"RoleTypeKind\":1}],\"PrincipalId\":4},{\"Member\":{\"Id\":11,\"IsHiddenInUI\":false,\"LoginName\":\"Limited Access System Group\",\"Title\":\"Limited Access System Group\",\"PrincipalType\":8,\"AllowMembersEditMembership\":false,\"AllowRequestToJoinLeave\":false,\"AutoAcceptRequestToJoinLeave\":false,\"Description\":\"Limited Access System Group\",\"OnlyAllowMembersViewMembership\":true,\"OwnerTitle\":\"System Account\",\"RequestToJoinLeaveEmailSetting\":null},\"RoleDefinitionBindings\":[{\"BasePermissions\":{\"High\":\"48\",\"Low\":\"134287360\"},\"Description\":\"Can view specific lists, document libraries, list items, folders, or documents when given permissions.\",\"Hidden\":true,\"Id\":1073741825,\"Name\":\"Limited Access\",\"Order\":160,\"RoleTypeKind\":1}],\"PrincipalId\":11},{\"Member\":{\"Id\":13,\"IsHiddenInUI\":false,\"LoginName\":\"i:0#.f|membership|rachaelg@stlpconsulting.com\",\"Title\":\"Rachael Greaves\",\"PrincipalType\":1,\"Email\":\"rachaelg@stlpconsulting.com\",\"IsEmailAuthenticationGuestUser\":false,\"IsShareByEmailGuestUser\":false,\"IsSiteAdmin\":false,\"UserId\":{\"NameId\":\"10037ffe8358cd7e\",\"NameIdIssuer\":\"urn:federation:microsoftonline\"}},\"RoleDefinitionBindings\":[{\"BasePermissions\":{\"High\":\"48\",\"Low\":\"134287360\"},\"Description\":\"Can view specific lists, document libraries, list items, folders, or documents when given permissions.\",\"Hidden\":true,\"Id\":1073741825,\"Name\":\"Limited Access\",\"Order\":160,\"RoleTypeKind\":1}],\"PrincipalId\":13}],\"FileSystemObjectType\":0,\"Id\":1428,\"ServerRedirectedEmbedUri\":null,\"ServerRedirectedEmbedUrl\":\"\",\"ContentTypeId\":\"0x010100106C32C4E8213D4D835C3041C0E99CC5\",\"Title\":null,\"IsMyDocuments\":null,\"SharedWithInternalId\":null,\"SharedWithInternalStringId\":null,\"SharedWithUsersId\":null,\"SharedWithDetails\":null,\"ComplianceAssetId\":null,\"MediaServiceAutoTags\":null,\"ID\":1428,\"Created\":\"2016-08-07T23:42:06\",\"AuthorId\":1,\"Modified\":\"2016-08-07T23:42:08\",\"EditorId\":1,\"OData__CopySource\":null,\"CheckoutUserId\":null,\"OData__UIVersionString\":\"1.0\",\"GUID\":\"6b42047e-01ee-4f59-b393-1d73324ffd96\"},\"CheckInComment\":\"\",\"CheckOutType\":2,\"ContentTag\":\"{B0E18341-7E54-4298-B039-04A52430BAA9},1,1\",\"CustomizedPageStatus\":0,\"ETag\":\"\\\"{B0E18341-7E54-4298-B039-04A52430BAA9},1\\\"\",\"Exists\":true,\"IrmEnabled\":false,\"Length\":\"3112598\",\"Level\":1,\"LinkingUri\":null,\"LinkingUrl\":\"\",\"MajorVersion\":1,\"MinorVersion\":0,\"Name\":\"ICAuditTool-20160807.zip\",\"ServerRelativeUrl\":\"/personal/gavinm_stlpconsulting_com/Documents/Castlepointv2/ICAuditTool-20160807.zip\",\"TimeCreated\":\"2016-08-07T13:42:06Z\",\"TimeLastModified\":\"2016-08-07T13:42:08Z\",\"Title\":null,\"UIVersion\":512,\"UIVersionLabel\":\"1.0\",\"UniqueId\":\"b0e18341-7e54-4298-b039-04a52430baa9\"}";

        internal static SPORoleMembership GetSPORoleMembership(string jsonSPORoleMembership)
        {
            SPORoleMembership rolemem = JsonConvert.DeserializeObject<SPORoleMembership>(jsonSPORoleMembership);
            return rolemem;
        }
        internal class SPORoleMembership
        {
            public ListItemAllFields ListItemAllFields { get; set; }
        }
        internal class ListItemAllFields
        {
            public List<RoleAssignments> RoleAssignments { get; set; }

        }
        internal class RoleAssignments
        {
            public Member Member { get; set; }
            public List<RoleDefinitionBindings> RoleDefinitionBindings { get; set; }
        }

        internal class Member
        {
            public int Id { get; set; }
            public bool IsHiddenInUI { get; set; }
            public string LoginName { get; set; }
            public string Title { get; set; }
            public bool IsSiteAdmin { get; set; }

            public UserId UserId { get; set; }
        }

        internal class UserId
        {
            public string NameId { get; set; }
            public string NameIdIssuer { get; set; }
        }

        internal class RoleDefinitionBindings
        {
            //            "   [{\"BasePermissions\":{\"High\":\"48\",\"Low\":\"134287360\"}," +
            //"   \"Description\":\"Can view specific lists, document libraries, list items, folders, or documents when given permissions.\"" +
            //"   ,\"Hidden\":true,\"Id\":1073741825,\"Name\":\"Limited Access\",\"Order\":160,\"RoleTypeKind\":1}]

            public BasePermissions BasePermissions { get; set; }
            public string Description { get; set; }
            public bool Hidden { get; set; }
            public int Id { get; set; }
            public string Name { get; set; }
            public int Order { get; set; }
            public int RoleTypeKind { get; set; }
        }

        internal class BasePermissions
        {
            public string High { get; set; }
            public string Low { get; set; }
        }
    }
}
