using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public static class TableNames
    {
        public static class Azure
        {
            public static class Account
            {
                public const string ManagedAccount = "stlpmanagedaccount";
            }

            public static class FileHashTableNames
            {
                public const string FileHash = "stlpfilehash";
            }

            public static class FileAlertTableNames
            {
                public const string FileAlert = "stlpfilealert";
            }
            public static class System
            {
                public const string Sys = "stlpsystems";
                public const string BatchConfig = "stlpsystembatchconfig";
            }

            public static class ManagedService
            {
                public const string Service = "stlpmanagedservice";
            }

            public static class RecordAssociation
            {
                public const string RecordAssociationMatchStatus = "stlprecordmatchstatus";
            }

            public static class Record
            {
                public const string RecordToMetadata = "stlprecordmetadata";
            }

            public static class NTFS
            {
                public const string NTFSFolders = "stlpntfsfolders";
                public const string NTFSFiles = "stlpntfsfiles";
                public const string NTFSFilesBatchStatus = "stlpntfsfilesbatchstatus";
            }

            public static class Sakai
            {
                public const string SakaiFolders = "sltpsakaifolders";
                public const string SakaiSites = "stlpsakaisites";
                public const string SakaiFiles = "stlpsakaifiles";
                public const string SakaiFilesBatchStatus = "stlpsakaifilesbatchstatus";
            }

            public static class SharePointOnline
            {
                public const string SPFile = "stlpo365spfiles";
                public const string SPFileBatchStatus = "stlpo365spfilesbatchstatus";
                public const string SPOTracking = "stlpspotracking";
                public const string SPFolder = "stlpo365spfolders";
                public const string SPList = "stlpo365splists";
            }

            public static class SharePoint
            {
                public const string SPFile = "spfiles";
                public const string SPFileBatchStatus = "spfilesbatchstatus";
                public const string SPOTracking = "spotracking";
                public const string SPFolder = "spfolders";
                public const string SPList = "splists";
            }

        }

        public static class Mongo
        {
            public static class Account
            {
                public const string ManagedAccount = "managedaccount";
            }

            public static class FileHashTableNames
            {
                public const string FileHash = "filehash";
            }

            public static class FileAlertTableNames
            {
                public const string FileAlert = "filealert";
            }

            public static class System
            {
                public const string BatchConfig = "systembatchconfig";
                public const string Sys = "stlpsystems";
            }

            public static class ManagedService
            {
                public const string Service = "managedservice";
            }


            public static class Record
            {
                public const string RecordToMetadata = "recordmetadata";
            }

            public static class RecordAssociation
            {
                public const string RecordAssociationMatchStatus = "recordmatchstatus";
            }
            public static class NTFS
            {
                public const string NTFSFolders = "ntfsfolders";
                public const string NTFSFiles = "ntfsfiles";
                public const string NTFSFilesBatchStatus = "ntfsfilesbatchstatus";
            }
            public static class Sakai
            {
                public const string SakaiFolders = "sakaifolders";
                public const string SakaiSites = "sakaisites";
                public const string SakaiFiles = "sakaifiles";
                public const string SakaiFilesBatchStatus = "sakaifilesbatchstatus";
            }
            public static class SharePointOnline
            {
                public const string SPFile = "o365spfiles";
                public const string SPFileBatchStatus = "o365spfilesbatchstatus";
                public const string SPOTracking = "spotracking";
                public const string SPFolder = "o365spfolders";
                public const string SPList = "o365splists";
            }
            public static class SharePoint
            {
                public const string SPFile = "spfiles";
                public const string SPFileBatchStatus = "spfilesbatchstatus";
                public const string SPOTracking = "sptracking";
                public const string SPFolder = "spfolders";
                public const string SPList = "splists";
            }
        }
    }
}
