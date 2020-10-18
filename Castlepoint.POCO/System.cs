using System;

namespace Castlepoint.POCO
{
    public class System
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public Guid SystemId { get; set; }
        public string SystemUri { get; set; }
        public string Label { get; set; }
        public bool ProcessRecords { get; set; }
        public bool FindRecords { get; set; }
        public string Type { get; set; }
        public string JsonConnectionConfig { get; set; }
        public string JsonRecordIdentificationConfig { get; set; }
        public string JsonSentenceStats { get; set; }
        public string JsonSystemStats { get; set; }
        /// <summary>
        /// JSON string storage for the POCO.System.SystemConfig class
        /// </summary>
        public string JsonSystemConfig { get; set; }
        public string JsonSecurityClassConfig { get; set; }
        public bool Enabled { get; set; }
        public string JsonAssignedOntology { get; set; }
        public string JsonAssignedRecordsAuthority { get; set; }
    }

    public class SystemConfig
    {
        public SystemConfig()
        {
            this.FindRecords = true;
            this.FindRecordAssociations = true;
            this.GetFileACL = false;
            this.GetFileSecurityClassification = false;
            this.ProcessFileMetadata = true;
            this.ProcessImageOCR = false;
            this.ProcessImageMetadata = false;
            this.ProcessCompressedFileContent = false;
            this.ProcessCompressedFileMetadata = false;
            this.ProcessFilesWithNoAssociation = false;
            this.ProcessRecords = true;
            this.ClassifyRecords = true;
            this.UpdateSystemStats = true;
            this.GetAuditData = false;
            this.MatchOntology = true;

            // Semaphore performance configuration
            this.semaphore_addfiles_initial_count = 1;
            this.semaphore_addfiles_max_count = 1;
            this.semaphore_folders_initial_count = 1;
            this.semaphore_folders_max_count = 1;
            this.semaphore_initial_count = 1;
            this.semaphore_max_count = 1;
            this.semaphore_processfiles_initial_count = 1;
            this.semaphore_processfiles_max_count = 1;
            this.semaphore_records_initial_count = 1;
            this.semaphore_records_max_count = 1;

        }

        public bool FindRecords { get; set; }
        public bool FindRecordAssociations { get; set; }
        public bool GetFileACL { get; set; }
        public bool GetFileSecurityClassification { get; set; }
        public bool ProcessFile { get; set; }
        public bool ProcessFileMetadata { get; set; }
        public bool ProcessImageOCR { get; set; }
        public bool ProcessImageMetadata { get; set; }
        public bool ProcessCompressedFileMetadata { get; set; }
        public bool ProcessCompressedFileContent { get; set; }
        public bool ProcessFilesWithNoAssociation { get; set; }
        public bool ProcessRecords { get; set; }
        public bool ClassifyRecords { get; set; }
        public bool UpdateSystemStats { get; set; }
        public bool GetAuditData { get; set; }
        public bool MatchOntology { get; set; }

        public bool ReportsExport { get; set; }


        // Semaphore configuration for multi-threading
        public int semaphore_initial_count { get; set; }
        public int semaphore_max_count { get; set; }

        public int semaphore_addfiles_initial_count { get; set; }
        public int semaphore_addfiles_max_count { get; set; }

        public int semaphore_folders_initial_count { get; set; }
        public int semaphore_folders_max_count { get; set; }

        public int semaphore_records_initial_count { get; set; }
        public int semaphore_records_max_count { get; set; }

        public int semaphore_processfiles_initial_count { get; set; }
        public int semaphore_processfiles_max_count { get; set; }
    }

    public class SystemEnabledUpdate
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public bool Enabled { get; set; }
    }
    public class SystemConfigUpdate
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string JsonSystemConfig { get; set; }
    }
    public class ConnectionConfigUpdate
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string JsonConnectionConfig { get; set; }
    }

    public class RecordConfigUpdate
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string JsonRecordIdentificationConfig { get; set; }
    }

    public class SystemNodeConfig
    {
        public SystemNodeConfig()
        {
            this.PartitionKey = string.Empty;
            this.RowKey = string.Empty;
            this.BatchNodes = "[]";
        }

        /// <summary>
        /// System Id from Systems table
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Guid-format id
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// Json-format list of batch service nodes [a,b,c...] that are allowed to process this system
        /// An empty node list means any batch service can process this system
        /// </summary>
        public string BatchNodes { get; set; }
    }

    public class SystemStat
    {
        public SystemStat()
        {
            this.NumFileKeyPhrases = 0;
            this.NumFileSubjectObjects = 0;
            this.NumFileNamedEntities = 0;
            this.NumRecordAuthorityKeyPhrases = 0;
            this.NumItems = 0;
            this.NumRecordItems = 0;
            this.NumRecords = 0;
        }
        public Int32 NumFileKeyPhrases { get; set; }
        public Int32 NumFileSubjectObjects { get; set; }
        public Int32 NumFileNamedEntities { get; set; }
        public Int32 NumRecordAuthorityKeyPhrases { get; set; }
        public Int32 NumItems { get; set; }
        public Int32 NumRecordItems { get; set; }
        public Int32 NumRecords { get; set; }
    }

    public class SentenceStat
    {
        public SentenceStat()
        {
            this.SchemaUri = "";
            this.Function = "";
            this.Activity = "";
            this.ClassNo = "";
            this.NumEntries = 0;
        }
        public string SchemaUri { get; set; }
        public string Function { get; set; }
        public string Activity { get; set; }
        public string ClassNo { get; set; }
        public Int32 NumEntries { get; set; }
    }

    public class SystemSecurityClassificationConfig
    {
        public SystemSecurityClassificationConfig()
        {
            this.SecurityClassificationFieldName = "";
            this.DLMFieldName = "";
        }
        public string SecurityClassificationFieldName { get; set; }
        public string DLMFieldName { get; set; }
    }

    public class SystemRecordIdentificationConfig
    {
        public SystemRecordIdentificationConfig()
        {
            this.ContextLevel = "folder";
            this.LevelRecordStart = 0;
        }
        public string ContextLevel { get; set; }
        public Int32 LevelRecordStart { get; set; }
    }
}
