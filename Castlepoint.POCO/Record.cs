using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class RecordCategorised
    {
        public RecordCategorised() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime LastCategorised { get; set; }
        public string LastCategorisedGuid { get; set; }
        public DateTime NextSentenceDate { get; set; }
        public string JsonMatchSummary { get; set; }

    }

    public class RecordIdUpdate
    {
        public RecordIdUpdate() { }
        public RecordIdUpdate(POCO.Record record)
        {
            this.PartitionKey = record.PartitionKey;
            this.RowKey = record.RowKey;
            this.RecordId = record.RecordId;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string RecordId { get; set; }

    }

    public class RecordCreationDate
    {
        public RecordCreationDate() { }
        public RecordCreationDate(POCO.Record record)
        {
            this.PartitionKey = record.PartitionKey;
            this.RowKey = record.RowKey;
            this.Created = record.Created;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime Created { get; set; }

    }
    public class Record
    {
        public Record() { }
        public Record(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.LastContentsUpdated = Utils.AzureTableMinDateTime;
            this.LastCategorised = Utils.AzureTableMinDateTime;
            this.NextSentenceDate = Utils.AzureTableMinDateTime;
            this.ExpiryDate = Utils.AzureTableMinDateTime;
            this.LastUpdated = Utils.AzureTableMinDateTime;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string RecordUri { get; set; }
        public DateTime Created { get; set; }
        public string Label { get; set; }
        public string Type { get; set; }
        public DateTime LastContentsUpdated { get; set; }
        public DateTime LastCategorised { get; set; }
        public DateTime NextSentenceDate { get; set; }
        public DateTime ExpiryDate { get; set; }
        public DateTime LastUpdated { get; set; }
        public string ItemUri { get; set; }
        public string SourceUri { get; set; }
        public string LastCategorisedGuid { get; set; }
        public string UniqueId { get; set; }
        public string Stats { get; set; }
        public string RASchemaUri { get; set; }
        public string Function { get; set; }
        public string Activity { get; set; }
        public string ClassNo { get; set; }
        public string SchemaUri { get; set; }
        public string ChangeFlagId { get; set; }
        public string RecordId { get; set; }
        public string JsonMatchSummary { get; set; }
        public string JsonRecordConfig { get; set; }

    }

    public class RecordConfig
    {
        public RecordConfig()
        {
            this.NoProcessRecord = false;
        }
        public bool NoProcessRecord { get; set; }
    }

    public class RecordChangeFlag
    {
        public RecordChangeFlag() { }
        public RecordChangeFlag(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ChangeFlagId { get; set; }
        public string LastCategorisedGuid { get; set; }

    }

    public class RecordToRecordAssociationWithMatchInfo : POCO.RecordToRecordAssociation
    {
        public string KeyPhrase { get; set; }
        public string NamedEntity { get; set; }
        public string SubjectObject { get; set; }
        public string OntologyTerm { get; set; }
    }

    public class RecordToRecordAssociation
    {
        public RecordToRecordAssociation() { }
        public RecordToRecordAssociation(string pkey, string rkey)
        {
            this.PartitionKey = pkey;
            this.RowKey = rkey;
            this.CreationTime = Utils.AzureTableMinDateTime;
            this.LastModifiedTime = Utils.AzureTableMinDateTime;
            this.Created = Utils.AzureTableMinDateTime;
            this.LastUpdated = Utils.AzureTableMinDateTime;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime CreationTime { get; set; }
        public DateTime LastModifiedTime { get; set; }
        public string Type { get; set; }
        public string Label { get; set; }
        public string FileName { get; set; }
        //TODO for Azure, will break Mongo because the props don't exist
        public DateTime Created { get; set; }
        public DateTime LastUpdated { get; set; }

    }

    public class RecordClassificationEntity
    {
        public RecordClassificationEntity(string sourceUri, string itemUri)
        {
            this.PartitionKey = sourceUri;
            this.RowKey = itemUri;
        }

        public RecordClassificationEntity()
        {
            this.ClassNo = "0";
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceUri { get { return this.PartitionKey; } }
        public string ItemUri { get { return this.RowKey; } }
        public string RASchemaUri { get; set; }
        public string Function { get; set; }
        public string Activity { get; set; }
        public string ClassNo { get; set; }
        public DateTime Created { get; set; }
        public DateTime LastUpdated { get; set; }
        public DateTime ExpiryDate { get; set; }
        //public DateTime SentenceDate {get; set;}
        public DateTime NextSentenceDate { get; set; }
        public DateTime LastCategorised { get; set; }
        public string LastCategorisedGuid { get; set; }
        public string JsonMatchSummary { get; set; }
    }

    public class RecordClassificationHistoryEntity 
    {
        public RecordClassificationHistoryEntity(RecordClassificationEntity recordClassEntity)
        {
            DateTime now = DateTime.UtcNow;
            this.PartitionKey = Utils.CleanTableKey(recordClassEntity.RowKey);
            this.RowKey = Utils.CleanTableKey(now.ToString(Utils.ISODateFormat));
            this.Function = recordClassEntity.Function;
            this.Activity = recordClassEntity.Activity;
            this.ClassNo = recordClassEntity.ClassNo;
            //this.Created = recordClassEntity.Created;
            //this.LastUpdated = recordClassEntity.LastUpdated;
            this.ExpiryDate = recordClassEntity.ExpiryDate;
            this.NextSentenceDate = recordClassEntity.NextSentenceDate;
            this.LastCategorised = recordClassEntity.LastCategorised;
            this.LastCategorisedGuid = recordClassEntity.LastCategorisedGuid;
            this.JsonMatchSummary = recordClassEntity.JsonMatchSummary;
        }

        public RecordClassificationHistoryEntity()
        {
            this.ClassNo = "0";
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string RecordUri { get { return this.PartitionKey; } }
        public string Function { get; set; }
        public string Activity { get; set; }
        public string ClassNo { get; set; }
        //public DateTime Created { get; set; }
        //public DateTime LastUpdated { get; set; }
        public DateTime ExpiryDate { get; set; }
        //public DateTime SentenceDate {get; set;}
        public DateTime NextSentenceDate { get; set; }
        public DateTime LastCategorised { get; set; }
        public string LastCategorisedGuid { get; set; }
        public string JsonMatchSummary { get; set; }
    }
}
