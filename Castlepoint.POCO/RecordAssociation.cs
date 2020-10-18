using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class RecordAuthorityMatchStatus
    {
        public RecordAuthorityMatchStatus() { }
        public RecordAuthorityMatchStatus(string partitionKey, string rowKey, string status)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Status = status;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Status { get; set; }
    }

    public class RecordAssociationKeyPhraseCount
    {
        public RecordAssociationKeyPhraseCount() { }
        public RecordAssociationKeyPhraseCount(string partitionKey, string rowKey, string location)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.KeyPhraseLocation = location;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string KeyPhraseLocation { get; set; }
        public int KeyPhraseCount { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
        /// <summary>
        /// The frequency of this key phrase compared to other key phrases in the Document
        /// </summary>
        public decimal DocumentFrequency { get; set; }

    }

    public class RecordAssociationFileMetadata
    {
        public RecordAssociationFileMetadata() { }
        public RecordAssociationFileMetadata(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string FieldName { get; set; }
        public string FieldValue { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }

    public class RecordAssociationKeyPhrase
    {
        public RecordAssociationKeyPhrase() { }
        public RecordAssociationKeyPhrase(string partitionKey, string rowKey, string location)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.KeyPhraseLocation = location;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string KeyPhraseLocation { get; set; }
        public int SplitPageNumber { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }

    public class RecordAssociationKeyPhraseReverse
    {
        public RecordAssociationKeyPhraseReverse() { }
        public RecordAssociationKeyPhraseReverse(string partitionKey, string rowKey, string location)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.KeyPhraseLocation = location;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string KeyPhraseLocation { get; set; }
        public int SplitPageNumber { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }

    public class RecordAssociationKeyPhraseReverseWord
    {
        public RecordAssociationKeyPhraseReverseWord() { }
        public RecordAssociationKeyPhraseReverseWord(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
        /// <summary>
        /// Word from the keyphrase
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Combination key of KeyPhrase|RecordAssociation
        /// </summary>
        public string RowKey { get; set; }
        public int WordNumber { get; set; }
        public int TotalWords { get; set; }
    }

    public class RecordAssociationSubjectObject
    {
        public RecordAssociationSubjectObject() { }
        public RecordAssociationSubjectObject(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string Subject { get; set; }
        public string Object { get; set; }
        public string Relationship { get; set; }
        public string SystemUri { get; set; }
    }

    public class RecordAssociationNamedEntity
    {
        public RecordAssociationNamedEntity() { }
        public RecordAssociationNamedEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string Type { get; set; }
        public string OriginalText { get; set; }
        public string NamedEntityLocation { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }

    public class RecordAssociationNamedEntityReverse
    {
        public RecordAssociationNamedEntityReverse() { }
        public RecordAssociationNamedEntityReverse(string partitionKey, string rowKey, string location)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.NamedEntityLocation = location;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string Type { get; set; }
        public string OriginalText { get; set; }

        public string NamedEntityLocation { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }

    public class RecordAssociationNamedEntityCount
    {
        public RecordAssociationNamedEntityCount() { }
        public RecordAssociationNamedEntityCount(string partitionKey, string rowKey, string location)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.NamedEntityLocation = location;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceFileName { get; set; }
        public string Type { get; set; }
        public string OriginalText { get; set; }
        public int NamedEntityCount { get; set; }
        public string NamedEntityLocation { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
        public decimal DocumentFrequency { get; set; }
    }

    public class RecordAssociationToRecord
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
    }

    public class RecordAssociation
    {
        public RecordAssociation(string sourceUri, string itemUri)
        {
            this.PartitionKey = sourceUri;
            this.RowKey = itemUri;
        }


        public RecordAssociation() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string SourceUri { get { return this.PartitionKey; } }
        public string ItemUri { get { return this.RowKey; } }
        public string AssociationUri { get; set; }
        public string ItemName { get; set; }
        public string KeyPhrases { get; set; }

    }
}
