using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class KeyPhraseToItemLookup
    {
        public KeyPhraseToItemLookup(string sourceUri, string itemUri)
        {
            this.PartitionKey = sourceUri;
            this.RowKey = itemUri;
            this.ItemName = "";
        }

        public KeyPhraseToItemLookup() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceUri { get { return this.PartitionKey; } }
        public string ItemUri { get { return this.RowKey; } }
        public string KeyPhrase { get; set; }
        public string ItemName { get; set; }

        public string KeyPhraseLocation { get; set; }

    }

    public class MetadataToRecordLookup
    {
        public MetadataToRecordLookup(string recordUri, string itemUri, string raSchemaUri, string metafield, string metavalue)
        {
            this.PartitionKey = recordUri;
            string rowKey = "";
            // Append item uri and key phrase as the row key
            if (!itemUri.EndsWith("|")) { rowKey = Utils.CleanTableKey(itemUri + "|" + metavalue); }
            else { rowKey = Utils.CleanTableKey(itemUri + metavalue); }
            this.RowKey = rowKey;
            this.ItemUri = itemUri;
            this.RASchemaUri = raSchemaUri;
            this.MetadataField = metafield;
            this.MetadataValue = metavalue;
        }

        public MetadataToRecordLookup() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string RecordUri { get { return this.PartitionKey; } }

        public string ItemKeyPhraseUri { get { return this.RowKey; } }
        public string ItemUri { get; set; }
        public string ItemType { get; set; }
        public string RASchemaUri { get; set; }
        public string RAFunction { get; set; }
        public string RAActivity { get; set; }
        public string RAClass { get; set; }
        public string MetadataField { get; set; }
        public string MetadataValue { get; set; }
        public int NumMatches { get; set; }
    }

    public class KeyPhraseToRecordLookup
    {
        public KeyPhraseToRecordLookup(string recordUri, string itemUri, string raSchemaUri, string keyPhrase)
        {
            this.PartitionKey = recordUri;
            string rowKey = "";
            // Append item uri and key phrase as the row key
            if (!itemUri.EndsWith("|")) { rowKey = Utils.CleanTableKey( itemUri + "|" + keyPhrase); }
            else { rowKey = Utils.CleanTableKey( itemUri + keyPhrase); }
            this.RowKey = rowKey;
            this.ItemUri = itemUri;
            this.RASchemaUri = raSchemaUri;
            this.KeyPhrase = keyPhrase;
        }

        public KeyPhraseToRecordLookup() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string RecordUri { get { return this.PartitionKey; } }

        public string ItemKeyPhraseUri { get { return this.RowKey; } }
        public string ItemUri { get; set; }
        public string ItemType { get; set; }
        public string RASchemaUri { get; set; }
        public string RAFunction { get; set; }
        public string RAActivity { get; set; }
        public string RAClass { get; set; }
        public string RATrigger { get; set; }
        public decimal RARetention { get; set; }
        public bool RARetainPermanent { get; set; }
        public string KeyPhrase { get; set; }
        public string KeyPhraseLocation { get; set; }
        public int NumMatches { get; set; }
    }

    public class NamedEntityToRecordLookup
    {
        public NamedEntityToRecordLookup(string recordUri, string itemUri, string raSchemaUri, string namedEntity, string namedEntityType)
        {
            this.PartitionKey = recordUri;
            string rowKey = "";
            // Append item uri and key phrase as the row key
            if (!itemUri.EndsWith("|")) { rowKey = Utils.CleanTableKey(itemUri + "|" + namedEntity); }
            else { rowKey = Utils.CleanTableKey(itemUri + namedEntity); }
            this.RowKey = rowKey;
            this.ItemUri = itemUri;
            this.RASchemaUri = raSchemaUri;
            this.NamedEntity = namedEntity;
            this.NamedEntityType = namedEntityType;
        }

        public NamedEntityToRecordLookup() { }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string RecordUri { get { return this.PartitionKey; } }

        public string ItemKeyPhraseUri { get { return this.RowKey; } }
        public string ItemUri { get; set; }
        public string RASchemaUri { get; set; }
        public string RAFunction { get; set; }
        public string RAActivity { get; set; }
        public string RAClass { get; set; }
        public string RATrigger { get; set; }
        public decimal RARetention { get; set; }
        public bool RARetainPermanent { get; set; }
        public string NamedEntity { get; set; }
        public string NamedEntityType { get; set; }
        public string NamedEntityLocation { get; set; }
        public int NumMatches { get; set; }
    }
}
