using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class OntologyMatchStatus
    {
        /// <summary>
        /// Ontology uri
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Record association uri
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// Batch status
        /// blank or completed
        /// </summary>
        public string BatchStatus { get; set; }
    }
    public class OntologyTerm
    {
        /// <summary>
        /// Ontology uri
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Term in lower case
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// RowKey of parent term
        /// </summary>
        public string ParentTerm { get; set; }
        /// <summary>
        /// Term
        /// </summary>
        public string Term { get; set; }
        /// <summary>
        /// List of alternate terms for the Term
        /// </summary>
        public string JsonAlternateTerms { get; set; }
        /// <summary>
        /// List of matching rules for the Term
        /// </summary>
        public string JsonTermRules { get; set; }
    }

    public class OntologyTermRules
    {
        public OntologyTermRules()
        {
            this.RuleVersion = "1.0";
        }
        public string RuleVersion { get; set; }
        /// <summary>
        /// Type of match to perform. One of:
        /// nomatch - term is ignored (use for setting a parent term or disabling a term match
        /// exact - term is exact matched (case is ignored)
        /// startswith - term is matched when it starts with the provided term (case is ignored)
        /// endswith - term is matched when it ends with the provided term (case is ignored)
        /// contains - term is matched when it contains the provided term (case is ignored)
        /// ngrams - term is matched using ngrams (case is ignored)
        /// regex - term is matched using regular expression
        /// </summary>
        public string MatchType { get; set; }
        public string MatchDefinition { get; set; }
    }

    public class OntologyTermMatchResults
    {
        public OntologyTermMatchResults()
        {
            this.NumRecordAssociationMatches = 0;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ParentTerm { get; set; }
        public string Term { get; set; }
        public int NumRecordAssociationMatches { get; set; }
        public int NumChildMatches { get; set; }
        public int TotalMatches { get { return NumRecordAssociationMatches + NumChildMatches; } }
    }

    public class OntologyMatchSummary
    {
        /// <summary>
        /// Ontology Uri
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// System Uri
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// Type of summary
        /// This value is used to deserialise the summary
        /// </summary>
        public string SummaryType { get; set; }
        /// <summary>
        /// Date the summary was created
        /// </summary>
        public DateTime DateCreated { get; set; }
        /// <summary>
        /// JSON match summary string
        /// </summary>
        public string JsonMatchSummary { get; set; }
    }

    public class OntologyTermMatch
    {
        public OntologyTermMatch() { }
        /// <summary>
        /// Ontology uri
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Term (lower case key)
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// Ontology uri
        /// </summary>
        public string OntologyUri { get; set; }
        /// <summary>
        /// Term (normal case)
        /// </summary>
        public string Term { get; set; }
        /// <summary>
        /// Record Association uri
        /// </summary>
        public string RecordAssociation { get; set; }
        /// <summary>
        /// Creation date and time of the Record Association
        /// </summary>
        public DateTime RecordAssociationCreated { get; set; }
        /// <summary>
        /// Modified date and time of the Record Association
        /// </summary>
        public DateTime RecordAssociationModified { get; set; }
        /// <summary>
        /// ID of parent term in the format PartitionKey|RowKey
        /// </summary>
        public string ParentTerm { get; set; }
        /// <summary>
        /// The type of match used for the term (such as exact, startswith, endswith etc)
        /// </summary>
        public string MatchType { get; set; }
        /// <summary>
        /// The source data that was matched against
        /// i.e. keyphrase, namedentity, metadata etc
        /// </summary>
        public string MatchSource { get; set; }
        /// <summary>
        /// RowKey of the Term that has been matched
        /// </summary>
        public string TermRowKey { get; set; }
        public decimal Confidence { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }
    public class OntologyTermMatchReverse
    {
        public OntologyTermMatchReverse() { }
        public OntologyTermMatchReverse(OntologyTermMatch match)
        {
            // Reverse the keys for lookup and indexing performance
            this.PartitionKey = match.RecordAssociation;
            this.RowKey = match.PartitionKey + "|" + match.RowKey;
            this.OntologyUri = match.OntologyUri;
            this.MatchType = match.MatchType;
            this.MatchSource = match.MatchSource;
            this.ParentTerm = match.ParentTerm;
            this.RecordAssociation = match.RecordAssociation;
            this.Term = match.Term;
            this.TermRowKey = match.TermRowKey;
        }
        /// <summary>
        /// Record Association
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Ontology Uri + "|" + Ontology Term
        /// </summary>
        public string RowKey { get; set; }
        /// <summary>
        /// Ontology uri
        /// </summary>
        public string OntologyUri { get; set; }
        /// <summary>
        /// Record Association uri
        /// </summary>
        public string RecordAssociation { get; set; }
        /// <summary>
        /// Term
        /// </summary>
        public string Term { get; set; }
        /// <summary>
        /// ID of parent term in the format PartitionKey|RowKey
        /// </summary>
        public string ParentTerm { get; set; }
        /// <summary>
        /// The type of match used for the term (such as exact, startswith, endswith etc)
        /// </summary>
        public string MatchType { get; set; }
        public string MatchSource { get; set; }
        public string TermRowKey { get; set; }
        public string SystemUri { get; set; }
        public string SystemType { get; set; }
    }

    public class Ontology
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Description { get; set; }
        public string Label { get; set; }
        public string Type { get; set; }
        public string UniqueId { get; set; }
    }

    public class OntologyAssigned
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public bool Enabled { get; set; }
    }

    public class RecordAuthorityFilter
    {
        public RecordAuthorityFilter() { }
        public RecordAuthorityFilter(string RASchemaUri, string Function, string Class)
        {
            this.RASchemaUri = RASchemaUri;
            this.Class = Class;
            this.Function = Function;
        }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string RASchemaUri { get; set; }
        public string Class { get; set; }
        public string Function { get; set; }
        public string JsonRecordMatch { get; set; }
    }

    public class RecordAuthorityFunctionActivityEntry
    {
        public RecordAuthorityFunctionActivityEntry(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public RecordAuthorityFunctionActivityEntry()
        {
            this.ContentType = 0;
            this.EntryNo = 0;
            this.Retention = 0;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Activity { get; set; }
        public string Authority { get; set; }
        public int ContentType { get; set; }
        public string DescriptionofRecords { get; set; }
        public string DisposalAction { get; set; }
        public int EntryNo { get; set; }
        public string Function { get; set; }
        public string FunctionDescription { get; set; }
        public decimal Retention { get; set; }
        public string Trigger { get; set; }
        public bool RetainPermanent { get; set; }
        public string ClassLabel { get; set; }
    }
}
