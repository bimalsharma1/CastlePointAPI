using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class RecordMatchSummary
    {
        public int TotalRecordAssociations { get; set; }
        public int NoRecordAuthority { get; set; }
        public int NoRecordAuthorityKeyPhrases { get; set; }
        public int NoMatches { get; set; }
        public int FileExtensionExcluded { get; set; }
        public int FileExtensionNotIncluded { get; set; }
        public int Match { get; set; }
        public int NoKeyPhraseNamedEnt { get; set; }
        public int NoFileExtension { get; set; }
        public int PreviousMatch { get; set; }
        public int PreviousNoMatch { get; set; }
        public int Unknown { get; set; }

    }

    public class RecordAuthorityMatchResult
    {
        
        public RecordAuthorityMatchResult(string raSchemaUri, string function, string activity, string classNo, int numMatches)
        {
            this.RASchemaUri = raSchemaUri;
            this.Function = function;
            this.Activity = activity;
            this.ClassNo = classNo;
            this.NumMatches = numMatches;
            this.Outcome = "match";
        }

        public RecordAuthorityMatchResult(string raSchemaUri, string function, string activity, string classNo)
        {
            this.RASchemaUri = raSchemaUri;
            this.Function = function;
            this.Activity = activity;
            this.ClassNo = classNo;
            this.NumMatches = 1;    // Default to one match
            this.Outcome = "match";
        }

        public RecordAuthorityMatchResult()
        {
            this.ClassNo = "0";
            this.NumMatches = 0;
            this.ContentType = 0;
            this.Retention = 0;
            this.Outcome = "match"; 
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string RASchemaUri { get; set; }
        public string Function { get; set; }
        public string Activity { get; set; }
        public string ClassNo { get; set; }
        public int NumMatches { get; set; }
        public int NumMatchesRequired { get; set; }
        public int ContentType { get; set; }
        public decimal Retention { get; set; }
        public string Trigger { get; set; }
        public string ItemUri { get; set; }
        public string KeyPhrase { get; set; }
        public string NamedEntity { get; set; }
        public string NamedEntityType { get; set; }
        public decimal Confidence { get; set; }
        public string Outcome { get; set; }
    }






    public class RecordAuthorityMatch
    {
        public RecordAuthorityMatch() { }
        public RecordAuthorityMatch(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SourceUri { get { return this.PartitionKey; } }
        public string ItemUri { get { return this.RowKey; } }
        public string KeyPhrases { get; set; }
        public string RecordAuthorityMatches { get; set; }
    }

    public class RecordAuthorityTermRules
    {
        public RecordAuthorityTermRules()
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
        public List<string> MatchDefinition { get; set; }
        /// <summary>
        /// Specifies where the data to check is located. Any combination of:
        /// keyphrase
        /// namedentity
        /// metadata
        /// </summary>
        public List<string> SourceData { get; set; }
        public string BooleanType { get; set; }
        public string Label { get; set; }
    }

    public class RecordAuthorityKeyPhrase
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SchemaUri { get { return this.PartitionKey; } }
        public string KeyPhrase { get { return this.RowKey; } }
        public string Function { get; set; }
        public string Activity { get; set; }
        public Int32 ClassNo { get; set; }
        public string ClassLabel { get; set; }
        public string Description { get; set; }
        public List<string> KeyPhraseNGrams { get; set; }
        public string JsonTermRules { get; set; }
        /// <summary>
        /// One of:
        /// and, or, nand, nor
        /// </summary>
        public string MatchBooleanType { get; set; }
        /// <summary>
        /// Allows setting a priority for matching. Match priorities are tested independently of other priorities in order
        /// For example: 5 x matches with Priority=1 are tested first, if any matches are found they are returned without testing other priority numbers
        /// For example: if no priority matches are found, system works through other priority numbers until all have been tested
        /// MatchPriority=0 is the lowest (and default) priority
        /// </summary>
        public int MatchPriority { get; set; }
        /// <summary>
        /// Marks a Record Authority entry as having a Retain Permanent requirement. This is equivalent to the NAA Retain National Archives (RNA)
        /// and QLD Retain Start Archive (RSA)
        /// </summary>
        public bool RetainPermanent { get; set; }
    }

    public class RecordAuthorityNamedEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string SchemaUri { get { return this.PartitionKey; } }
        public string NamedEntity { get { return this.RowKey; } }
        public string NamedEntityType { get; set; }
        public string Function { get; set; }
        public string Activity { get; set; }
        public Int32 ClassNo { get; set; }
        public string Description { get; set; }
        public List<string> NamedEntityNGrams { get; set; }
    }


}
