using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class CaptureRegexMatch
    {
        /// <summary>
        /// System Id
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Source document + | + Capture text
        /// </summary>
        public string RowKey { get; set; }
        public string RecordUri { get; set; }
        public string SourceDocumentUri { get; set; }
        public string CaptureText { get; set; }
        public string CaptureType { get; set; }
        public string JsonConfidence { get; set; }
        public int ConfidenceScore { get; set; }
        public string SystemType { get; set; }
        public string SystemUri { get; set; }
    }

    public class CaptureRegexMatchReverse
    {
        /// <summary>
        /// Capture type
        /// </summary>
        public string PartitionKey { get; set; }
        /// <summary>
        /// Capture text + | + source document
        /// </summary>
        public string RowKey { get; set; }
        public string RecordUri { get; set; }
        public string SourceDocumentUri { get; set; }
        public string CaptureText { get; set; }
        public string CaptureType { get; set; }
        public string JsonConfidence { get; set; }
        public int ConfidenceScore { get; set; }
        public string SystemType { get; set; }
        public string SystemUri { get; set; }
    }
}
