using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class LogClassification
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime DateCreated { get; set; }
        public string BatchGuid { get; set; }
        public string LogLevel { get; set; }
        public string Category { get; set; }
        public string Detail { get; set; }
        public string SystemId { get; set; }
        public string RecordId { get; set; }
        public string FileId { get; set; }
    }

    public class LogRecordProcessing
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ProcessingResultAsJson { get; set; }

    }

    public class LogFileProcessing
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ProcessingResultAsJson { get; set; }

    }

    public class LogEventProcessingTime
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ItemUri { get; set; }
        public string Event { get; set; }
        public double TotalMilliseconds { get; set; }
        public string MIMEType { get; set; }
        public int NumBytes { get; set; }
    }

    public class LogServiceEvent
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string LogId { get; set; }
        public string NodeName { get; set; }
        public string NodeIp { get; set; }
        public DateTime LogDateTime { get; set; }
        public string ServiceModule { get; set; }
        public string ServiceEvent { get; set; }
    }

    public class LogError
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string CorrelationId { get; set; }
        public string ErrorType { get; set; }
        public string ErrorDetail { get; set; }
    }
}
