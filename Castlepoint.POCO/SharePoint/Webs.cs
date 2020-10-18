using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.SharePoint
{
    public class SharePointWebInfoLastUpdated
    {
        public SharePointWebInfoLastUpdated()
        {

        }
        public SharePointWebInfoLastUpdated(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }


        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string LastItemModifiedDate { get; set; }

    }
    public class SharePointWebInfo
    {


        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string Created { get; set; }
        public string Description { get; set; }
        public string WebId { get; set; }
        public int Language { get; set; }
        public string LastItemModifiedDate { get; set; }
        public string ServerRelativeUrl { get; set; }
        public string Title { get; set; }
        public string Url { get; set; }
    }

    public class SharePointWebInfoResultJson
    {
        public SharePointWebInfo d { get; set; }
    }

    //public class WebInfoNamespace
    //{
    //    public SharePointWebInfo FirstUniqueAncestorSecurableObject { get; set; }
    //}

    public class SearchQueryWebsJson
    {
        public SearchQueryNamespace d { get; set; }
    }
    public class SearchQueryNamespace
    {
        public SearchQuery query { get; set; }
    }

    public class SearchQuery
    {
        public PrimaryQueryResult PrimaryQueryResult { get; set; }
    }
    public class PrimaryQueryResult
    {
        public RelevantResults RelevantResults { get; set; }
    }
    public class RelevantResults
    {
        public ResultProperties Properties { get; set; }
        public ResultTable Table { get; set; }
    }

    public class ResultTable
    {
        public ResultRows Rows { get; set; }
    }

    public class ResultRows
    {
        public List<ResultElementRow> results { get; set; }
    }

    public class ResultElementRow
    {
        public ResultCells Cells { get; set; }
    }
    public class ResultCells
    {
        public List<SearchResult> results { get; set; }
    }

    public class ResultProperties
    {
        public List<SearchResult> results { get; set; }
    }

    public class SearchResult
    {
        public string Key { get; set; }
        public string Value { get; set; }
        public string ValueType { get; set; }
    }
}

