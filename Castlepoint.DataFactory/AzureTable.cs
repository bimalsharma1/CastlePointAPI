using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Castlepoint.DataFactory
{
    public class AzureTableAdaptor<T> where T : ITableEntity,new()
    {
        internal List<T> ReadTableData(DataConfig providerConfig, string tableName)
        {
            string combinedFilter = string.Empty;
            return ReadTableData(providerConfig, tableName, combinedFilter);
        }

        /// <summary>
        /// Reads data from an Azure table
        /// </summary>
        /// <param name="providerConfig"></param>
        /// <param name="tableName"></param>
        /// <param name="filter">Azure Table query filter</param>
        /// <returns></returns>
        internal List<T> ReadTableData(DataConfig providerConfig, string tableName, string filter)
        {
            return ReadTableData(providerConfig, tableName, filter, 1000);
        }


        /// <summary>
        /// Reads data from an Azure table
        /// </summary>
        /// <param name="providerConfig"></param>
        /// <param name="tableName"></param>
        /// <param name="filter">Azure Table query filter</param>
        /// <returns></returns>
        internal List<T> ReadTableData(DataConfig providerConfig, string tableName, string filter, int maxRows)
        {
            var data = new List<T>();

            TableContinuationToken token = null;
            TableContinuationToken nextToken = null;

            do
            {
                token = nextToken;
                List<T> queryData = ReadTableDataWithToken(providerConfig, tableName, filter, maxRows, token, out nextToken);
                data.AddRange(queryData);

            } while (nextToken != null); // && (query.TakeCount == null || data.Count<query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            //CloudTable table = Utils.GetCloudTable(providerConfig, tableName);

            //var query = new TableQuery<T>().Where(filter);
            //query.TakeCount = maxRows;

            //TableContinuationToken token = null;

            //var runningQuery = new TableQuery<T>()
            //{
            //    FilterString = query.FilterString,
            //    SelectColumns = query.SelectColumns
            //};

            //do
            //{
            //    runningQuery.TakeCount = query.TakeCount - data.Count;

            //    Task<TableQuerySegment<T>> tSeg = table.ExecuteQuerySegmentedAsync(runningQuery, token);
            //    tSeg.Wait();
            //    token = tSeg.Result.ContinuationToken;
            //    data.AddRange(tSeg.Result);

            //} while (token != null && (query.TakeCount == null || data.Count < query.TakeCount.Value));    //!ct.IsCancellationRequested &&

            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="providerConfig"></param>
        /// <param name="tableName"></param>
        /// <param name="filter"></param>
        /// <param name="maxRows"></param>
        /// <param name="currentToken"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal List<T> ReadTableDataWithToken(DataConfig providerConfig, string tableName, string filter, int maxRows, TableContinuationToken currentToken, out TableContinuationToken token)
        {
            var data = new List<T>();

            CloudTable table = Utils.GetCloudTable(providerConfig, tableName);

            var query = new TableQuery<T>().Where(filter);

            query.TakeCount = maxRows;

            var runningQuery = new TableQuery<T>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            runningQuery.TakeCount = query.TakeCount - data.Count;

            Task<TableQuerySegment<T>> tSeg = table.ExecuteQuerySegmentedAsync(runningQuery, currentToken);
            tSeg.Wait();
            token = tSeg.Result.ContinuationToken;
            data.AddRange(tSeg.Result);

            return data;
        }
    }
}
