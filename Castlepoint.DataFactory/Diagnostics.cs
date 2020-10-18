using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    class MongoDiagnostic : POCO.CPDiagnostic
    {
        public ObjectId _id { get; set; }

    }
    class AzureDiagnostic : EntityAdapter<POCO.CPDiagnostic>
    {
        public AzureDiagnostic() { }
        public AzureDiagnostic(POCO.CPDiagnostic o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }
    public static class Diagnostics
    {
        internal static class AzureTableNames
        {
            internal static string FileProcessing { get { return "diagfileprocessing"; } }
        }
        internal static class MongoTableNames
        {
            internal static string FileProcessing { get { return "diagfileprocessing"; } }
        }

        public static List<POCO.CPDiagnostic> GetDiagnosticEntries(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.CPDiagnostic> diags = new List<POCO.CPDiagnostic>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureDiagnostic> azdata = new List<AzureDiagnostic>();
                    AzureTableAdaptor<AzureDiagnostic> adaptor = new AzureTableAdaptor<AzureDiagnostic>();
                    azdata = adaptor.ReadTableData(providerConfig, AzureTableNames.FileProcessing + DateTime.UtcNow.ToString(Utils.TableSuffixDateFormatYM), combinedFilter);

                    foreach (var doc in azdata)
                    {
                        diags.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoDiagnostic>(providerConfig, MongoTableNames.FileProcessing + DateTime.UtcNow.ToString(Utils.TableSuffixDateFormatYM));

                    FilterDefinition<MongoDiagnostic> filter = Utils.GenerateMongoFilter<MongoDiagnostic>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        diags.Add(keyphrase);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return diags;
        }
    }
}
