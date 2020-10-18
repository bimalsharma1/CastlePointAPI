using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Castlepoint.POCO;
using Castlepoint.Utilities;

namespace Castlepoint.DataFactory
{
    public class MongoManagedService : POCO.ManagedService
    {
    }
    public class AzureManagedService : EntityAdapter<POCO.ManagedService>
    {
        public AzureManagedService() { }
        public AzureManagedService(POCO.ManagedService o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class ManagedService
    {
        public static List<POCO.ManagedService> GetManagedService(DataConfig providerConfig, List<Filter> filters)
        {

            List<POCO.ManagedService> docs = new List<POCO.ManagedService>();

            switch (providerConfig.ProviderType)
            {
                case ProviderType.Azure:

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureManagedService> azdata = new List<AzureManagedService>();
                    AzureTableAdaptor<AzureManagedService> adaptor = new AzureTableAdaptor<AzureManagedService>();
                    azdata = adaptor.ReadTableData(providerConfig, POCO.TableNames.Azure.ManagedService.Service, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        docs.Add(doc.Value);
                    }

                    break;
                case ProviderType.Mongo:
                    var collection = Utils.GetMongoCollection<MongoManagedService>(providerConfig, POCO.TableNames.Mongo.ManagedService.Service);

                    FilterDefinition<MongoManagedService> filter = Utils.GenerateMongoFilter<MongoManagedService>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var doc in documents)
                    {
                        docs.Add(doc);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return docs;
        }
    }


}
