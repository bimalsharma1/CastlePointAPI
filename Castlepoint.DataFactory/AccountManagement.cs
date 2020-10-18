using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Logging;

namespace Castlepoint.DataFactory
{
    class MongoAccountManaged : POCO.Account.Managed
    {
        public ObjectId _id { get; set; }
    }

    class AzureAccountManaged : EntityAdapter<POCO.Account.Managed>
    {
        public AzureAccountManaged() { }
        public AzureAccountManaged(POCO.Account.Managed o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public static class AccountManagement
    {
        public static List<POCO.Account.Managed> Get(DataConfig providerConfig)
        {
            List<Filter> filters = new List<Filter>();
            return GetByFilter(providerConfig, filters);
        }

        public static List<POCO.Account.Managed> GetByFilter(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.Account.Managed> auditEntries = new List<POCO.Account.Managed>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureAccountManaged> azdata = new List<AzureAccountManaged>();
                    AzureTableAdaptor<AzureAccountManaged> adaptor = new AzureTableAdaptor<AzureAccountManaged>();
                    azdata = adaptor.ReadTableData(providerConfig, POCO.TableNames.Azure.Account.ManagedAccount, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        auditEntries.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoAccountManaged>(providerConfig, POCO.TableNames.Mongo.Account.ManagedAccount);

                    FilterDefinition<MongoAccountManaged> filter = Utils.GenerateMongoFilter<MongoAccountManaged>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var logentry in documents)
                    {
                        auditEntries.Add(logentry);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return auditEntries;
        }
    }
}
