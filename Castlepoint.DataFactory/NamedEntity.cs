using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Castlepoint.DataFactory
{
    public class NamedEntity
    {
        public class MongoRecordAssociationNamedEntityReverse : POCO.RecordAssociationNamedEntityReverse
        {
            public ObjectId _id { get; set; }
        }

        public class AzureRecordAssociationNamedEntityReverse : EntityAdapter<POCO.RecordAssociationNamedEntityReverse>
        {
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }

        internal static class AzureTableNames
        {
            internal const string RecordAssociationNamedEntityReverse = "stlprecordassociationnamedentityreverse";
        }

        internal static class MongoTableNames
        {
            internal const string RecordAssociationNamedEntityReverse = "recordassociationnamedentityreverse";
        }


        public static List<POCO.RecordAssociationNamedEntityReverse> GetReverseNamedEntities(DataConfig providerConfig, List<Filter> filters)
        {
            // TODO update when Named Entity uses file name and body separately
            // Currently defaults to Named Entities in the Body of a document only
            string namedEntityLocation = "body";

            return GetReverseNamedEntities(providerConfig, filters, namedEntityLocation);
        }

        public static List<POCO.RecordAssociationNamedEntityReverse> GetReverseNamedEntities(DataConfig providerConfig, List<Filter> filters, string namedEntityLocation)
        {
            List<POCO.RecordAssociationNamedEntityReverse> namedents = new List<POCO.RecordAssociationNamedEntityReverse>();

            string tableName = string.Empty;
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    switch (namedEntityLocation)
                    {
                        case "filename":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntityReverse;
                                break;
                            }
                        case "body":
                            {
                                tableName = AzureTableNames.RecordAssociationNamedEntityReverse;
                                break;
                            }
                        default:
                            throw new ApplicationException("Named entity location not recognised: " + namedEntityLocation);
                    }

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRecordAssociationNamedEntityReverse> azdata = new List<AzureRecordAssociationNamedEntityReverse>();
                    AzureTableAdaptor<AzureRecordAssociationNamedEntityReverse> adaptor = new AzureTableAdaptor<AzureRecordAssociationNamedEntityReverse>();
                    azdata = adaptor.ReadTableData(providerConfig, tableName, combinedFilter);

                    foreach (var doc in azdata)
                    {
                        namedents.Add(doc.Value);
                    }

                    break;
                case "internal.mongodb":
                    switch (namedEntityLocation)
                    {
                        case "filename":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntityReverse;
                                break;
                            }
                        case "body":
                            {
                                tableName = MongoTableNames.RecordAssociationNamedEntityReverse;
                                break;
                            }
                        default:
                            throw new ApplicationException("Named entity location not recognised: " + namedEntityLocation);
                    }


                    var collection = Utils.GetMongoCollection<MongoRecordAssociationNamedEntityReverse>(providerConfig, tableName);

                    FilterDefinition<MongoRecordAssociationNamedEntityReverse> filter = Utils.GenerateMongoFilter<MongoRecordAssociationNamedEntityReverse>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var keyphrase in documents)
                    {
                        namedents.Add(keyphrase);
                    }

                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return namedents;
        }
    }
}
