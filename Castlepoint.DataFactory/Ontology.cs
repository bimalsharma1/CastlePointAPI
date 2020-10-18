using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Castlepoint.POCO;

namespace Castlepoint.DataFactory
{
    public static class Ontology
    {
        internal class AzureTableNames
        {
            internal const string Ontology = "stlpontology";
            internal const string OntologyTerms = "stlpontologyterms";
            internal const string OntologyTermMatches = "stlpontologyterms";
            internal const string SystemsAssignedOntology = "stlpsystemsassignedontology";
            internal const string RecordsAssignedOntology = "stlprecordsassignedontology";
            internal const string RecordAssociationOntologyMatch = "stlprecordassociationontologymatch";
            internal const string RecordAssociationRegexMatch = "stlprecordassociationregexmatch";
            internal const string OntologyMatchSummary = "stlpontologymatchsummary";
            internal const string OntologyMatchStatus = "stlpontologymatchstatus";
        }
        internal class MongoTableNames
        {
            internal const string Ontology = "ontology";
            internal const string OntologyTerms = "ontologyterms";
            internal const string OntologyTermMatches = "ontologyterms";
            internal const string SystemsAssignedOntology = "systemsassignedontology";
            internal const string RecordsAssignedOntology = "recordsassignedontology";
            internal const string RecordAssociationOntologyMatch = "recordassociationontologymatch";
            internal const string RecordAssociationRegexMatch = "recordassociationregexmatch";
            internal const string OntologyMatchSummary = "ontologymatchsummary";
            internal const string OntologyMatchStatus = "ontologymatchstatus";
        }

        class MongoRegexMatch : POCO.CaptureRegexMatch
        {
            public ObjectId _id { get; set; }
        }
        class MongoRegexMatchUpsert : POCO.CaptureRegexMatch
        {
            //public ObjectId _id { get; set; }
        }

        class AzureRegexMatch : EntityAdapter<POCO.CaptureRegexMatch>
        {
            public AzureRegexMatch() { }
            public AzureRegexMatch(POCO.CaptureRegexMatch o) : base(o) { }
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }



        class MongoOntology : POCO.Ontology
        {
            public ObjectId _id { get; set; }
        }
        class AzureOntology : EntityAdapter<POCO.Ontology>
        {
            public AzureOntology() { }
            public AzureOntology(POCO.Ontology o) : base(o) { }
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }
        class MongoOntologyAssigned : POCO.OntologyAssigned
        {
            public ObjectId _id { get; set; }
        }
        class AzureOntologyAssigned : EntityAdapter<POCO.OntologyAssigned>
        {
            public AzureOntologyAssigned() { }
            public AzureOntologyAssigned(POCO.OntologyAssigned o) : base(o) { }
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }
        class MongoOntologyMatchSummary : POCO.OntologyMatchSummary
        {
            public ObjectId _id { get; set; }
        }
        class AzureOntologyMatchSummary : EntityAdapter<POCO.OntologyMatchSummary>
        {
            public AzureOntologyMatchSummary() { }
            public AzureOntologyMatchSummary(POCO.OntologyMatchSummary o) : base(o) { }
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }

        class MongoOntologyTerm : POCO.OntologyTerm
        {
            public ObjectId _id { get; set; }
        }
        class AzureOntologyTerm : EntityAdapter<POCO.OntologyTerm>
        {
            public AzureOntologyTerm() { }
            public AzureOntologyTerm(POCO.OntologyTerm o) : base(o) { }
            protected override string BuildPartitionKey()
            {
                return this.Value.PartitionKey;
            }

            protected override string BuildRowKey()
            {
                return this.Value.RowKey;
            }
        }

        public static string Add(DataConfig providerConfig, POCO.Ontology ontology)
        {
            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureOntology az = new AzureOntology(ontology);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.Ontology);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    //TODO return the inserted record id/timestamp
                    return string.Empty;


                case "internal.mongodb":
                    IMongoCollection<MongoOntology> collection = Utils.GetMongoCollection<MongoOntology>(providerConfig, MongoTableNames.Ontology);
                    MongoOntology mongoObject = Utils.ConvertType<MongoOntology>(ontology);
                    collection.InsertOne(mongoObject);
                    return string.Empty;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return string.Empty;
        }

        public static int AssignToSystem(DataConfig providerConfig, POCO.OntologyAssigned ontAssigned)
        {
            int numRows = 0;

            // Check if this exists already
            // TODO
            //List<POCO.RecordAuthorityFilter> rafilts = GetSystemAssignedOntology(providerConfig, ontAssigned);
            //if (rafilts.Count > 0)
            //{
            //    return numRows;
            //}

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    AzureOntologyAssigned az = new AzureOntologyAssigned(ontAssigned);

                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.SystemsAssignedOntology);
                    TableOperation operation = TableOperation.InsertOrMerge(az);
                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    //TODO return the inserted record id/timestamp
                    return numRows;

                case "internal.mongodb":
                    IMongoCollection<MongoOntologyAssigned> collection = Utils.GetMongoCollection<MongoOntologyAssigned>(providerConfig, MongoTableNames.SystemsAssignedOntology);
                    MongoOntologyAssigned mongoObject = Utils.ConvertType<MongoOntologyAssigned>(ontAssigned);
                    collection.InsertOne(mongoObject);
                    return numRows;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return numRows;
        }

        public static void DeleteRegexeMatchs(DataConfig providerConfig, POCO.System system, string fileUri)
        {

            List<Filter> filters = new List<Filter>();
            Filter pk = new Filter("PartitionKey", system.PartitionKey, "eq");
            filters.Add(pk);
            string rowkey = fileUri;
            Filter rk = new Filter("RowKey", rowkey, "ge");
            filters.Add(rk);
            rk = new Filter("RowKey", Utils.GetLessThanFilter(rowkey), "lt");
            filters.Add(rk);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    // Get a list of entities to delete
                    List<POCO.CaptureRegexMatch> captureMatches = GetRegexMatches(providerConfig, system, fileUri);

                    //TODO better way for bulk delete
                    foreach (POCO.CaptureRegexMatch l in captureMatches)
                    {
                        AzureRegexMatch az = new AzureRegexMatch(l);
                        az.ETag = "*";

                        CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationRegexMatch);
                        TableOperation operation = TableOperation.Delete(az);

                        Task<TableResult> tDelete = table.ExecuteAsync(operation);
                        tDelete.Wait();

                        // Check for "success with no status" code
                        if (tDelete.Result.HttpStatusCode != 204)
                        {
                            // TODO
                            bool isNotDeleted = true;
                        }

                    }

                    break;

                case "internal.mongodb":
                    FilterDefinition<BsonDocument> filter = Utils.GenerateMongoFilter<BsonDocument>(filters);

                    // Delete the rows                      
                    IMongoCollection<BsonDocument> collection = Utils.GetMongoCollection<BsonDocument>(providerConfig, Record.MongoTableNames.RecordKeyPhrases);
                    DeleteResult result = collection.DeleteMany(filter);

                    return;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }
            return;
        }

        public static bool AddRegexMatchs(DataConfig providerConfig, List<CaptureRegexMatch> matches)
        {
            bool isSaved = false;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<TableOperation> ops = new List<TableOperation>();
                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationRegexMatch);

                    foreach(POCO.CaptureRegexMatch match in matches)
                    {
                        AzureRegexMatch az = new AzureRegexMatch(match);
                        TableOperation operation = TableOperation.InsertOrReplace(az);

                        Task tUpdate = table.ExecuteAsync(operation);
                        tUpdate.Wait();
                    }

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoRegexMatchUpsert> collection = Utils.GetMongoCollection<MongoRegexMatchUpsert>(providerConfig, MongoTableNames.RecordAssociationRegexMatch);

                    foreach(POCO.CaptureRegexMatch match in matches)
                    {
                        MongoRegexMatchUpsert mongoObject = Utils.ConvertType<MongoRegexMatchUpsert>(match);


                        // Create the update filter
                        List<DataFactory.Filter> filters = new List<DataFactory.Filter>();
                        DataFactory.Filter pkFilter = new DataFactory.Filter("PartitionKey", Utils.CleanTableKey(mongoObject.PartitionKey), "eq");
                        DataFactory.Filter rkFilter = new DataFactory.Filter("RowKey", Utils.CleanTableKey(mongoObject.RowKey), "eq");
                        filters.Add(pkFilter);
                        filters.Add(rkFilter);
                        FilterDefinition<MongoRegexMatchUpsert> filter = Utils.GenerateMongoFilter<MongoRegexMatchUpsert>(filters);

                        // Create the upsert options
                        MongoDB.Driver.ReplaceOptions options = new ReplaceOptions();
                        options.IsUpsert = true;

                        // Upsert
                        collection.ReplaceOne(filter, mongoObject, options);

                    }

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            isSaved = true;
            return isSaved;
        }

        public static bool AddOntologyMatchSummary(DataConfig providerConfig, POCO.OntologyMatchSummary ontologysummary)
        {
            bool isSaved = false;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<TableOperation> ops = new List<TableOperation>();
                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.RecordAssociationOntologyMatch);

                    AzureOntologyMatchSummary az = new AzureOntologyMatchSummary(ontologysummary);
                        TableOperation operation = TableOperation.InsertOrReplace(az);

                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoOntologyMatchSummary> collection = Utils.GetMongoCollection<MongoOntologyMatchSummary>(providerConfig, MongoTableNames.RecordAssociationOntologyMatch);
                    MongoOntologyMatchSummary mongoObject = Utils.ConvertType<MongoOntologyMatchSummary>(ontologysummary);
                    collection.InsertOne(mongoObject);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            isSaved = true;
            return isSaved;
        }

        public static bool AddOntologyMatchStatus(DataConfig providerConfig, POCO.RecordToRecordAssociation recassoc, POCO.OntologyAssigned ont, string matchStatus)
        {
            bool isSaved = false;

            POCO.OntologyMatchStatus ontstat = new OntologyMatchStatus();
            ontstat.PartitionKey = ont.RowKey;
            ontstat.RowKey = recassoc.RowKey;
            ontstat.BatchStatus = matchStatus;

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    List<TableOperation> ops = new List<TableOperation>();
                    CloudTable table = Utils.GetCloudTable(providerConfig, AzureTableNames.OntologyMatchStatus);

                    AzureOntologyMatchStatus az = new AzureOntologyMatchStatus(ontstat);
                    TableOperation operation = TableOperation.InsertOrReplace(az);

                    Task tUpdate = table.ExecuteAsync(operation);
                    tUpdate.Wait();

                    break;

                case "internal.mongodb":

                    IMongoCollection<MongoOntologyMatchStatus> collection = Utils.GetMongoCollection<MongoOntologyMatchStatus>(providerConfig, MongoTableNames.OntologyMatchStatus);
                    MongoOntologyMatchStatus mongoObject = Utils.ConvertType<MongoOntologyMatchStatus>(ontstat);
                    collection.InsertOne(mongoObject);

                    break;

                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            isSaved = true;
            return isSaved;
        }

        public static List<POCO.OntologyMatchSummary> GetOntologyMatchSummary(DataConfig providerConfig, POCO.Ontology ontology)
        {
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", ontology.PartitionKey, "eq");
            filters.Add(pkFilter);

            return GetOntologyMatchSummary(providerConfig, filters);
        }

        public static List<POCO.OntologyMatchSummary> GetOntologyMatchSummary(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.OntologyMatchSummary> ontologysummaries = new List<POCO.OntologyMatchSummary>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyMatchSummary> azs = new List<AzureOntologyMatchSummary>();
                    AzureTableAdaptor<AzureOntologyMatchSummary> az = new AzureTableAdaptor<AzureOntologyMatchSummary>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.OntologyMatchSummary, combinedFilter);

                    foreach (var doc in azs)
                    {
                        ontologysummaries.Add(doc.Value);
                    }

                    return ontologysummaries;

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyMatchSummary>(providerConfig, MongoTableNames.OntologyMatchSummary);

                    FilterDefinition<MongoOntologyMatchSummary> filter = Utils.GenerateMongoFilter<MongoOntologyMatchSummary>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var o in documents)
                    {
                        ontologysummaries.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ontologysummaries;
        }

        public static List<POCO.Ontology> GetOntology(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.Ontology> ontologies = new List<POCO.Ontology>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntology> azs = new List<AzureOntology>();
                    AzureTableAdaptor<AzureOntology> az = new AzureTableAdaptor<AzureOntology>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.Ontology, combinedFilter);

                    foreach (var doc in azs)
                    {
                        ontologies.Add(doc.Value);
                    }

                    return ontologies;

                    break;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntology>(providerConfig, MongoTableNames.Ontology);

                    FilterDefinition<MongoOntology> filter = Utils.GenerateMongoFilter<MongoOntology>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var o in documents)
                    {
                        ontologies.Add(o);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ontologies;
        }

        public static List<POCO.OntologyAssigned> GetSystemAssignedOntology(DataConfig providerConfig, POCO.System system)
        {
            List<POCO.OntologyAssigned> ontologies = new List<POCO.OntologyAssigned>();

            List<Filter> filters = new List<Filter>();
            Filter systemFilter = new Filter("PartitionKey", Utils.CleanTableKey(system.PartitionKey), "eq");
            filters.Add(systemFilter);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyAssigned> azs = new List<AzureOntologyAssigned>();
                    AzureTableAdaptor<AzureOntologyAssigned> az = new AzureTableAdaptor<AzureOntologyAssigned>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.SystemsAssignedOntology, combinedFilter);

                    foreach (var doc in azs)
                    {
                        ontologies.Add(doc.Value);
                    }

                    return ontologies;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyAssigned>(providerConfig, MongoTableNames.SystemsAssignedOntology);

                    FilterDefinition<MongoOntologyAssigned> filter = Utils.GenerateMongoFilter<MongoOntologyAssigned>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var ontology in documents)
                    {
                        ontologies.Add(ontology);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ontologies;
        }

        public static List<POCO.OntologyAssigned> GetRecordAssignedOntology(DataConfig providerConfig, POCO.Record record)
        {
            List<POCO.OntologyAssigned> ontologies = new List<POCO.OntologyAssigned>();

            List<Filter> filters = new List<Filter>();
            Filter systemFilter = new Filter("PartitionKey", Utils.CleanTableKey(record.PartitionKey), "eq");
            filters.Add(systemFilter);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":

                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyAssigned> azs = new List<AzureOntologyAssigned>();
                    AzureTableAdaptor<AzureOntologyAssigned> az = new AzureTableAdaptor<AzureOntologyAssigned>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.RecordsAssignedOntology, combinedFilter);

                    foreach (var doc in azs)
                    {
                        ontologies.Add(doc.Value);
                    }

                    return ontologies;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyAssigned>(providerConfig, MongoTableNames.RecordsAssignedOntology);

                    FilterDefinition<MongoOntologyAssigned> filter = Utils.GenerateMongoFilter<MongoOntologyAssigned>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var ontology in documents)
                    {
                        ontologies.Add(ontology);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ontologies;
        }

        public static List<POCO.OntologyTermMatchResults> GetOntologyMatchSummary(DataConfig providerConfig, POCO.OntologyAssigned ontologyAssigned)
        {
            // Get the ontology terms for this ontology
            List<POCO.OntologyTerm> oTerms = DataFactory.Ontology.GetOntologyTerms(providerConfig, ontologyAssigned);

            // Convert the ontology terms into match results
            List<POCO.OntologyTermMatchResults> results = new List<POCO.OntologyTermMatchResults>();
            foreach(POCO.OntologyTerm term in oTerms)
            {
                // Create a matching result object
                POCO.OntologyTermMatchResults result = new POCO.OntologyTermMatchResults();
                result.PartitionKey = term.PartitionKey;
                result.RowKey = term.RowKey;
                result.Term = term.Term;
                result.ParentTerm = term.ParentTerm;
                results.Add(result);
            }

            // Get all the ontology term matches for this ontology
            List<POCO.OntologyTermMatch> termMatches = DataFactory.Ontology.GetOntologyTermMatches(providerConfig, ontologyAssigned.RowKey);

            foreach (POCO.OntologyTermMatch match in termMatches)
            {
                // Find the Term Match Result for this Term Match
                POCO.OntologyTermMatchResults matchedResult = results.Find(r => r.PartitionKey == match.PartitionKey && r.RowKey == match.TermRowKey);
                if (matchedResult != null)
                {
                    matchedResult.NumRecordAssociationMatches++;
                }
            }

            //TODO use Linq to select all the parent nodes instead
            // Walk the ontology tree and sum child counts to parent counts
            for(int i=0;i<results.Count;i++)
            {
                POCO.OntologyTermMatchResults result = results[i];
                // Find a parent ontology term (which has no ParentTerm = OntologyUri (PartitionKey)) 
                if (result.ParentTerm == result.PartitionKey)
                {
                    // Recursively sum the child terms up to its parent
                    SumOntologySummary(ref result, ref results);
                }
            }

            return results;

        }

        public class OntologyParentTerm
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
        }

        private static void SumOntologySummary(ref POCO.OntologyTermMatchResults parentResult, ref List<POCO.OntologyTermMatchResults> results)
        {
            // Make sure this term isn't referencing itself (this is allowed, but shouldn't try and look for any child terms)
            //if (parentResult.PartitionKey==parentResult.ParentTerm)
            //{
            //    return;
            //}

            int sumChildrenMatches = 0;
            // Find all the term matches with the same parent result and process
            for(int i = 0; i<results.Count;i++)
            {
                POCO.OntologyTermMatchResults childResult = results[i];
                // Check if the parent term matches our parent result
                if (childResult.ParentTerm==parentResult.RowKey)
                {
                    // Process children of this term
                    SumOntologySummary(ref childResult, ref results);

                    // Add the record association matches for this child to the sum
                    if (childResult.NumRecordAssociationMatches>0)
                    {
                        bool hasRecordAssocMatches = true;
                    }
                    sumChildrenMatches += childResult.NumRecordAssociationMatches;
                }
            }

            // Update the sum for this item
            parentResult.NumRecordAssociationMatches += sumChildrenMatches;
        }

        

        public static List<POCO.OntologyTerm> GetOntologyTerms(DataConfig providerConfig, string ontologyUri)
        {
            List<POCO.OntologyTerm> ontologyTerms = new List<POCO.OntologyTerm>();

            // Create a filter to match the Ontology provided
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", ontologyUri, "eq");
            filters.Add(pkFilter);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyTerm> azs = new List<AzureOntologyTerm>();
                    AzureTableAdaptor<AzureOntologyTerm> az = new AzureTableAdaptor<AzureOntologyTerm>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.OntologyTerms, combinedFilter);

                    foreach (var doc in azs)
                    {
                        ontologyTerms.Add(doc.Value);
                    }

                    return ontologyTerms;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyTerm>(providerConfig, MongoTableNames.OntologyTerms);

                    FilterDefinition<MongoOntologyTerm> filter = Utils.GenerateMongoFilter<MongoOntologyTerm>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var ontology in documents)
                    {
                        ontologyTerms.Add(ontology);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ontologyTerms;
        }

        public static List<POCO.CaptureRegexMatch> GetRegexMatches(DataConfig providerConfig, List<DataFactory.Filter> filters)
        {
            List<POCO.CaptureRegexMatch> regexMatches = new List<POCO.CaptureRegexMatch>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureRegexMatch> azs = new List<AzureRegexMatch>();
                    AzureTableAdaptor<AzureRegexMatch> az = new AzureTableAdaptor<AzureRegexMatch>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.RecordAssociationRegexMatch, combinedFilter);

                    foreach (var doc in azs)
                    {
                        regexMatches.Add(doc.Value);
                    }

                    return regexMatches;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoRegexMatch>(providerConfig, MongoTableNames.RecordAssociationRegexMatch);

                    FilterDefinition<MongoRegexMatch> filter = Utils.GenerateMongoFilter<MongoRegexMatch>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var ontology in documents)
                    {
                        regexMatches.Add(ontology);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return regexMatches;
        }

        public static List<POCO.CaptureRegexMatch> GetRegexMatches(DataConfig providerConfig, POCO.System system, string fileUri)
        {
           

            // Create a filter to match the Ontology provided
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", system.PartitionKey, "eq");
            filters.Add(pkFilter);
            string rowkey = fileUri;
            Filter rk = new Filter("RowKey", rowkey, "ge");
            filters.Add(rk);
            rk = new Filter("RowKey", Utils.GetLessThanFilter(rowkey), "lt");
            filters.Add(rk);

            return GetRegexMatches(providerConfig, filters);
        }

        public static List<POCO.OntologyTermMatch> GetOntologyTermMatches(DataConfig providerConfig, string ontologyUri)
        {
            // Create a filter to match the Ontology provided
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", ontologyUri, "eq");
            filters.Add(pkFilter);

            return GetOntologyTermMatches(providerConfig, filters);
        }

            public static List<POCO.OntologyTermMatch> GetOntologyTermMatches(DataConfig providerConfig, List<Filter> filters)
        {
            List<POCO.OntologyTermMatch> ontologyTermMatches = new List<POCO.OntologyTermMatch>();

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyTermMatch> azs = new List<AzureOntologyTermMatch>();
                    AzureTableAdaptor<AzureOntologyTermMatch> az = new AzureTableAdaptor<AzureOntologyTermMatch>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.RecordAssociationOntologyMatch, combinedFilter);

                    foreach (var doc in azs)
                    {
                        ontologyTermMatches.Add(doc.Value);
                    }

                    return ontologyTermMatches;
                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyTermMatch>(providerConfig, RecordAssociation.MongoTableNames.RecordAssociationOntologyMatch);

                    FilterDefinition<MongoOntologyTermMatch> filter = Utils.GenerateMongoFilter<MongoOntologyTermMatch>(filters);

                    var documents = collection.Find(filter).ToList();

                    foreach (var ontology in documents)
                    {
                        ontologyTermMatches.Add(ontology);
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return ontologyTermMatches;
        }

        public static POCO.OntologyMatchStatus GetOntologyMatchStatus(DataConfig providerConfig, RecordToRecordAssociation recassoc, OntologyAssigned ontology)
        {
            POCO.OntologyMatchStatus matchStatus = null;

            // Create a filter to match the Ontology provided
            List<Filter> filters = new List<Filter>();
            Filter pkFilter = new Filter("PartitionKey", ontology.RowKey, "eq");
            filters.Add(pkFilter);
            Filter rkFilter = new Filter("RowKey", recassoc.RowKey, "eq");
            filters.Add(rkFilter);

            switch (providerConfig.ProviderType)
            {
                case "azure.tableservice":
                    string combinedFilter = Utils.GenerateAzureFilter(filters);

                    List<AzureOntologyMatchStatus> azs = new List<AzureOntologyMatchStatus>();
                    AzureTableAdaptor<AzureOntologyMatchStatus> az = new AzureTableAdaptor<AzureOntologyMatchStatus>();
                    azs = az.ReadTableData(providerConfig, AzureTableNames.OntologyMatchStatus, combinedFilter);

                    if(azs.Count>0)
                    {
                        matchStatus = azs[0].Value;
                    }

                    return matchStatus;

                case "internal.mongodb":
                    var collection = Utils.GetMongoCollection<MongoOntologyMatchStatus>(providerConfig, MongoTableNames.OntologyMatchStatus);

                    FilterDefinition<MongoOntologyMatchStatus> filter = Utils.GenerateMongoFilter<MongoOntologyMatchStatus>(filters);

                    var documents = collection.Find(filter).ToList();
                    if (documents.Count>0)
                    {
                        matchStatus = documents[0];
                    }
                    break;
                default:
                    throw new ApplicationException("Data provider not recognised: " + providerConfig.ProviderType);
            }

            return matchStatus;
        }

        public static List<POCO.OntologyTerm> GetOntologyTerms(DataConfig providerConfig, POCO.OntologyAssigned ontologyAssigned)
        {
            return GetOntologyTerms(providerConfig, ontologyAssigned.RowKey);
        }
        public static List<POCO.OntologyTerm> GetOntologyTerms(DataConfig providerConfig, POCO.Ontology ontology)
        {
            return GetOntologyTerms(providerConfig, ontology.PartitionKey);
        }

    }

}
