using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.DataFactory.Dataset.ASIC
{
    public class CompanyRegister:POCO.Dataset.CompanyRegister
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
    }

    public class AzureCompanyRegister : EntityAdapter<CompanyRegister>
    {
        public AzureCompanyRegister() { }
        public AzureCompanyRegister(CompanyRegister o) : base(o) { }
        protected override string BuildPartitionKey()
        {
            return this.Value.PartitionKey;
        }

        protected override string BuildRowKey()
        {
            return this.Value.RowKey;
        }
    }

    public class MongoCompanyRegister : CompanyRegister
    {

    }
}
