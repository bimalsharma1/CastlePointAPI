using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.DataFactory
{
    public class Filter
    {
        public Filter(string fieldName, string fieldValue, string comparison)
        {
            this.FieldName = fieldName;
            this.FieldValue = fieldValue;
            this.Comparison = comparison;
        }
        public string FieldName { get; set; }
        public string FieldValue { get; set; }
        public string Comparison { get; set; }
    }
}
