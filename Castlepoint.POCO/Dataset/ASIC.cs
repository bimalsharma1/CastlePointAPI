using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Dataset
{
    public class CompanyRegister
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string CompanyName { get; set; }
        public string ACN { get; set; }
        public string Type { get; set; }
        public string Class { get; set; }
        public string SubClass { get; set; }
        public string Status { get; set; }
        public string DateofRegistration { get; set; }
        public string PreviousStateofRegistration { get; set; }
        public string StateRegistrationNumber { get; set; }
        public string ModifiedSinceLastReport { get; set; }
        public string CurrentNameIndicator { get; set; }
        public string ABN { get; set; }
        public string CurrentName { get; set; }
        public string CurrentNameStartDate { get; set; }
    }
}
